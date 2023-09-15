#!/usr/bin/env python

import json, math, datetime, openai, os, pyarrow, asyncio, re, time
from datetime import timezone
import kaskada as kd
import pyarrow as pa
import pandas as pd
from sklearn import preprocessing
from atproto.firehose import AsyncFirehoseSubscribeReposClient, parse_subscribe_repos_message
from atproto import CAR, AtUri, models
from atproto.xrpc_client.models import get_or_create, ids, is_record_type


from web3 import Web3
from websockets import connect
from web3.exceptions import ContractLogicError
import argparse

parser = argparse.ArgumentParser(description='Predict NFT prices')

parser.add_argument('--key', help='Infura API key')
parser.add_argument('--secret', help='Infura API secret')

args = parser.parse_args()

infura_ws_url = f"wss://mainnet.infura.io/ws/v3/{args.key}"
infura_http_url = f"https://mainnet.infura.io/v3/{args.key}"

web3 = Web3(Web3.HTTPProvider(infura_http_url))

# TODO: TransferBatch also...
options1155 = {'topics': [Web3.keccak(text='TransferSingle(address,address,address,uint256,uint256)').hex()]}
request_string_1155 = json.dumps({"jsonrpc":"2.0", "id":1, "method":"eth_subscribe", "params":["logs", options1155]})
standard_erc1155_abi = [
    {
        "constant": True,
        "inputs": [
            {"name": "_id", "type": "uint256"}
        ],
        "name": "uri",
        "outputs": [
            {"name": "", "type": "string"}
        ],
        "type": "function"
    }
]

async def main():
    kd.init_session()

    at_client = AsyncFirehoseSubscribeReposClient()

    transfers = kd.sources.PyDict(
        #rows = [],
        schema = pa.schema([
            pa.field("BlockTimestamp", pa.int64()),
            pa.field("TokenAddress", pa.string()),
            pa.field("TokenID", pa.string()),
            pa.field("FromAddress", pa.string()),
            pa.field("ToAddress", pa.string()),
            pa.field("Price", pa.float64()),
            pa.field("Quantity", pa.string()),
        ]),
        time_column = "BlockTimestamp",
        key_column = "TokenAddress",
        time_unit = "us",
    )

    async def receive_transfers():
        async with connect(infura_ws_url) as ws:
            await ws.send(request_string_1155)
            subscription_response = await ws.recv()
            print(subscription_response)
            while True:
                message = await asyncio.wait_for(ws.recv(), timeout=120)
                event = json.loads(message)
                result = event['params']['result']

                token_address = result["address"] 
                token_id = int(result['data'][:66], 16)
                contract_address = Web3.to_checksum_address(token_address)
                contract = web3.eth.contract(address=contract_address, abi=standard_erc1155_abi)
                try:
                    metadata_uri = contract.functions.uri(token_id).call()
                except ContractLogicError:
                    print("contract reverted")
                    continue

                print(f"contract uri: {metadata_uri}")

                # block = int(result['blockNumber'], 16)
                # tx_hash = result['transactionHash']
                await transfers.add_rows({
                    "BlockTimestamp": time.time(),
                    "TokenAddress": str(result["address"]),
                    "TokenID": str(int(result['data'][:66], 16)),
                    "FromAddress": '0x' + result['topics'][2][26:],
                    "ToAddress": '0x' + result['topics'][3][26:],
                    "Price": float(int(result['data'][66:], 16)), # NOTE: May be better to pull the source transfer
                    "Quantity": "1", # This protocol doesn't support quantity
                })

    async def receive_at(message) -> None:
        commit = parse_subscribe_repos_message(message)

        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return 

        ops = _get_ops_by_type(commit)
        for post in ops['posts']['created']:
            post_msg = post['record'].text
            post_langs = post['record'].langs
            print(f'New post in the network! Langs: {post_langs}. Text: {post_msg}')

    async def receive_outputs():
        mean_buyer_price = transfers.col("Price").with_key(transfers.col("ToAddress")).mean()

        features = kd.record({
            "mbp": mean_buyer_price.lookup(transfers.col("ToAddress")),
            "last_transfer": transfers.col("Price").lag(1).else_(0),
            "avg_price": transfers.col("Price").mean(window=kd.windows.Sliding(10, transfers.is_not_null())),
            "avg_purchase": transfers.col("Price").mul(transfers.col("Quantity").cast(pa.int32())).mean().else_(0),
        })

        async for row in features.run_iter(kind="row", mode="live"):
            print(f"Got outputs: {row}")

    #await asyncio.gather(receive_transfers(), at_client.start(receive_at), receive_outputs())
    #await at_client.start(receive_at)
    await receive_transfers()

# From https://raw.githubusercontent.com/MarshalX/atproto/main/examples/firehose/process_commits.py
def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> dict:  # noqa: C901
    operation_by_type = {
        'posts': {'created': [], 'deleted': []},
        'reposts': {'created': [], 'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []},
    }

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'update':
            # not supported yet
            continue

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = get_or_create(record_raw_data, strict=False)
            if uri.collection == ids.AppBskyFeedLike and is_record_type(record, ids.AppBskyFeedLike):
                operation_by_type['likes']['created'].append({'record': record, **create_info})
            elif uri.collection == ids.AppBskyFeedPost and is_record_type(record, ids.AppBskyFeedPost):
                operation_by_type['posts']['created'].append({'record': record, **create_info})
            elif uri.collection == ids.AppBskyFeedRepost and is_record_type(record, ids.AppBskyFeedRepost):
                operation_by_type['reposts']['created'].append({'record': record, **create_info})
            elif uri.collection == ids.AppBskyGraphFollow and is_record_type(record, ids.AppBskyGraphFollow):
                operation_by_type['follows']['created'].append({'record': record, **create_info})

        if op.action == 'delete':
            if uri.collection == ids.AppBskyFeedLike:
                operation_by_type['likes']['deleted'].append({'uri': str(uri)})
            elif uri.collection == ids.AppBskyFeedPost:
                operation_by_type['posts']['deleted'].append({'uri': str(uri)})
            elif uri.collection == ids.AppBskyFeedRepost:
                operation_by_type['reposts']['deleted'].append({'uri': str(uri)})
            elif uri.collection == ids.AppBskyGraphFollow:
                operation_by_type['follows']['deleted'].append({'uri': str(uri)})

    return operation_by_type

if __name__ == "__main__":
   asyncio.run(main())