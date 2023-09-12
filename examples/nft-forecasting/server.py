#!/usr/bin/env python

import json, math, datetime, openai, os, pyarrow, asyncio, re, time
from datetime import timezone
import kaskada as kd
import pyarrow as pa
import pandas as pd
from sklearn import preprocessing

from web3 import Web3
from websockets import connect
import argparse

parser = argparse.ArgumentParser(description='Predict NFT prices')

parser.add_argument('--key', help='Infura API key')
parser.add_argument('--secret', help='Infura API secret')

args = parser.parse_args()

infura_ws_url = f"wss://mainnet.infura.io/ws/v3/{args.key}"
infura_http_url = f"https://mainnet.infura.io/v3/{args.key}"

web3 = Web3(Web3.HTTPProvider(infura_http_url))

options1155 = {'topics': [Web3.keccak(text='TransferSingle(address,address,address,uint256,uint256)').hex()]}
request_string_1155 = json.dumps({"jsonrpc":"2.0", "id":1, "method":"eth_subscribe", "params":["logs", options1155]})

async def main():
    kd.init_session()

    transfers = kd.sources.PyDict(
        rows = [],
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

    async def receive_events():
        async with connect(infura_ws_url) as ws:
            await ws.send(request_string_1155)
            subscription_response = await ws.recv()
            print(subscription_response)
            while True:
                message = await asyncio.wait_for(ws.recv(), timeout=60)
                event = json.loads(message)

                result = event['params']['result']
                # block = int(result['blockNumber'], 16)
                # tx_hash = result['transactionHash']
                transfers.add_rows({
                    "BlockTimestamp": time.time(),
                    "TokenAddress": str(result["address"]),
                    "TokenID": str(int(result['data'][:66], 16)),
                    "FromAddress": '0x' + result['topics'][2][26:],
                    "ToAddress": '0x' + result['topics'][3][26:],
                    "Price": float(int(result['data'][66:], 16)), # NOTE: May be better to pull the source transfer
                    "Quantity": "1", # This protocol doesn't support quantity
                })

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

    await asyncio.gather(receive_events(), receive_outputs())

if __name__ == "__main__":
   asyncio.run(main())
