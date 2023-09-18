#!/usr/bin/env python

import json, math, datetime, openai, os, pyarrow, asyncio, re, time
import argparse
from datetime import timezone
import kaskada as kd
import pyarrow as pa
import pandas as pd
from sklearn import preprocessing
import asyncpraw


from web3 import Web3
from websockets import connect
from web3.exceptions import ContractLogicError

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

parser = argparse.ArgumentParser(description='Predict NFT prices')

parser.add_argument('--infura-key', help='Infura API key')
parser.add_argument('--infura-secret', help='Infura API secret')
parser.add_argument('--reddit-client-id', help='Reddit Client ID')
parser.add_argument('--reddit-client-secret', help='Reddit Client Secret')
parser.add_argument('--reddit-user-agent', help='Reddit user agent', default='kaskada-demo')

args = parser.parse_args()

infura_ws_url = f"wss://mainnet.infura.io/ws/v3/{args.infura_key}"
infura_http_url = f"https://mainnet.infura.io/v3/{args.infura_key}"

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

    reddit = asyncpraw.Reddit(
        client_id=args.reddit_client_id,
        client_secret=args.reddit_client_secret,
        user_agent=args.reddit_user_agent,
    )

    transfers = kd.sources.PyDict(
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

    comments = kd.sources.PyDict(
        schema=pa.schema([
            pa.field("author", pa.string()),
            pa.field("body", pa.string()),
            pa.field("permalink", pa.string()),
            pa.field("submission_id", pa.string()),
            pa.field("subreddit", pa.string()),
            pa.field("ts", pa.float64()),
        ]),
        time_column="ts",
        key_column="submission_id",
        time_unit="s",
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

    async def receive_comments():
        # Create the subreddit handles
        subreddits = await asyncio.gather(*[reddit.subreddit(n) for n in ["NFT", "NFTsMarketplace"]])

        async def consume_comments(sr): 
            # Consume the stream of new comments
            async for comment in sr.stream.comments():
                print(f"Adding comment {comment}")
                # Add each comment to the Kaskada data source
                await comments.add_rows({
                    "author": comment.author.name,
                    "body": comment.body,
                    "permalink": comment.permalink,
                    "submission_id": comment.submission.id,
                    "subreddit_id": comment.subreddit.display_name,
                    "ts": time.time(),
                })  

        await asyncio.gather(*[consume_comments(sr) for sr in subreddits])

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

    async def do_sentiment():
        sid_obj = SentimentIntensityAnalyzer()

        @kd.udf("f(post: string) -> f32")
        def to_sentiment(post):
            # Use the VADER algorithm to assess sentiment from -1 to +1 for each comment
            return post.apply(lambda p: sid_obj.polarity_scores(p)["compound"] )

        sentiment = comments.with_key(False).col("body").pipe(to_sentiment).mean(window=kd.windows.Since.minutely())

        async for row in sentiment.run_iter(kind="row", mode="live"):
            print(f"Got outputs: {row}")

    #await asyncio.gather(receive_transfers(), receive_comments(), receive_outputs())
    #await at_client.start(receive_at)
    #await receive_transfers()
    #await receive_comments()
    await asyncio.gather(receive_comments(), do_sentiment())

if __name__ == "__main__":
   asyncio.run(main())