import json, math, datetime, openai, os, pyarrow, pandas, asyncio
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse
import sparrow_py as kt

async def main():
    start = datetime.datetime.now()
    
    # Load user label map
    output_map = {}
    with open('./user_output_map.json', 'r') as file:
        output_map = json.load(file)

    # Initialize clients
    kt.init_session()
    openai.api_key = os.environ.get("OPEN_AI_KEY")
    slack = SocketModeClient(
        app_token=os.environ.get("SLACK_APP_TOKEN"),
        web_client=AsyncWebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
    )


    
    # Backfill state with historical data
    historical_data = pandas.read_parquet("./messages.parquet")[:1]
    schema = pyarrow.Schema.from_pandas(historical_data)
    messages = kt.sources.ArrowSource(
        data = historical_data,
        time_column_name = "ts", 
        key_column_name = "channel",
    )


    
    # Receive Slack messages in real-time
    async def handle_message(client, req):
        # Acknowledge the message back to Slack
        await client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))

        if req.type == "events_api" and "event" in req.payload:
            # ignore message edit, delete, reaction events
            if "previous_message" in req.payload["event"] or req.payload["event"]["type"] == "reaction_added":
                return

            req.payload["event"]["ts"] = datetime.datetime.fromtimestamp(float(req.payload["event"]["ts"]))
            del req.payload["event"]["team"]
            data = pyarrow.RecordBatch.from_pylist([req.payload["event"]], schema=schema)
            messages.add_data(data)

    slack.socket_mode_request_listeners.append(handle_message)
    await slack.connect()
    

    
    # Compute conversations from individual messages
    conversations = messages.with_key(kt.record({
            "channel": messages.col("channel"),
            "thread": messages.col("thread_ts"),
        })) \
        .select("user", "ts", "text", "reactions") \
        .collect(max=3)


    
    # Handle each conversation as it occurs
    async for row in conversations.run(materialize=True).iter_rows_async():
        conversation = row[" result"]
        if len(conversation) == 0 or row["_time"] < start:
            continue

        print(f'Starting completion on conversation with first message text: {conversation[0]["text"]}')

        # Ask the model who should be notified
        res = openai.Completion.create(
            model="davinci:ft-personal:coversation-users-full-kaskada-2023-08-05-14-25-30",
            prompt="start -> " + "\n\n".join([f' {msg["user"]} --> {msg["text"]} ' for msg in conversation]) + "\n\n###\n\n",
            logprobs=5,
            max_tokens=1,
            stop=" end",
            temperature=1,
        )

        msg = conversation.pop(0)
        users = []
        logprobs = res["choices"][0]["logprobs"]["top_logprobs"][0]
        print(f"Predicted interest logprobs: {logprobs}")
        print(f"Notifying users: {users}")
        for user in logprobs:
            if math.exp(logprobs[user]) > 0.30:
                user = user.strip()
                # if users include `nil`, stop processing
                if user == "nil":
                    users = []
                    break
                users.append(user)
        
        # Send notification to users
        for user_num in users:
            if user_num not in output_map:
                print(f'User: {user_num} not in output_map, stopping.')
                continue

            user_id = output_map[user_num]

            print(f'Found user {user_num} in output map: {user_id}')

            app = slack.web_client.users_conversations(
                types="im",
                user=user_id,
            )
            if len(app["channels"]) == 0:
                print(f'User: {user_id} hasn\'t installed the slackbot yet')
                continue

            app_channel = app["channels"][0]["id"]
            print(f'Got user\'s slackbot channel id: {app_channel}')

            link = slack.web_client.chat_getPermalink(
                channel=msg["channel"],
                message_ts=msg["ts"],
            )["permalink"]

            print(f'Got message link: {link}')

            slack.web_client.chat_postMessage(
                channel=app_channel,
                text=f'You may be interested in this converstation: <{link}|{msg["text"]}>'
            )

            print(f'Posted alert message')

    print("Done")

if __name__ == "__main__":
   asyncio.run(main())