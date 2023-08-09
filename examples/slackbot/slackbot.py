import json, math, datetime, openai, os, pyarrow, pandas, asyncio
#from slack_sdk.web import WebClient
from slack_sdk.web.async_client import AsyncWebClient
#from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse
import sparrow_py as kt

async def main():
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
            e = req.payload["event"]

            print(f'Received event from slack websocket: {e}')

            # ignore message edit, delete, reaction events
            if "previous_message" in e or  e["type"] == "reaction_added":
                return

            try:
                e["ts"] = datetime.datetime.fromtimestamp(float(e["ts"]))
                del e["team"]
                data = pyarrow.RecordBatch.from_pylist([e], schema=schema)
                
                print(f'Sending message event to kaskada: {e}')

                # Deliver the message to Kaskada
                messages.add_data(data)
                print("Done sending message")
            except Exception as e: print(e)

    slack.socket_mode_request_listeners.append(handle_message)
    await slack.connect()

    # Handle messages
    message_time = messages.time_of()
    #last_message_time = message_time.lag(1) # !!!
    #is_new_conversation = True #message_time.seconds_since(last_message_time) > 10 * 60

    conversations = messages \
        .select("user", "ts", "text") \
        .collect(max=100) #.collect(window=kt.SinceWindow(predicate=is_new_conversation), max=100)

    # A "conversation" is a list of messages
    start = now = datetime.datetime.now()
    print("Listening for new messages...")
    async for conversation in conversations.run(materialize=True).iter_rows_async():
        #if len(conversation) == 0 or conversation["_time"] < start:
        #    continue

        print(f'Conversation: {conversation}')
        print(f'Starting completion on conversation with first message text: {conversation["result"][0]["text"]}')

        prompt = "start -> " + "\n\n".join([f' {msg["user"]} --> {msg["text"]} ' for msg in conversation["result"]]) + "\n\n###\n\n"

        print(f'Using prompt: {prompt}')

        # Credentials don't work yet...
        continue

        # Ask the model who should be notified
        res = openai.Completion.create(
            model="davinci:ft-personal:coversation-users-full-kaskada-2023-08-05-14-25-30",
            prompt=prompt,
            logprobs=5,
            max_tokens=1,
            stop=" end",
            temperature=0,
        )

        print(f'Received completion response: {res}')

        users = []
        logprobs = res["choices"][0]["logprobs"]["top_logprobs"][0]

        print(f'Found logprobs: {logprobs}')
        for user in logprobs:
            if math.exp(logprobs[user]) > 0.50:
                user = users.strip()
                # if users include `nil`, stop processing
                if user == "nil":
                    users = []
                    break
                users.append(user)

        print(f'Found users to alert: {users}')

        # alert on most recent message in conversation
        msg = conversation.pop()

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