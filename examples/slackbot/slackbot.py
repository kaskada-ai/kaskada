import json, math, openai, os, pyarrow
from slack_sdk.web import WebClient
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse
import sparrow_pi as kt

def build_conversation(messages):
    message_time = messages.col("ts")
    last_message_time = message_time.lag(1) # !!!
    is_new_conversation = message_time.seconds_since(last_message_time) > 10 * 60

    return messages \
        .select("user", "ts", "text", "reactions") \
        .collect(window=kt.windows.Since(is_new_conversation), max=100)

def build_examples(messages):
    duration = kt.minutes(5)  # !!!

    coverstation = build_conversation(messages)
    shifted_coversation = coverstation.shift_by(duration)  # !!!

    reaction_users = coverstation.col("reactions").col("name").collect(kt.windows.Trailing(duration)).flatten()  # !!!
    participating_users = coverstation.col("user").collect(kt.windows.Trailing(duration))  # !!!
    engaged_users = kt.union(reaction_users, participating_users)  # !!!

    return kt.record({ "prompt": shifted_coversation, "completion": engaged_users}) \
        .filter(shifted_coversation.is_not_null())

def format_prompt(prompt):
    return "start -> " + "\n\n".join([f' {msg["user"]} --> {msg["text"]} ' for msg in prompt]) + "\n\n###\n\n"

def main():
    output_map = {}

    with open('./user_output_map.json', 'r') as file:
        output_map = json.load(file)

    print(f'Loaded output map: {output_map}')

    # Initialize Kaskada with a local execution context.
    kt.init_session()

    # Initialize OpenAI
    openai.api_key = os.environ.get("OPEN_AI_KEY")

    # Initialize Slack
    slack = SocketModeClient(
        app_token=os.environ.get("SLACK_APP_TOKEN"),
        web_client=WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
    )

    min_prob_for_response = 0.50

    # Receive Slack messages in real-time
    live_messages = kt.sources.read_stream(entity_column="channel", time_column="ts")

    # Receive messages from Slack
    def handle_message(client, req):
        # Acknowledge the message back to Slack
        client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))

        if req.type == "events_api" and "event" in req.payload:
            e = req.payload["event"]

            print(f'Received event from slack websocket: {e}')

            # ignore message edit, delete, reaction events
            if "previous_message" in e or  e["type"] == "reaction_added":
                return

            print(f'Sending message event to kaskada: {e}')

            # Deliver the message to Kaskada
            live_messages.add_event(pyarrow.json.read_json(e))

    slack.socket_mode_request_listeners.append(handle_message)
    slack.connect()

    # Handle messages in realtime
    # A "conversation" is a list of messages
    for conversation in build_conversation(live_messages).start().to_generator():
        if len(conversation) == 0:
            continue

        print(f'Starting completion on conversation with first message text: {conversation[0]["text"]}')

        prompt = format_prompt(conversation)

        print(f'Using prompt: {prompt}')

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
            if math.exp(logprobs[user]) > min_prob_for_response:
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

if __name__ == "__main__":
   main()