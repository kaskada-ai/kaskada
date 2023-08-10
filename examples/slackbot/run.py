import json, math, openai, os
from slack_sdk.web import WebClient
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse

output_map = {}

with open('/Users/eric.pinzur/Documents/slackbot2000/conversation_users_output_map.json', 'r') as file:
    output_map = json.load(file)

print(f'Loaded output map: {output_map}')


# Initialize OpenAI
openai.api_key = os.environ.get("OPEN_AI_KEY")

# Initialize Slack
slack = SocketModeClient(
    app_token=os.environ.get("SLACK_APP_TOKEN"),
    web_client=WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
)

min_prob_for_response = 0.50

# Format for the OpenAI API
def format_prompt(prompt):
    return "start -> " + "\n\n".join([f' {msg["user"]} --> {msg["text"]} ' for msg in prompt]) + "\n\n###\n\n"

def handle_conversation(conversation):
    if len(conversation) == 0:
        return

    print(f'Starting prediction on conversation: {conversation[0]["text"]}')

    # Ask the model who should be notified
    res = openai.Completion.create(
        model="davinci:ft-personal:coversation-users-full-kaskada-2023-08-05-14-25-30",
        prompt=format_prompt(conversation),
        max_tokens=1,
        stop=" end",
        temperature=0,
        logprobs=5,
    )

    users = []
    logprobs = res["choices"][0]["logprobs"]["top_logprobs"][0]

    print(f'Recieved log probs: {logprobs}')
    for user in logprobs:
        if math.exp(logprobs[user]) > min_prob_for_response:
            # if `nil` user is an option, stop processing
            user = user.strip()
            if user == "nil":
                users = []
                print('Found nil, stopping.')
                break
            users.append(user)

    print(f'Found users to alert: {users}')
    # alert on most recent message in conversation
    msg = conversation.pop()

    # Send notification to users
    for user_num in users:
        if user_num not in output_map:
            print(f'User: {user_num} not in output_map, stopping.')
        else:
            user_id = output_map[user_num]

            print(f'Found user {user_num} in output map: {user_id}')

            link = slack.web_client.chat_getPermalink(
                channel=msg["channel"],
                message_ts=msg["ts"],
            )["permalink"]

            print(f'Got message link: {link}')

            res = slack.web_client.users_conversations(
                types="im",
                user=user_id,
            )
            if len(res["channels"]) == 0:
                print(f'User: {user} hasn\'t installed the slackbot yet')
            else:
                app_channel = res["channels"][0]["id"]
                print(f'Got user\'s bot channel id: {app_channel}')

                slack.web_client.chat_postMessage(
                    channel=app_channel,
                    text=f'You may be interested in this converstation: <{link}|{msg["text"]}>'
                )

                print(f'Posted alert message')

# Receive Slack messages in real-time
#live_messages = kt.sources.read_stream(entity_column="channel", time_column="ts")

# Receive messages from Slack
def handle_message(client, req):
    # Acknowledge the message back to Slack
    client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))

    # Deliver the message to Kaskada
    #live_messages.add_event(pyarrow.json.read_json(req.payload))

    if req.type == "events_api" and "event" in req.payload:
        e = req.payload["event"]

        # ignore message edit, delete, reaction events
        if "previous_message" in e or  e["type"] == "reaction_added":
            return

        # make single-message conversations for now
        handle_conversation([e])


# Handle messages in realtime
# A "conversation" is a list of messages
#for conversation in build_conversation(live_messages).start().to_generator():


slack.socket_mode_request_listeners.append(handle_message)
slack.connect()

# Just not to stop this process
from threading import Event
Event().wait()