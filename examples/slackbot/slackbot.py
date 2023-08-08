from slack_sdk.socket_mode import SocketModeClient, SocketModeResponse
import sparrow_pi as kt
import openai
import getpass
import pyarrow
import math

def build_conversation(messages):
    message_time = messages.col("ts")
    last_message_time = message_time.lag(1) # !!!
    is_new_conversation = message_time.seconds_since(last_message_time) > 10 * 60

    return messages \
        .select("user", "ts", "text", "reactions") \
        .collect(window=kt.windows.Since(is_new_conversation), max=100) \
        .select("user", "ts", "text") \
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

def main():
    # Initialize Kaskada with a local execution context.
    kt.init_session()

    # Initialize OpenAI
    openai.api_key = getpass.getpass('OpenAI: API Key')

    # Initialize Slack
    slack = SocketModeClient(
        app_token=getpass.getpass('Slack: App Token'),
        web_client=getpass.getpass('Slack: Bot Token'),
    )

    min_prob_for_response = 0.75

    # Receive Slack messages in real-time
    live_messages = kt.sources.read_stream(entity_column="channel", time_column="ts")

    # Receive messages from Slack
    def handle_message(client, req):
        # Acknowledge the message back to Slack
        client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))
        
        # Deliver the message to Kaskada
        live_messages.add_event(pyarrow.json.read_json(req.payload))
    slack.socket_mode_request_listeners.append(handle_message)
    slack.connect()

    # Handle messages in realtime
    # A "conversation" is a list of messages
    for conversation in build_conversation(live_messages).start().to_generator():
        if len(conversation) == 0:
            continue
        
        # Ask the model who should be notified
        res = openai.Completion.create(
            model="ft-2zaA7qi0rxJduWQpdvOvmGn3", 
            prompt=format_prompt(conversation),
            max_tokens=1,
            temperature=0,
            logprobs=5,
        )

        users = []
        logprobs = res["choices"][0]["logprobs"]["top_logprobs"][0]
        for user in logprobs:
            if math.exp(logprobs[user]) > min_prob_for_response:
                # if `nil` user is an option, stop processing
                if user == "nil":
                    users = []
                    break
                users.append(user)

        # alert on most recent message in conversation
        msg = conversation.pop()
        
        # Send notification to users
        for user in users:
            user_id = le.inverse_transform(user)

            link = slack.web_client.chat_getPermalink(
                channel=msg["channel"],
                message_ts=msg["ts"],
            )["permalink"]
            
            app_channel = slack.web_client.users_conversations(
                types="im",
                user=user_id,
            )["channels"][0]["id"]
            
            slack.web_client.chat_postMessage(
                channel=app_channel,
                text=f'You may be interested in this converstation: <{link}|{msg["text"]}>'
            )

if __name__ == "__main__":
   main()