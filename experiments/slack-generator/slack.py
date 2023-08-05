import logging
import os

from json_normalize import json_normalize
from slack_sdk.web import WebClient
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.socket_mode.request import SocketModeRequest


logging.basicConfig(level=logging.INFO)

# Initialize SocketModeClient with an app-level token + WebClient
client = SocketModeClient(
    # This app-level token will be used only for establishing a connection
    app_token=os.environ.get("SLACK_APP_TOKEN"),  # xapp-A111-222-xyz
    # You will be using this WebClient for performing Web API calls in listeners
    web_client=WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))  # xoxb-111-222-xyz
)

def sendMessageReminder(user : str, message_channel : str, message_ts : str):
    # Get the message that was reacted to:
    # Call the conversations.history method using the WebClient
    result = client.web_client.conversations_history(
        channel=message_channel,
        inclusive=True,
        oldest=message_ts,
        limit=1,
    )
    message_text = result["messages"][0]["text"]

    # Get a link for the message:
    result = client.web_client.chat_getPermalink(
        channel=message_channel,
        message_ts=message_ts,
    )
    link = result["permalink"]

    # Find the user's App Channel
    result = client.web_client.users_conversations(
        types="im",
        user=user,
    )

    app_channel = result["channels"][0]["id"]

    client.web_client.chat_postMessage(
        channel=app_channel,
        text=f'You put eyes on this message: <{link}|{message_text}>'
    )

#def getMessageHistory():


def process(client: SocketModeClient, req: SocketModeRequest):
    # Acknowledge the request
    response = SocketModeResponse(envelope_id=req.envelope_id)
    client.send_socket_mode_response(response)

    if req.type == "events_api" and "event" in req.payload:
        e = req.payload["event"]

        # ignore message edit or delete events
        if "previous_message" in e:
            return

        # don't do this.  Just load the data and you are done. do the tricky stuff in kaskada.
        print(list(json_normalize(e, drop_nodes=["blocks", "client_msg_id", "channel_type", "item_user"])))

        if e["type"] == "reaction_added" and e["reaction"] == "eyes":
            sendMessageReminder(e["user"], e["item"]["channel"], e["item"]["ts"])


# Add a new listener to receive messages from Slack
# You can add more listeners like this
client.socket_mode_request_listeners.append(process)
# Establish a WebSocket connection to the Socket Mode servers
client.connect()
# Just not to stop this process
from threading import Event
Event().wait()

