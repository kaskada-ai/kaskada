import json, openai, os, time, logging, backoff

logging.getLogger('backoff').addHandler(logging.StreamHandler())

work_dir = "/Users/eric.pinzur/Documents/slackbot2000"
openai.api_key = os.environ.get("OPEN_AI_KEY")

good = open(f'{work_dir}/conversation_next_message_examples_out_good_summarized.jsonl', 'r')
bad = open(f'{work_dir}/conversation_next_message_examples_out_bad.jsonl', 'r')

messages = [{
    "role": "system",
    "content": "You are a helpful assistant. Your job is to determine if a prompt will be helpful for fine-tuning a model. All prompts start with 'start -->' and end with: '\\n\\n###\\n\\n'. You should respond 'yes' if you think the prompt has enough context to be helpful, or 'no' if not. No explanation is needed. You should only respond with 'yes' or 'no'."
}]

count = 0
while True:

    good_line = good.readline()
    bad_line = bad.readline()
    count += 1

    if not good_line or not bad_line:
        break

    good_data = json.loads(good_line)
    bad_data = json.loads(bad_line)

    messages.append({
        "role": "user",
        "content": f'start -->{good_data["prompt"]}'
    })

    messages.append({
        "role": "assistant",
        "content": "yes"
    })

    messages.append({
        "role": "user",
        "content": f'start -->{bad_data["prompt"]}'
    })

    messages.append({
        "role": "assistant",
        "content": "no"
    })

good.close()
bad.close()

file = open(f'{work_dir}/conversation_next_message_examples.jsonl', 'r')
out_file = open(f'{work_dir}/conversation_next_message_examples_cleaned.jsonl', 'a')

@backoff.on_exception(backoff.expo, (openai.error.RateLimitError, openai.error.ServiceUnavailableError))
def chat_with_backoff(**kwargs):
    time.sleep(1)
    try:
        return openai.ChatCompletion.create(**kwargs)
    except openai.error.InvalidRequestError:
        return None

count = 0
while True:
    line = file.readline()
    count +=1

    if not line:
        break

    if count < 8243:
        continue

    data = json.loads(line)

    if data["completion"] not in [" 1", " 2", " 5", " 10"]:
        continue

    prompt = data["prompt"]

    if len(prompt) < 100:
        continue

    msgs = messages.copy()

    msgs.append({
        "role": "user",
        "content": f'start -->{prompt}'
    })

    res = chat_with_backoff(
        model = "gpt-3.5-turbo",
        messages = msgs
    )
    if not res:
        continue
    response = res["choices"][0]["message"]["content"]

    print(f'Result was `{response}` for prompt: {prompt}')

    print(f'Currently processing line: {count}')

    if response == "yes":
        out_file.write(line)
        out_file.flush()

file.close()
out_file.close()
