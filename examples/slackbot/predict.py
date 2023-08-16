import os, openai, pandas, multiprocessing

work_dir = "/Users/eric.pinzur/Documents/slackbot2000"
openai.api_key = os.environ.get("OPEN_AI_KEY")

next_message_model = 'ada:ft-personal:next-message-user-full-kaskada-2023-08-03-15-42-55'
# i think this model was mistakenly trained with the next_users data
next_reaction_model = 'ada:ft-personal:next-reaction-full-kaskada-2023-08-03-17-03-13'
next_users_model = 'ada:ft-personal:next-users-in-window-full-kaskada-2023-08-03-18-17-36'
coversations_model = "davinci:ft-personal:coversation-users-full-kaskada-2023-08-05-14-25-30"
conversations_b = "davinci:ft-personal:coversation-users-full-kaskada-b-2023-08-11-01-52-17"
conversations_c ="davinci:ft-personal:coversation-users-full-kaskada-c-2023-08-11-04-49-55"
conversations_nu_ada = "ada:ft-personal:coversation-next-message-ada-2023-08-15-12-27-13"
conversations_nu_cur = "curie:ft-personal:coversation-next-message-ada-2023-08-15-12-45-56"

cur4_model = "curie:ft-datastax:coversation-next-message-cur-4-2023-08-15-16-49-08"
dav_model = "davinci:ft-datastax:coversation-next-message-dav-2023-08-15-16-40-40"
dav4_model = "davinci:ft-datastax:coversation-next-message-dav-4-2023-08-15-18-05-07"
cur8_model = "curie:ft-datastax:coversation-next-message-cur-8-2023-08-15-18-01-18"

in_file_name = 'conversation_next_message_examples_joined_prepared_valid'
model = cur8_model
stop = None


df = pandas.read_json(f'{work_dir}/{in_file_name}.jsonl', lines=True)
prompts = df['prompt']
total = len(prompts)
items = [(i, prompts[i]) for i in range(total)]

df['prediction'] = None

# task that operates on an item
def task(index, prompt):
    if len(prompt) > 5000:
        return (index, {})
    pred = openai.Completion.create(model=model, prompt=prompt, max_tokens=1, stop=stop, n=1, logprobs=5, temperature=0)
    # pred = pred['choices'][0]['text']
    return (index, pred)

if __name__ == '__main__':
    # create a process pool that uses all cpus
    with multiprocessing.Pool(2) as pool:
        for (index, pred) in pool.starmap(task, items):
            df.at[index, 'prediction'] = pred

df.to_json(f'{work_dir}/{in_file_name}_with_pred.jsonl', lines=True, orient='records')
