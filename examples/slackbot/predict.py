import os, openai, pandas, multiprocessing

work_dir = "/Users/eric.pinzur/Documents/slackbot2000"
openai.api_key = os.environ.get("OPEN_AI_KEY")

next_message_model = 'ada:ft-personal:next-message-user-full-kaskada-2023-08-03-15-42-55'
# i think this model was mistakenly trained with the next_users data
next_reaction_model = 'ada:ft-personal:next-reaction-full-kaskada-2023-08-03-17-03-13'
next_users_model = 'ada:ft-personal:next-users-in-window-full-kaskada-2023-08-03-18-17-36'
coversations_model = "davinci:ft-personal:coversation-users-full-kaskada-2023-08-05-14-25-30"

in_file_name = 'conversation_user_examples_prepared_valid'
model = coversations_model
stop = " end"


df = pandas.read_json(f'{work_dir}/{in_file_name}.jsonl', lines=True)
prompts = df['prompt']
total = len(prompts)
items = [(i, prompts[i]) for i in range(total)]

df['prediction'] = None

# task that operates on an item
def task(index, prompt):
    if len(prompt) > 5000:
        return (index, {})
    pred = openai.Completion.create(model=model, prompt=prompt, max_tokens=1, stop=' end', n=1, logprobs=5, temperature=0)
    # pred = pred['choices'][0]['text']
    return (index, pred)

if __name__ == '__main__':
    # create a process pool that uses all cpus
    with multiprocessing.Pool() as pool:
        for (index, pred) in pool.starmap(task, items):
            df.at[index, 'prediction'] = pred

df.to_json(f'{work_dir}/{in_file_name}_with_pred.jsonl', lines=True, orient='records')
