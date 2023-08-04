import openai, pandas, multiprocessing

work_dir = "/Users/eric.pinzur/Documents/slackbot2000"
openai.api_key = ""

next_message_model = 'ada:ft-personal:next-message-user-full-kaskada-2023-08-03-15-42-55'
# i think this model was mistakenly trained with the next_users data
next_reaction_model = 'ada:ft-personal:next-reaction-full-kaskada-2023-08-03-17-03-13'
next_users_model = 'ada:ft-personal:next-users-in-window-full-kaskada-2023-08-03-18-17-36'

in_file_name = 'next_reaction_valid'
model = next_reaction_model
stop = " END"


df = pandas.read_json(f'{work_dir}/{in_file_name}.jsonl', lines=True)
prompts = df['prompt']
total = len(prompts)
items = [(i, prompts[i]) for i in range(total)]

# task that operates on an item
def task(index, prompt):
    res = openai.Completion.create(model=next_users_model, prompt=prompt, max_tokens=50, stop=stop)
    pred = res['choices'][0]['text']
    return (index, pred)

if __name__ == '__main__':
    # create a process pool that uses all cpus
    with multiprocessing.Pool() as pool:
        for (index, pred) in pool.starmap(task, items):
            df.at[index, 'prediction'] = pred

df.to_json(f'{work_dir}/{in_file_name}_with_pred.jsonl', lines=True, orient='records')

