import json

file = "/Users/eric.pinzur/Documents/slackbot2000/conversation_next_message_examples"

with open(f'{file}.jsonl', 'r') as in_file:
    with open(f'{file}_out_good.jsonl', 'w') as good_file:
        with open(f'{file}_out_bad.jsonl', 'w') as bad_file:
            while True:

                line = in_file.readline()

                if not line:
                    break

                data = json.loads(line)

                prompt = data["prompt"]

                print(f'\nPrompt:\n\n{prompt}')

                meaningful = None

                while True:
                    print(f'\nIs this a meaningful prompt (y/n):')
                    i = input()
                    if i == "n":
                        meaningful = False
                        break
                    elif i == "y":
                        meaningful = True
                        break
                    else:
                        continue

                if meaningful:
                    good_file.write(line)
                else:
                    bad_file.write(line)
