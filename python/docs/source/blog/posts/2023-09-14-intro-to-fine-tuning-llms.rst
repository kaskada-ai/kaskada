---
blogpost: true
date: 2023-Sep-14
author: eric
tags:
  - few-shot learning
  - fine-tuning
  - gen-ai
  - kaskada
  - llm
  - python
  - open-ai
excerpt: 2
description: Intro to Fine-Tuning Large Language Models (LLMs)
---

=====
Intro
=====


As a contributor to the `Kaskada <http://kaskada.io/kaskada>`__
open-source project, I’ve always been driven by the immense potential of
today’s technology. In this post, I’ll discuss my learnings around
fine-tuning large language models (LLMs). As an example, I’ll use my
work on `BeepGPT <https://github.com/kaskada-ai/beep-gpt>`__, where I
set out to build a customized, real-time model to predict which Slack
conversations might different users find interesting.

First, I’d like to say that I am not a Data Scientist. I’ve worked with
a few Data Scientists, but my knowledge in the space is limited. With
the advent of high-quality LLMs, software engineers like me can now
delve into Data Science with little-to-no Data Science background.

So, why should you care? Because the line between Data Science
professionals and enthusiasts like you and me is becoming delightfully
blurred. Dive in to explore how I ventured into the domain of Data
Scientists and what I uncovered along the way.

Here are a few tips before we get started
=========================================

First is a willingness to experiment. Model fine-tuning is an iterative
process. The first way you build your training examples will likely not
produce a successful model. When working on BeepGPT I experimented with
five different approaches (over a week) before I found one that was
successful at predicting user interest.

Second is the importance of data quality. When fine-tuning a model,
numerous training examples are sent to a model to update its behavior.
The examples must contain strong signals that relate to the desired
output. Despite the recent advances made with LLMs, garbage in still
leads to garbage out. [see: @gigo]

One trick I developed was using LLMs to help make decisions about the
quality of each example. With few-shot learning, I taught a model what
strong signals look like, and I was able to use that to enhance the
quality of my dataset.

Finally, it is vital to have many training examples. Ideally, a
fine-tuning job should be run with at least a few thousand training
examples. The more examples you can provide to the model, the more it
can learn, and the better the predictions will be in production.

Definitions
===========

Large Language Model (LLM)
--------------------------

A large language model is an artificial intelligence (AI) algorithm that
uses deep learning techniques and massively large data sets to
understand, summarize, generate, and predict new content. [@llm]

Tokens
------

LLMs process text as tokens instead of as characters. Tokens represent
sets of characters that commonly occur in a specific sequence. On
average, a token represents about four characters. You can use `OpenAI’s
tokenizer tool <https://platform.openai.com/tokenizer>`__ to see how
different texts get converted into tokens, for example:

.. container:: sourceCode

   .. raw:: html

      <pre class="sourceCode"><span class="tokenizer-tkn tokenizer-tkn-0" title="15592">Team</span><span class="tokenizer-tkn tokenizer-tkn-1" title="11">,</span><span class="tokenizer-tkn tokenizer-tkn-2" title="750"> did</span><span class="tokenizer-tkn tokenizer-tkn-3" title="345"> you</span><span class="tokenizer-tkn tokenizer-tkn-4" title="2883"> enjoy</span><span class="tokenizer-tkn tokenizer-tkn-0" title="262"> the</span><span class="tokenizer-tkn tokenizer-tkn-1" title="299"> n</span><span class="tokenizer-tkn tokenizer-tkn-2" title="620">ach</span><span class="tokenizer-tkn tokenizer-tkn-3" title="418">os</span><span class="tokenizer-tkn tokenizer-tkn-4" title="7415"> yesterday</span><span class="tokenizer-tkn
      tokenizer-tkn-0" title="30">?</span></pre>

The color highlighting shows how 41 characters become 11 tokens. You can
mouse over to see the actual token values.

Common words and most positive integers under 1000 equate to a single
token. Whitespace and capitalization matter: `` Team``, `` team``,
``Team``, and ``team`` equate to four different tokens.

Prompts & Completions
---------------------

Prompts are the input to LLMs. If you have played with ChatGPT before,
the request you sent was a prompt.

Completions are the outputs from LLMs. With ChatGPT, the response to
your question was a completion.

Training Examples
-----------------

Training examples are prompt & completion pairs. The prompt is the text
we would have sent to the model, and the completion is the response we
would have expected.

Maximum Token Length
--------------------

The maximum length of an API request to a model, in tokens. Depending on
the model, there is a different maximum length.

Few-Shot Learning
-----------------

Few-shot learning is a method of working with language models to improve
response quality for specific tasks. It works by including training
examples on each request to a model, demonstrating the desired response
behavior. For example, when playing with ChatGPT:

.. code:: default

   > I'd like you to identify if an animal passed to you is a bird or not-a-bird. Here are some examples:
   > Penguin: bird
   > Rabbit: not-a-bird
   > Parrot:

.. container:: center

   ``Penguin: bird`` and ``Rabbit: not-a-bird`` are few-shot examples.

Few-shot learning helps adapt an existing LLM to perform a new task
quickly. However, it is limited by the Maximum Token Length of the
model. If you are trying to train a model to do a complex behavior, you
might not be able to include all your desired training examples in a
single request. Additionally, the cost and response time increase as the
number of tokens you send increases.

Model Fine-Tuning
-----------------

Fine-tuning is the process of re-training existing base language models
(like GPT3) with new datasets to perform specific tasks. Fine-tuning
improves on few-shot learning by training on many more examples than can
fit in a single request.

Continuing the previous example, we could fine-tune an existing model by
providing a file containing hundreds of animals labeled ``bird`` or
``not-a-bird``. The output of this would be a new model that would be
more efficient at this specific task.

Making requests to a fine-tuned model is generally cheaper and faster
than sending few-shot learning requests. However, the process of
fine-tuning a model itself can be quite expensive.

It is important to weigh the tradeoffs between few-shot learning and
model fine-tuning.

Building Training Examples
==========================

Hypothesis generation
---------------------

Before we start building training examples, you need to form hypotheses
about what you want to predict and how you might do so successfully.
This is where the iterative process starts.

For BeepGPT I experimented with the following ideas:

-  For a set of recent messages in a channel, try to predict:

   -  The reaction (if any) to the most recent message
   -  The next user that will reply
   -  The set of users that might interact next (reply or react)

-  For the set of recent messages in a conversation, try to predict:

   -  The set of users that might interact next
   -  The next user that will reply

Note that I didn’t initially develop all of these ideas, only the first
one. I built out training examples for each idea and tried to fine-tune
a model. When the results weren’t as good as expected, I moved on to the
next idea. Experimentation is key.

Ultimately, I was most successful with the final idea: Predict the next
user to reply to the set of recent messages in a conversation. The rest
of the post will focus on this.

.. container::

      **Tip**

      I used `Kaskada <http://kaskada.io/kaskada>`__ to iterate on these
      ideas quickly. Kaskada is a tool that makes it easy to collect and
      aggregate events from raw data. You don’t need to preprocess
      anything. Just import the raw events and start experimenting.
      Furthermore, Kaskada ensures that your examples will not be
      subject to leakage, which is a big problem in predictive modeling.
      In a future post, I’ll show how I used Kaskada to generate
      training examples for each of the above ideas.

Example construction
--------------------

Consider this conversation:

.. container:: hanging-indent margin-0

   **UserA**: ``Team, did you enjoy the nachos yesterday?``

   **UserB**: ``Yes, I love Mexican food.``

   **UserA**:
   ``<@UserC> I'm trying to get my application deployed in Kubernetes. However, I can't determine whether to use a deployment or a stateful set. I found some docs here: http://tinyurl.com/4k53dc8h, but I'm still unsure which to choose. Can you help?``

   **UserB**:
   ``UserC is at lunch now. They will be back in about an hour. I don't know much about this either, but I can try to help. Or is it okay to wait until UserC returns?``

   **UserA**: ``I can wait for UserC to return.``

   **UserC**:
   ``I can help with this. Can you tell me more about your application? Does it have any persistent storage requirements?``

Reviewing the definition of *Training Examples*, it states: “Training
examples are prompt & completion pairs. The prompt is the text we would
have sent to the model, and the completion is the response we would have
expected.”

In BeepGPT, remember that we are trying to predict who might respond
next in a conversation. We want the model to understand how users
interact based on their interests, social relationships,
responsibilities, etc. Therefore, we build the *prompt* from the
previous messages in the conversation. For the *completion*, we will use
the event we extracted from our data (the next user to reply) as the
training signal.

From the first two messages in the conversation, we can generate a
training example. The prompt is the first message, and the completion is
the user that responded:

.. container:: hanging-indent margin-0

   **prompt**: ``Team, did you enjoy the nachos yesterday?``
   **completion**: ``UserB``

Instead, if we consider the last four messages we can build another
training example. Note, we use two new-line characters (``\n\n``) to
join messages:

.. container:: hanging-indent margin-0

   **prompt**:
   ``<@UserC> I'm trying to get my application deployed in Kubernetes. However, I can't determine whether to use a deployment or a stateful set. I found some docs here: http://tinyurl.com/4k53dc8h, but I'm still unsure which to choose. Can you help?\n\nUserC is at lunch now. They will be back in about an hour. I don't know much about this either, but I can try to help. Or is it okay to wait until UserC returns?\n\nI can wait for UserC to return.``

   **completion**: ``UserC``

When using the OpenAI fine-tuning API, each training example should be a
blob of JSON in a specific format on a single line. Converting our two
examples above, we now have the following:

.. code:: json

   {"prompt": "Team, did you enjoy the nachos yesterday?", "completion": "UserB"}
   {"prompt": "<@UserC> I'm trying to get my application deployed in Kubernetes. However, I can't determine whether to use a deployment or a stateful set. I found some docs here: http://tinyurl.com/4k53dc8h, but I'm still unsure which to choose. Can you help?\n\nUserC is at lunch now. They will be back in about an hour. I don't know much about this either, but I can try to help. Or is it okay to wait until UserC returns?\n\nI can wait for UserC to return.", "completion": "UserC"}

Formatting examples
-------------------

Next, there are several formatting rules that you are recommended to
follow. I don’t understand why these are recommended, but I followed
them anyway.

-  All prompts should end with the same set of characters. The set of
   characters used should not occur elsewhere in your dataset. The
   recommended string for textual input data is ``\n\n###\n\n``.
-  All completions should start with a single whitespace character.

Applying these rules to our examples, we get:

.. code:: json

   {"prompt": "Team, did you enjoy the nachos yesterday?\n\n###\n\n", "completion": " UserB"}
   {"prompt": "<@UserC> I'm trying to get my application deployed in Kubernetes. However, I can't determine whether to use a deployment or a stateful set. I found some docs here: http://tinyurl.com/4k53dc8h, but I'm still unsure which to choose. Can you help?\n\nUserC is at lunch now. They will be back in about an hour. I don't know much about this either, but I can try to help. Or is it okay to wait until UserC returns?\n\nI can wait for UserC to return.\n\n###\n\n", "completion": " UserC"}

Training example cleanup
------------------------

Finally, I found that model training works best if the following is
done:

-  Non-textual data like http-links, code blocks, and IDs are removed
   from the prompts.
-  Completions are reduced to a single token in length.

We can use regex and other string functions to remove non-textual data
from the prompts. And we can use standard data science tools like the
`Scikit-Learn
LabelEncoder <https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelEncoder.html>`__
to create a mapping from UserIds to integers. Remember that positive
integers under one thousand map to unique tokens.

So now we have:

.. code:: json

   {"prompt": "Team, did you enjoy the nachos yesterday?\n\n###\n\n", "completion": " 1"}
   {"prompt": "I'm trying to get my application deployed in Kubernetes. However, I can't determine whether to use a deployment or a stateful set. I found some docs here:, but I'm still unsure which to choose. Can you help?\n\nUserC is at lunch now. They will be back in about an hour. I don't know much about this either, but I can try to help. Or is it okay to wait until UserC returns?\n\nI can wait for UserC to return.\n\n###\n\n", "completion": " 2"}

.. container::

      **Important**

      Removing IDs was especially helpful. Before I did so, the model
      essentially learned how to map from input UserId to output user.
      It skipped learning anything valuable from the actual text of the
      messages.

We now have two training examples that we could use for fine-tuning a
model.

Ideally, for fine-tuning, we would have several thousand examples. Using
a tool like `Kaskada <http://kaskada.io/kaskada>`__, generating examples
like this from your entire Slack history should be relatively easy.

Refining Training Examples
==========================

Before proceeding with fine-tuning, I recommend taking the following
steps:

1. Use few-shot learning to determine which examples contain strong
   signals to be helpful.
2. Use the OpenAI CLI to validate that the training examples are in the
   correct format.

Determining signal strength
---------------------------

Overview
~~~~~~~~

I found that it is best if each training example contains strong signals
to predict your desired outcome. If an example doesn’t have enough
signal, you should consider excluding it from your training set.

Depending on your goal, including some negative examples may also be
helpful. Negative examples help train the model about when not to make a
prediction or stated another way, about when to predict that no action
should be taken.

For example, with BeepGPT we are trying to predict when a set of
messages might be interesting for a specific user. If we look at our
training examples from the previous section, the first does not contain
anything interesting. We would not want to alert anyone about this
message. Therefore, we should convert this example into a negative
example.

The second example does contain a strong signal. Here, we would like to
alert users who have previously answered questions about Kubernetes.
This example should be left as is.

To convert an example to a negative one, we must change its completion
to indicate a non-response. I chose to use `` nil`` for this, which is
represented by a single token in OpenAI.

.. code:: json

   {"prompt": "Team, did you enjoy the nachos yesterday?\n\n###\n\n", "completion": " nil"}
   {"prompt": "I'm trying to get my application deployed in Kubernetes. However, I can't determine whether to use a deployment or a stateful set. I found some docs here: but I'm still unsure which to choose. Can you help?\n\nUserC is at lunch now. They will be back in about an hour. I don't know much about this either, but I can try to help. Or is it okay to wait until UserC returns?\n\nI can wait for UserC to return.\n\n###\n\n", "completion": " 2"}

Automating this process
~~~~~~~~~~~~~~~~~~~~~~~

Instead of manually going through each generated example to determine if
it should be positive or negative, we can use a model with few-shot
learning to do this work.

.. container::

      **Game Changer**

      I developed this trick of using few-shot learning to improve
      training data quality. It didn’t seem like it would work
      initially, but it was surprisingly effective. I think this is
      potentially a game-changing technique for enhancing the
      fine-tuning of LLMs.

To start with few-shot learning, we need to find a few examples that we
will provide to the model to make decisions on our behalf. Look through
your generated examples and try to find 20-30 for each bucket:
**positive** and **negative**. Add these to files:
``examples_pos.jsonl`` and ``examples_neg.jsonl``

Positive (strong signal) examples:

-  I’ve been utilizing the Rust syntax highlighter for my code blocks.
   It does a good job of differentiating between functions and literals.
-  The agent doesn’t push to Prometheus; this is just another proxy
   location that Prometheus scrapes.

Negative (weak signal) examples:

-  There are some very interesting ideas here. thx for sharing.
-  Were there any issues with this? I’ll start verifying a few things in
   a bit.
-  Standup?

We will now use OpenAI’s ChatCompletion API with few-shot learning to
iterate over our complete set of training examples and label each as
positive or negative.

First, we will generate an instruction set for the model by building up
an array of messages in JSON. Each message object contains ``role`` and
``content`` properties. The ``role`` can be either ``system``, ``user``,
or ``assistant``.

The first message should always be from the ``system`` role and provide
general instructions to the model of its function. Following this,
message pairs of ``user`` and ``assistant`` should be added, where the
``user`` content is our example input and the ``assistant`` content is
our expected response from the API. The model uses these few-shot
training examples to help it determine our desired output.

Then, we append a final ``user`` message to the instruction set
containing the content we want evaluated.

Open the code folds below to view some example Python code for
performing this refinement.

.. container::

      **Tips & Warnings**

      -  This will cost a fair amount on OpenAI. A rough estimate is $50
         per 10,000 examples.
      -  This can take a long time to run to completion. The
         ChatCompletion API limits the number of tokens used per minute.
         In my experience, running 10,000 examples through this process
         takes about 8 hours.
      -  The code is written in blocks to run inside a Jupyter Notebook
         environment.
      -  If you want to run the code yourself, you will need an OpenAI
         API key.

.. container:: cell

   .. code:: python

      %pip install openai backoff numpy pandas scikit-learn

      # We use the `backoff` library to retry requests that have failed due to a
      # rate-limit error. Despite this addition, sometimes the process stalls and
      # must be manually restarted. The code below appends to the output file instead
      # of replacing it, so that the process can be resumed after an error occurs.

      # numpy pandas scikit-learn are standard data science libraries we
      # will use later in the process for a variety of tasks

.. container:: cell

   .. code:: python

      import backoff, getpass, json, numpy, openai, pandas, sklearn, time

      openai.api_key = getpass.getpass('OpenAI API Key:')

.. container:: cell

   .. code:: python

      # This code assumes that you have a file named
      # `examples.jsonl`, which contains the full set
      # of training examples generated above.

      # get a total count of examples in the input file
      total_count = 0
      with open(f'examples.jsonl', 'r') as in_file:
          for line in in_file:
              total_count += 1

      # initialize a progress counter
      success_count = 0

      # build up the instruction set for few-shot learning

      # start with a `system` message that provides the general instructions to the model
      system_instructions = "You are a helpful assistant. Your job is to determine \
          if a prompt will help fine-tune a model. All prompts start with \
          'start -->' and end with: '\\n\\n###\\n\\n'. You should respond 'yes' if you \
          think the prompt has enough context to be helpful, or 'no' if not. No \
          explanation is needed. You should only respond with 'yes' or 'no'."
      instructions = [{"role": "system", "content": system_instructions}]

      # then add the positive and negative examples that we
      # manually pulled out of the full set
      pos = open(f'examples_pos.jsonl', 'r')
      neg = open(f'examples_neg.jsonl', 'r')

      while True:
          pos_line = pos.readline()
          neg_line = neg.readline()

          if (not pos_line) or (not neg_line):
              break

          pos_data = json.loads(pos_line)
          neg_data = json.loads(neg_line)

          # alternate adding positive and negative examples
          instructions.append({"role": "user", "content": f'start -->{pos_data["prompt"]}'})
          instructions.append({"role": "assistant", "content": "yes"})
          instructions.append({"role": "user", "content": f'start -->{neg_data["prompt"]}'})
          instructions.append({"role": "assistant","content": "no"})

      pos.close()
      neg.close()

      # setup a method to retry requests automatically
      @backoff.on_exception(backoff.expo, (openai.error.RateLimitError, openai.error.ServiceUnavailableError))
      def chat_with_backoff(**kwargs):
          # add an additional delay, because the first retry almost always fails
          time.sleep(1)
          try:
              return openai.ChatCompletion.create(**kwargs)
          except openai.error.InvalidRequestError:
              return None

.. container:: cell

   .. code:: python

      # if this code block stalls, you can restart it to resume processing.

      # If you get an error about too many tokens used, reduce the number of
      # positive and negative examples in your generated instructions.
      # Or try to summarize the positive & negative examples (manually or
      # with ChatGPT) to reduce their length.

      from IPython.display import clear_output

      count = 0
      with open(f'examples.jsonl', 'r') as in_file:
          with open(f'examples_refined.jsonl', 'a') as out_file:
              for line in in_file:
                  count +=1

                  # skip examples already processed on previous runs
                  if count < success_count:
                      continue

                  print(f'Currently processing line {count} of {total_count}')
                  clear_output(wait=True)

                  # get the next example from the file
                  data = json.loads(line)
                  prompt = data["prompt"]

                  # add the example to a copy of the instruction set
                  msgs = instructions.copy()
                  msgs.append({"role": "user", "content": f'start -->{prompt}'})

                  # send the request
                  res = chat_with_backoff(model = "gpt-3.5-turbo", messages = msgs)

                  # if the request failed for some reason, skip the example
                  if not res:
                      continue

                  # get the response and write the example back to disk
                  if res["choices"][0]["message"]["content"] == "no":
                      # for negative messages, re-write the completion as ` nil`
                      data["completion"] = " nil"
                  out_file.write(json.dumps(data) + '\n')
                  out_file.flush()

                  # save progress for restart
                  success_count = count

Example validation
------------------

Finally, we will use a CLI tool provided by OpenAI to validate our
training examples and split them into two files. The tool does the
following for us:

-  Ensures that all prompts end with the same suffix.
-  Removes examples that use too many tokens.
-  Deletes duplicated examples.

We can run the CLI tool directly from a Python Jupyter Notebook with the
code below.

.. container:: cell

   .. code:: python

      from types import SimpleNamespace

      args = SimpleNamespace(file='examples_refined.jsonl', quiet=True)
      openai.cli.FineTune.prepare_data(args)

The output of the above command should be two files:

-  ``examples_refined_prepared_train.jsonl`` -> We will use this to
   fine-tune our model
-  ``examples_refined_prepared_valid.jsonl`` -> We will use this to
   validate our fine-tuned model

.. container::

      **Important**

      The output from the CLI tool will include a command for starting a
      model fine-tuning. I recommend you skip those instructions and use
      mine below instead.

.. _model-fine-tuning-1:

Model Fine-Tuning
=================

Now that we have refined training examples, we can fine-tune a model.

Upload training data
--------------------

First, we upload the refined examples to OpenAI. We must ensure the file
has been successfully uploaded before moving on to the next step.

.. container:: cell

   .. code:: python

      training_file_name = "examples_refined_prepared_train.jsonl"

      # start the file upload
      training_file_id = openai.cli.FineTune._get_or_upload(training_file_name, True)

      # Poll and display the upload status until it finishes
      while True:
          time.sleep(2)
          file_status = openai.File.retrieve(training_file_id)["status"]
          print(f'Upload status: {file_status}')
          if file_status in ["succeeded", "failed", "processed"]:
              break

Create a fine-tuning job
------------------------

Next, we create a fine-tuning job using the file_id from the upload.

When doing fine-tuning, you need to choose a base model to start from.
The current options are:

-  ``ada`` -> Capable of very simple tasks, usually the fastest model in
   the GPT-3 series, and lowest cost.
-  ``babbage`` -> Capable of straightforward tasks. Very fast and low
   cost.
-  ``curie`` -> Very capable, but faster and lower cost than Davinci.
-  ``davinci`` -> Most capable GPT3 model. It is more expensive to train
   and run in production.

You must also choose the number of epochs to train the model for. An
epoch refers to one full cycle through the training dataset.

The cost of fine-tuning is based on the base model, the number of
examples, and the number of epochs you will run. Generally, doubling the
number of epochs doubles the training cost. However, there is a
trade-off to consider here because cheaper base models will be cheaper
to run in production.

With the example set I was using for BeepGPT, I found that eight epochs
on ``curie`` produced a model with a similar capability as four on
``davinci``. Depending on your use case, you may or may not have a
similar result.

.. container:: cell

   .. code:: python

      create_args = {
          "training_file": training_file_id,
          "model": "davinci",
          "n_epochs": 4,
          "suffix": "beep-gpt"
      }

      # Create the fine-tune job and retrieve the job ID
      resp = openai.FineTune.create(**create_args)
      job_id = resp["id"]

Wait for the job to finish
--------------------------

After the fine-tuning job has been created, we need to wait for it to
start processing and then for it to finish.

Depending on the current backlog at OpenAI, I’ve seen that jobs can take
up to a dozen hours to start.

After the job starts successfully, you can see its status and wait for
it to finish. This can also take a long time. When using ``davinci``
with four epochs, I estimate about 1 hour per 1000 training examples.

.. container:: cell

   .. code:: python

      # Poll and display the fine-tuning status until it finishes
      from IPython.display import clear_output

      while True:
          time.sleep(5)
          job_details = openai.FineTune.retrieve(id=job_id)

          print(f'Job status: {job_details["status"]}')
          print(f'Job events: {job_details["events"]}')
          clear_output(wait=True)

          if job_details["status"] == "succeeded":
              model_id = job_details["fine_tuned_model"]
              print(f'Successfully fine-tuned model with ID: {model_id}')

          if job_details["status"] in ["failed", "succeeded"]:
              break

Using the fine-tuned model
--------------------------

Now that we have a finished model, we can try sending a few prompts and
see if it recommends alerting any users. We can use the validation file
for this.

See the `OpenAI
docs <https://platform.openai.com/docs/api-reference/completions/create>`__
for info on the parameters we send to the Completion API.

.. container:: cell

   .. code:: python

      # choose which row in the validation file to send
      row = 6

      count = 0
      with open(f'examples_refined_prepared_valid.jsonl', 'r') as in_file:
          for line in in_file:
              count +=1

              if count < row:
                  continue

              data = json.loads(line)
              prompt = data["prompt"]
              completion = data["completion"]

              # this is the text we send to the model for it to
              # determine if we should alert a user
              print(f'Prompt: {prompt}')

              # this is the user (or nil) we would have expected
              # for the response (from the validation file)
              print(f'Completion: {completion}')

              # this is the response from the model. The `text` field contains
              # the actual prediction. The `logprobs` array contains the
              # log-probability from the 5 highest potential matches.
              print(f'Prediction:')
              openai.Completion.create(model=model_id, prompt=prompt, max_tokens=1, logprobs=5, temperature=0)

Model Validation
================

You can run your model over the full validation data set to validate it.
Then, use a basic data science performance measurement to check the
quality of your model. I calculate the model’s `F1
Score <https://scikit-learn.org/stable/modules/generated/sklearn.metrics.f1_score.html#sklearn.metrics.f1_score>`__
in the example below, but you can use other performance indicators if
desired.

.. container:: cell

   .. code:: python

      with open(f'examples_refined_prepared_valid.jsonl', 'r') as in_file:
          with open(f'examples_refined_prepared_valid_pred.jsonl', 'w') as out_file:
              for line in in_file:
                  # for each example in the validation input file
                  # run the completion API using our fine-tuned model
                  # write the results to the output file
                  pred = openai.Completion.create(model=model_id, prompt=prompt, max_tokens=1, logprobs=5, temperature=0)
                  data["prediction"] = pred
                  out_file.write(json.dumps(data) + '\n')

.. container:: cell

   .. code:: python

      # load the results from above into a dataframe
      df = pandas.read_json(f'examples_refined_prepared_valid_pred.jsonl', lines=True)
      df["test"] = None
      df["pred"] = None

      # fill the test and pred columns in the dataframe
      # from the Completion API results
      for i in range(len(df)):
          completions = df['completion'][i].strip().split()
          df.at[i, "test"] = completions
          prediction = df['prediction'][i]
          if "choices" in prediction:
              predictions = prediction["choices"][0]["text"].strip().split()
              df.at[i, "pred"] = predictions

      # drop rows where "pred" is null
      df = df[df.pred.notnull()]

      # compute the 'macro' F1 score. also can try
      # the 'micro' or 'weighted' score based on need
      from sklearn.metrics import f1_score
      f1 = f1_score(df['test'], df['pred'], average='macro')
      f1

An F1 score is a measure of a model’s accuracy, and it takes into
account both precision and recall.

-  Precision is the number of true positive predictions divided by the
   total number of positive predictions. It measures how accurate the
   model’s positive predictions are.
-  Recall is the number of true positive predictions divided by the
   total number of positive cases. It measures how well the model
   identifies positive cases.

The F1 score is the harmonic mean of precision and recall. F1 scores
range from 0 to 1, with a score of 1 indicating perfect precision and
recall and 0 indicating poor performance. As a general rule of thumb, an
F1 score of 0.7 or higher is often considered good. [@f1score]

Conclusion
==========

As we wrap up this post, it’s evident that fine-tuning large language
models can be a promising endeavor, even for those of us without a Data
Science background. Through the example of
`BeepGPT <https://github.com/kaskada-ai/beep-gpt>`__, we saw that the
process, while requiring patience and iteration, can produce models that
offer valuable insights.

Key Takeaways
-------------

1. **Experimentation is vital**: Training a model that works efficiently
   requires much trial and error. As showcased with BeepGPT, sometimes
   the fifth attempt may be the charm!

2. **Experimentation is vital**: Training a model that works efficiently
   requires much trial and error. As showcased with BeepGPT, sometimes
   the fifth attempt may be the charm!

3. **Few-Shot Learning – A Game-Changer**: In the midst of refining
   BeepGPT, I stumbled upon an innovative trick: leveraging few-shot
   learning to elevate the quality of our training data. While it seemed
   unconventional at first, the results were staggering. This technique
   might just revolutionize the way we fine-tune LLMs in the future.

4. **Prioritize Data Quality**: Even with tricks up our sleeve, the core
   principle remains - garbage in equals garbage out. The essence of a
   model’s efficiency lies in the caliber of data it’s trained on.

5. **Comprehensive Training Examples**: Building and refining a large
   set of training examples ensures a model can predict accurately in
   real-world scenarios.

Next Steps
----------

1. **Deep Dive into Tools**: In future posts, I’ll explore
   `Kaskada <http://kaskada.io/kaskada>`__ more deeply, showcasing how
   it aids in simplifying the process of gathering and refining training
   examples.

2. **Optimization**: As technology evolves, so do the tools and
   strategies for model fine-tuning. I’ll explore strategies to optimize
   the training process, from reducing costs to increasing accuracy.

3. **Model Deployment**: With a validated model, the following steps
   involve deploying it into real-world applications. In upcoming posts,
   I’ll look at strategies and best practices for integrating models
   with various platforms.

4. **Feedback Loop**: Continuous improvement is a hallmark of successful
   machine learning. We’ll explore ways to gather feedback on model
   predictions, further refining and improving it over time.

In conclusion, the landscape of data science and machine learning is
more accessible than ever. With the right tools, patience, and
curiosity, even software engineers with minimal data science experience
can harness the power of advanced models to provide tangible value.
Whether you’re a seasoned pro or a newcomer like me, the journey of
discovery and innovation in this space is just beginning.

References
==========

.. container::
   :name: refs
