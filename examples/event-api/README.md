# Example: Embedded event-processing API

This example shows the use of Kaskada for implementing real-time, low-latency event processing 
as part of a service implemented in Python.

## Running

Ensure your environment is setup. Using a virtual environment is reccommended

```sh
pip install -r requirements.txt
```

Next, run the service   

```sh
python server.py
```

You should see logs similar to "Waiting for events...".

Now, in a separate terminal/process, you can start sending JSON events to the server.
You'll see aggregated feature values in the response body:

```sh
curl -H "Content-Type: application/json" localhost:8080 -d '{"user": "me"}'
# > {"count": 1, "count_1m": 1}
```