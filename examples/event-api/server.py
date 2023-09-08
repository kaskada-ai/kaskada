#!/usr/bin/env python

import json, asyncio, time, uuid, asyncio
import kaskada as kd
from aiohttp import web

async def main():
    kd.init_session()

    start = time.time()
    requestmap = dict()

    # Initialize event source with schema from historical data.
    events = kd.sources.PyDict(
        rows = [{"ts": start, "user": "user_1", "request_id": "12345678-1234-5678-1234-567812345678"}],
        time_column = "ts",
        key_column = "user",
        time_unit = "s",
        retained=False,
    )

    # Compute features over events
    output = (kd.record({
        "response": kd.record({
            "count": events.count(),
            "count_1m": events.count(window=kd.windows.Since.minutely())
        }),
        "request_id": events.col("request_id"),
        "ts": events.col("ts"),
    }))

    # Receive JSON messages in real-time
    async def handle_http(req: web.Request) -> web.Response:
        data = await req.json()

        # Add the current time to the event
        data["ts"] = time.time()

        # Create a future so the aggregated result can be returned in the API response
        request_id = str(uuid.uuid4())
        requestmap[request_id] = asyncio.Future()
        data["request_id"] = request_id

        # Send the event to Kaskada to be processed as a stream
        print(f"Got data: {data}")
        events.add_rows(data)

        # Wait for the response to be completed by the Kaskada handler
        print(f"Waiting for response")
        resp = await requestmap[request_id]

        # Return result as the response body
        print(f"Sending response: {resp}")
        return web.Response(text = json.dumps(resp))

    # Setup the async web server
    app = web.Application()
    app.router.add_post('/', handle_http)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()


    # Handle each conversation as it occurs
    print(f"Waiting for events...")
    async for row in output.run(materialize=True).iter_rows_async():
        try:
            # Ignore historical rows
            if row["ts"] <= start:
                continue

            print(f"Recieved from K*: {row}")

            request_id = row["request_id"]
            fut = requestmap.pop(request_id, None)
            if fut == None:
                print(f"Unrecognized request_id: {request_id}")
                continue

            fut.set_result(row["response"])

        except Exception as e:
            print(f"Failed to handle live event from Kaskada: {e}")

    # Wait for web server to terminate gracefully
    await runner.cleanup()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
