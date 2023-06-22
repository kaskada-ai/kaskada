# Install: pip install pandas pyarrow faker
# Run: python generate.py --users 500 --items 500 --purchases 10000 --page_views 5000 --reviews 2500
import argparse
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Initialize the faker
fake = Faker()

def generate_and_write_data(event_type, n_events, users, items, file_name):
    print(f"Generating and writing {n_events} {event_type}...")
    events = []

    for i in range(n_events):
        time = fake.date_time_between_dates(datetime_start=datetime(2022,1,1), datetime_end=datetime(2022,12,31))
        user = random.choice(users)
        item = random.choice(items)

        if event_type == "purchases":
            amount = round(random.uniform(5, 500), 2)
            events.append([time, user, amount, item])
        elif event_type == "page_views":
            events.append([time, user, item])
        elif event_type == "reviews":
            rating = random.randint(1, 5)
            events.append([time, user, item, rating])

        # Print progress every 5000 events
        if (i + 1) % 5000 == 0:
            print(f"{i + 1} {event_type} generated...")

    # Create a DataFrame from the events
    if event_type == "purchases":
        df = pd.DataFrame(events, columns=['time', 'user', 'amount', 'item'])
    elif event_type == "page_views":
        df = pd.DataFrame(events, columns=['time', 'user', 'item'])
    elif event_type == "reviews":
        df = pd.DataFrame(events, columns=['time', 'user', 'item', 'rating'])

    # Write the DataFrame to a Parquet file
    print(f"Writing {n_events} {event_type} to {file_name}...")
    write_parquet(df, file_name)
    print(f"Finished writing {n_events} {event_type} to {file_name}.")

def write_parquet(df, filename):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename, allow_truncated_timestamps=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", help="Number of users", type=int, default=100)
    parser.add_argument("--items", help="Number of items", type=int, default=100)
    parser.add_argument("--purchases", help="Number of purchases", type=int, default=1000)
    parser.add_argument("--page_views", help="Number of page views", type=int, default=1000)
    parser.add_argument("--reviews", help="Number of reviews", type=int, default=500)
    args = parser.parse_args()

    users = [fake.uuid4() for _ in range(args.users)]
    items = [fake.uuid4() for _ in range(args.items)]

    generate_and_write_data("purchases", args.purchases, users, items, "purchases.parquet")
    generate_and_write_data("page_views", args.page_views, users, items, "page_views.parquet")
    generate_and_write_data("reviews", args.reviews, users, items, "reviews.parquet")