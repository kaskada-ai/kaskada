import random
import argparse
import io
import avro, avro.datafile
import pulsar
from avro.io import DatumWriter, BinaryEncoder

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Generate a random transaction message')
parser.add_argument('--user_id', type=int, help='User ID')
parser.add_argument('--product_id', type=int, help='Product ID')
parser.add_argument('--quantity', type=int, help='Quantity')
parser.add_argument('--total_spent', type=float, help='Total spent')

# Parse the command-line arguments
args = parser.parse_args()

# Read the Avro schema from a file
with open("transactions.avsc", "r") as f:
    schema_str = f.read()

# Parse the schema
schema = avro.schema.parse(schema_str)

# Generate a random transaction message
def random_transaction(user_id, product_id, quantity, total_spent):
    user_id = user_id or random.randint(1, 100)
    product_id = product_id or random.randint(1, 100)
    quantity = quantity or random.randint(1, 10)
    total_spent = total_spent or round(random.uniform(1, 100), 2)
    return {
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity,
        "total_spent": total_spent
    }

# Serialize the transaction message to Avro binary format
transaction = random_transaction(args.user_id, args.product_id, args.quantity, args.total_spent)
bytes_writer = io.BytesIO()
encoder = BinaryEncoder(bytes_writer)
writer = avro.datafile.DataFileWriter(bytes_writer, avro.io.DatumWriter(), schema)
writer.append(transaction)
writer.flush()

# Publish the serialized transaction message to the Pulsar topic
client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer("transactions")
producer.send(bytes_writer.getvalue())
client.close()
