import random
import argparse
import io
import os
import avro, avro.datafile
import pulsar
from pulsar import AuthenticationToken
from avro.io import DatumWriter, BinaryEncoder

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Generate a random transaction message')
parser.add_argument('--user_id', type=int, help='User ID')
parser.add_argument('--product_id', type=int, help='Product ID')
parser.add_argument('--quantity', type=int, help='Quantity')
parser.add_argument('--total_spent', type=float, help='Total spent')
parser.add_argument('--topic', type=str, help='Pulsar topic name', default='transactions')

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

# Read the Pulsar configuration file from the environment variable
pulsar_config_file = os.environ['PULSAR_CLIENT_CONF']
pulsar_config = {}
with open(pulsar_config_file, 'r') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#'):
            key, value = line.split('=', 1)
            pulsar_config[key.strip()] = value.strip()

# Connect to the Pulsar cluster using the URL from the config file
pulsar_url = pulsar_config.get('brokerServiceUrl', 'pulsar://localhost:6650')

# Get the JWT token from the config file
auth_params = pulsar_config.get('authParams', None)
if auth_params and auth_params.startswith('token:'):
    token = auth_params[len('token:'):].strip()
else:
    token = None

# Use the JWT token for authentication
authentication = AuthenticationToken(token) if token else None
client = pulsar.Client(pulsar_url, authentication=authentication)

producer = client.create_producer(args.topic)
producer.send(bytes_writer.getvalue())
client.close()
