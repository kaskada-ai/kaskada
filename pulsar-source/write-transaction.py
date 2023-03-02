import io
import avro, avro.datafile
from avro.io import DatumWriter, BinaryEncoder

# Read the Avro schema from a file
with open("transactions.avsc", "r") as f:
    schema_str = f.read()
schema = avro.schema.parse(schema_str)

# hardcoded test transaction
t = {
    "user_id": 1,
    "product_id": 11,
    "quantity": 111,
    "total_spent": 111.11
}

# Serialize the transaction message to Avro binary format
bytes_writer = io.BytesIO()
encoder = BinaryEncoder(bytes_writer)
writer = avro.datafile.DataFileWriter(bytes_writer, avro.io.DatumWriter(), schema)
writer.append(t)
writer.flush()

# write to a file
with open("transaction.avro", "wb") as f:
    f.write(bytes_writer.getvalue())
