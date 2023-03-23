import json
import avro.schema
import sys

if len(sys.argv) < 2:
    print("Usage: python schema-encode.py <avsc_file>")
    sys.exit(1)
avsc_file = sys.argv[1]

with open(avsc_file, 'r') as f:
    schema_str = f.read()

schema = avro.schema.parse(schema_str)
schema_dict = {
    "type": "AVRO",
    "schema": json.dumps(schema.to_json()),
    "properties": {}
}

print(json.dumps(schema_dict, indent=2))
