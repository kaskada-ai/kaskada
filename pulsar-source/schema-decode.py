import sys
import json
import avro.schema

if len(sys.argv) < 2:
    print("Usage: python schema-decode.py <json_file>")
    sys.exit(1)
json_file = sys.argv[1]

with open(json_file, 'r') as f:
    schema_dict = json.load(f)

schema_json_str = schema_dict["schema"]
schema = avro.schema.parse(schema_json_str)

print(json.dumps(schema.to_json(), indent=2))
