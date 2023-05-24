## Materialization Process

This command allows you to spin up a long-lived materialization process that indefinitely reads messages from a Pulsar stream and produces results to a destination of choice. 

#### Local Setup
1. Starting in the `examples/materialize_cli/` directory, download the Pulsar distribution and run a standalone cluster: https://pulsar.apache.org/docs/3.0.x/getting-started-standalone/
2. From the pulsar directory, create an input topic: 

  `bin/pulsar-admin topics create persistent://public/default/my_input_topic`

3. Using the provided schema definition, upload a schema definition to the topic:

  `bin/pulsar-admin schemas upload -f ../topic_schema_definition public/default/my_input_topic`

  * This schema definition is an example, and matches the other provided schemas and messages. Modifying this schema requires modifying the others as well
4. Check the schema is set: 

  `bin/pulsar-admin schemas get persistent://public/default/my_input_topic`

#### Running Materialize
1. Inspect the `materialize_schema.yaml` to ensure the schema is as expected.
2. Inspect the `materialize_script.yaml` to see the example query and
   destination. The example provided produces results to a local directory in
csv format.
3. From the Kaskada root directory, run the materialize command:

  `cargo run --bin sparrow-main -- materialize --schema=examples/materialize_cli/materialize_schema.yaml --script=examples/materialize_cli/materialize_script.yaml`

4. The expected log output should show the consumer attempting to read messages from your stream. Look for the log: "Writing to output file: <path_to_your_repo>/materialize_output/<output_file>". This is where results will be materialized to.
5. In another terminal window, go to your pulsar directory, and publish messages to your input topic:

  `bin/pulsar-client produce persistent://public/default/my_input_topic -f "../messages/msg1.avro,../messages/msg2.avro,../messages/msg3.avro"`
 
6. Back in the materialization window, logs should indicate that rows were processed. 
7. Check your output file to verify rows are produced.


#### Late Data Handling
You may notice that only 2 rows were written to your output file, but 3 messages were sent to the topic. This is due to how Kaskada handles late data - it keeps an input buffer of messages that advances the watermark (a timestamp) as messages are read. Once the watermark advances past the time an input event occurred, that message can be processed. This parameter is configured from the `bounded_lateness_ns` integer in the `materialize_script.yaml`. 

#### Updating your schema
1. Update the topic schema definition to the desired schema.
2. Either create a new topic and assign the schema to the new topic, or delete and re-upload the new schema. 

  `bin/pulsar-admin schemas delete persistent://public/default/my_input_topic`

3. Update the schema defined in `materialize_schema.yaml`. Datatype definitions can be found in the protobuf registry defined here: https://buf.build/kaskada/kaskada/docs/main:kaskada.kaskada.v1alpha#kaskada.kaskada.v1alpha.DataType.PrimitiveType
4. Create new messages to produce to your topic. As an example, this can be done by creating the corresponding messages as json files, then converting to avro using avro-tools:

  `avro-tools fromjson --schema-file schema_filemsg.json > msg.avro`
 
Example schema_file:
```
{"type": "record", "name": "MyRecord", "fields": [{"name": "time", "type":"long"}, {"name": "id", "type": "long"}, {"name": "my_val", "type": "long"}]}
```
5. Follow the process to run the materialize command above.

#### Updating your query
1. Open the `materialize_script.yaml`. 
2. Add formulas and modify the query as shown.








