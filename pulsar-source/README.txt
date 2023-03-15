Running a quick demo:

# upload the schema to the “transactions” topic
$ python schema-encode.py transactions.avsc > transactions.json 
$ ~/Projects/apache-pulsar-2.11.0/bin/pulsar-admin schemas upload --filename /Users/jonathan/Projects/kaskada/pulsar-source/transactions.json transactions

# create some sample messages.  Prepare will not block once it
# gets to the end of the topic, so produce these before invoking prepare
$ python random-transaction.py
[repeat as desired]

# produce!
cargo run --bin sparrow-main -- prepare --schema=transactions-schema.yaml --table=Transactions --output-path=prepared pulsar://localhost:6650/public/default/transactions

