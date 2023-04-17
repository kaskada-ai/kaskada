# Running a quick demo #

1. Create a Pulsar topic in Astra.  You need to do this explicitly, I used the GUI.
   My topic name is transactions, tenant is kaskada, namespace is default.

1. Download the Astra connection bundle named client.conf, and export its
   location as an env var.  pulsar-admin and random-transaction.py will both use this.
    ```
    $ export PULSAR_CLIENT_CONF=/Users/jonathan/Downloads/client.conf
     ```

1. upload the schema to the “transactions” topic
    ```
   $ python schema-encode.py transactions.avsc > transactions.json
   $ ~/Projects/apache-pulsar-2.11.0/bin/pulsar-admin schemas upload --filename /Users/jonathan/Projects/kaskada/pulsar-source/transactions.json kaskada/default/transactions
    ```
    Java will spit out a bunch of warnings that you can ignore.  You can verify that it worked from the Astra GUI.
   
1. create some sample messages.  Prepare will not block once it
   gets to the end of the topic, so produce these before invoking prepare
    ```
   $ python random-transaction.py --topic kaskada/default/transactions
   ```
    [repeat as desired]

1. Prepare the messages!
    ```
   $ export PULSAR_TENANT=kaskada
   $ export PULSAR_TOPIC=transactions
   $ export PULSAR_SUBSCRIPTION=test-sub-1
   $ mkdir prepared/
   $ cargo run --bin sparrow-main -- prepare --schema=transactions-schema.yaml --table=Transactions --output-path=prepared pulsar
   ```
