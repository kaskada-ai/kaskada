This is a simple test of Sparrow through its CLI interface.

### How to run it:

1. Get the data [from Kaggle](https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data). We need the transactions.csv file.

2. Prepare the CSV file. This will create a corresponding Parquet file with all the events correctly sorted:

   `mkdir prepared && cargo run prepare --schema=schema.yaml --table=Transaction --output-path=prepared/ input/transactions.csv`

3. Run the query. Here the output is specified in the query yaml:

   `cargo run batch --schema=schema.yaml --script=query-complex.yaml`
