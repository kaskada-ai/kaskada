This is a simple test of Sparrow through its CLI interface.

1. Get the data from Kaggle: https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data
   We need the transactions.csv file.
2. Prepare the CSV file. This will create a corresponding Parquet file with
   all the events correctly sorted:
   cargo run prepare --schema=schema1.yaml --table=Transaction --output-path=prepared/ input/transactions.csv
3. Run the query:
   cargo run batch --schema=schema1.yaml --script=query-complex.yaml

