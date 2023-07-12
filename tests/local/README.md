# Local testing of the full stack

## With remote object storage

### With AWS S3

* created a test bucket in a region, with a name
  * copy the `testdata` folder into the bucket
  * also create a `data` folder in the bucket

* created an IAM user with the following in-line policy to access the bucket:

```
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "ListBucket",
			"Effect": "Allow",
			"Action": [
				"s3:ListBucket",
				"s3:GetBucketLocation",
				"s3:*Multipart*"
			],
			"Resource": "arn:aws:s3:::arn:aws:s3:::<bucket_name>"
		},
		{
			"Sid": "ReadTestData",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:ListBucket"
			],
			"Resource": "arn:aws:s3:::arn:aws:s3:::<bucket_name>/testdata/*"
		},
		{
			"Sid": "ReadWriteData",
			"Effect": "Allow",
			"Action": [
				"s3:DeleteObject",
				"s3:GetObject",
				"s3:ListBucket",
				"s3:PutObject"
			],
			"Resource": "arn:aws:s3:::arn:aws:s3:::<bucket_name>/data/*"
		}
	]
}
```

* Added the folling policy to the bucket to allow the test user to access it:
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowTestUser",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::<aws_account>:user/<user_name>"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::<bucket_name>",
                "arn:aws:s3:::<bucket_name>/*"
            ]
        }
    ]
}
```

* Created access key and secret for the user

* Started Wren with the following command:
    (from `wren` folder)
    ```
    DB_IN_MEMORY=false \
    DB_PATH=$(pwd)/../tests/local/data/kaskada.db \
    DEBUG=true \
    OBJECT_STORE_TYPE=s3 \
    OBJECT_STORE_BUCKET=<bucket_name> \
    OBJECT_STORE_PATH=/data \
    AWS_REGION=<region> \
    AWS_ACCESS_KEY_ID=<user_access_key> \
    AWS_SECRET_ACCESS_KEY=<user_secret> \
    go run main.go
    ```

* Started Sparrow with the following command:
    (from root of repo)
    ```
    AWS_REGION=<region> \
    AWS_ACCESS_KEY_ID=<user_access_key> \
    AWS_SECRET_ACCESS_KEY=<user_secret> \
    cargo run -p sparrow-main serve
    ```

* Started build and local jupyter as described in the readme: clients/python/README.md

* created a local session and experimented:

    ```
    from kaskada.api.remote_session import RemoteBuilder
    from kaskada import table

    session = RemoteBuilder("localhost:50051", False).build()

    table.create_table(
        table_name = "Purchase",
        time_column_name = "purchase_time",
        entity_key_column_name = "customer_id",
    )

    # load remote file into table
    table.load(table_name="Purchase", file="s3://<bucket_name>/testdata/purchases/purchases_part1.parquet")

    # load remote public file into table (doesn't work yet)
    # table.load(table_name="Purchase", file="s3://kaskada-public-assets/example_data/purchases_sorted.parquet")

    # load local file into table
    table.load(table_name="Purchase", file="file:///<path_to_repo>/testdata/purchases/purchases_part3.parquet")

    table.get_table("Purchase")

    %load_ext fenlmagic
    ```

    ```
    %%fenl
    Purchase
    ```

* Confirmed prepare cache and results were on S3

