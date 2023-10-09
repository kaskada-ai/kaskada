import kaskada as kd
import pytest


@pytest.mark.skip("temporary skip, failing windows build")
async def test_read_parquet(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases_part1.parquet",
        time_column="purchase_time",
        key_column="customer_id",
    )
    golden.jsonl(source)

    await source.add_file("../testdata/purchases/purchases_part2.parquet")
    golden.jsonl(source)


@pytest.mark.skip("temporary skip, failing windows build")
async def test_read_parquet_with_subsort(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases_part1.parquet",
        time_column="purchase_time",
        key_column="customer_id",
        subsort_column="subsort_id",
    )
    golden.jsonl(source)

    await source.add_file("../testdata/purchases/purchases_part2.parquet")
    golden.jsonl(source)


# Verifies that we drain the output and progress channels correctly.
#
# When the parquet file contains more rows than
# (CHANNEL_SIZE / MAX_BATCH_SIZE), the channels previously filled
# up, causing the sender to block. This test verifies that the
# channels correctly drain, allowing the sender to continue.
# See https://github.com/kaskada-ai/kaskada/issues/775
@pytest.mark.skip("temporary skip, failing windows build")
async def test_large_parquet_file(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/parquet/purchases_100k.parquet",
        time_column="time",
        key_column="user",
    )
    user = source.col("user")
    amount = source.col("amount")

    # Add a filter to reduce the output file size while ensuring the entire
    # file is still processed
    predicate = user.eq("5fec83d4-f5c6-4943-ab05-2b6760330daf").and_(amount.gt(490))
    golden.jsonl(source.filter(predicate))


@pytest.mark.skip("temporary skip, failing windows build")
async def test_time_column_as_float_can_cast_s(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases_float.parquet",
        time_column="purchase_time",
        key_column="customer_id",
        time_unit="s",
    )
    golden.jsonl(source)


@pytest.mark.skip("temporary skip, failing windows build")
async def test_time_column_as_float_can_cast_ns(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases_float.parquet",
        time_column="purchase_time",
        key_column="customer_id",
    )
    golden.jsonl(source)


@pytest.mark.skip("temporary skip, failing windows build")
async def test_with_space_in_path(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases part1.parquet",
        time_column="purchase_time",
        key_column="customer_id",
        subsort_column="subsort_id",
    )
    golden.jsonl(source)


@pytest.mark.skip("temporary skip, failing windows build")
async def test_with_trailing_slash(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases part1.parquet/",
        time_column="purchase_time",
        key_column="customer_id",
        subsort_column="subsort_id",
    )
    golden.jsonl(source)


@pytest.mark.svc("minio")
async def test_read_parquet_from_s3_minio(minio, golden) -> None:
    # Upload a parquet file to minio for testing purposes
    (minio_host, minio_port) = minio
    import boto3

    aws_endpoint = f"http://{minio_host}:{minio_port}"
    # Defaults set in the `pytest-docker-fixtures`
    # https://github.com/guillotinaweb/pytest-docker-fixtures/blob/236fdc1b6a9db03640040af2baf3bd3dfcc8d187/pytest_docker_fixtures/images.py#L42
    aws_access_key_id = "x" * 10
    aws_secret_access_key = "x" * 10
    s3_client = boto3.client(
        "s3",
        endpoint_url=aws_endpoint,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        # aws_session_token=None,
        # config=boto3.session.Config(signature_version='s3v4'),
        # verify=False
    )
    s3_client.create_bucket(Bucket="test-bucket")
    s3_client.upload_file(
        "../testdata/purchases/purchases_part1.parquet",
        "test-bucket",
        "purchases_part1.parquet",
    )

    # This is a hack.
    # The session (which is only created once) will cache the S3 client.
    # This means that generally, changing the environment variables would
    # require creating a new session to be picked up.
    # Currently, we don't allow recreating the session (or scoping it).
    # We probably should.
    #
    # But... this still works. The trick is that the S3 client isn't
    # created until the first S3 URL is used. As long as we set the
    # environment variables before that, they will be picked up and
    # stored in the session appropriately.
    import os

    os.environ["AWS_ENDPOINT"] = aws_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    # Minio requires HTTP.
    os.environ["AWS_ALLOW_HTTP"] = "true"

    source = await kd.sources.Parquet.create(
        "s3://test-bucket/purchases_part1.parquet",
        time_column="purchase_time",
        key_column="customer_id",
    )
    golden.jsonl(source)
