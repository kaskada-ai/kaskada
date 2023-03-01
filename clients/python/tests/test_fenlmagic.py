from pathlib import Path
from unittest.mock import patch

from google.protobuf import timestamp_pb2, wrappers_pb2

import fenlmagic
import kaskada.client
import kaskada.kaskada.v1alpha.common_pb2 as common_pb
import kaskada.kaskada.v1alpha.query_service_pb2 as query_pb


@patch("kaskada.client.Client")
def test_fenl_with_defaults(mockClient):
    expression = "test_with_defaults"
    args = ""
    expected_request = query_pb.CreateQueryRequest(
        query=query_pb.Query(
            expression=expression,
            as_files=common_pb.AsFiles(file_type=common_pb.FILE_TYPE_PARQUET),
            result_behavior="RESULT_BEHAVIOR_ALL_RESULTS",
            limits=query_pb.Query.Limits(),
        ),
        query_options=query_pb.QueryOptions(presign_results=True),
    )

    magic = fenlmagic.FenlMagics(None, client=mockClient)
    magic.fenl(args, expression)

    mockClient.query_stub.CreateQuery.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_fenl_with_all_args(mockClient):
    expression = "test_with_args"
    args = f"--changed-since-time 2022-01-01T10:00:20.021-05:00 --data-token data_token_arg --dry-run tRue --experimental trUE --output cSv --preview-rows 42 --result-behavior fiNal-REsults --final-time 2023-01-01T10:00:20.036-05:00"

    expected_request = query_pb.CreateQueryRequest(
        query=query_pb.Query(
            expression=expression,
            as_files=common_pb.AsFiles(file_type=common_pb.FILE_TYPE_CSV),
            changed_since_time=timestamp_pb2.Timestamp(
                seconds=1641049220, nanos=21000000
            ),
            final_result_time=timestamp_pb2.Timestamp(
                seconds=1672585220, nanos=36000000
            ),
            result_behavior="RESULT_BEHAVIOR_FINAL_RESULTS_AT_TIME",
            limits=query_pb.Query.Limits(preview_rows=42),
            data_token_id=wrappers_pb2.StringValue(value="data_token_arg"),
        ),
        query_options=query_pb.QueryOptions(
            dry_run=True,
            experimental_features=True,
            presign_results=True,
        ),
    )

    magic = fenlmagic.FenlMagics(None, client=mockClient)
    magic.fenl(args, expression)

    mockClient.query_stub.CreateQuery.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )
