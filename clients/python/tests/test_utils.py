import pytest

import fenlmagic.utils as utils
import kaskada.query as query


def test_arg_to_response_type():
    csv_type = "csv"
    expected = query.ResponseType.FILE_TYPE_CSV
    assert utils.arg_to_response_type(csv_type) == expected

    parquet_type = "parquet"
    expected = query.ResponseType.FILE_TYPE_PARQUET
    assert utils.arg_to_response_type(parquet_type) == expected

    with pytest.raises(Exception):
        invalid_type = "xlsx"
        utils.arg_to_response_type(invalid_type) == expected
