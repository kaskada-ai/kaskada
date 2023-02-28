import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Union

import google.protobuf.wrappers_pb2 as wrappers
import grpc
import pandas as pd

import kaskada.kaskada.v1alpha.common_pb2 as common_pb
import kaskada.kaskada.v1alpha.table_service_pb2 as table_pb
from kaskada.client import Client, get_client
from kaskada.utils import handleException, handleGrpcError

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


def get_table_name(
    table: Union[
        table_pb.Table, table_pb.CreateTableResponse, table_pb.GetTableResponse, str
    ]
) -> str:
    """
    Gets the table name from either the table protobuf, create table response, get table response, or a string

    Args:
        table (Union[table_pb.Table, table_pb.CreateTableResponse, table_pb.GetTableResponse, str]):
            The target table object

    Returns:
        str: The table name (None if unable to match)
    """
    table_name = None
    if isinstance(table, table_pb.Table):
        table_name = table.table_name
    elif isinstance(table, table_pb.CreateTableResponse) or isinstance(
        table, table_pb.GetTableResponse
    ):
        table_name = table.table.table_name
    elif isinstance(table, str):
        table_name = table

    if table_name is None:
        raise Exception(
            "invalid table parameter provided. the table parameter must be the table object, table response from the \
                SDK, or the table name"
        )

    return table_name


def list_tables(
    search: Optional[str] = None, client: Optional[Client] = None
) -> table_pb.ListTablesResponse:
    """
    Lists all tables the user has access to

    Args:
        search (str, optional): The search parameter to filter list by. Defaults to None.
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        table_pb.ListTablesResponse: Response from the API
    """
    try:
        client = get_client(client)
        req = table_pb.ListTablesRequest(
            search=search,
        )
        logger.debug(f"List Tables Request: {req}")
        return client.table_stub.ListTables(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def get_table(
    table: Union[
        table_pb.Table, table_pb.CreateTableResponse, table_pb.GetTableResponse, str
    ],
    client: Optional[Client] = None,
) -> table_pb.GetTableResponse:
    """
    Gets a table by name

    Args:
        table (Union[Table, CreateTableResponse, GetTableResponse, str]): The target table object
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        table_pb.GetTableResponse: Response from the API
    """
    try:
        table_name = get_table_name(table)
        client = get_client(client)
        req = table_pb.GetTableRequest(table_name=table_name)
        logger.debug(f"Get Tables Request: {req}")
        return client.table_stub.GetTable(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def create_table(
    table_name: str,
    time_column_name: str,
    entity_key_column_name: str,
    subsort_column_name: Optional[str] = None,
    grouping_id: Optional[str] = None,
    client: Optional[Client] = None,
) -> table_pb.CreateTableResponse:
    """
    Creates a table

    Args:
        table_name (str):
            The name of the table
        time_column_name (str):
            The time column
        entity_key_column_name (str):
            The entity key column
        subsort_column_name (str, optional):
            The subsort column. Defaults to None and Kaskada will generate a subsort column for the data.
        grouping_id (str, optional):
            The grouping id. Defaults to None.
        client (Client, optional):
            The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        table_pb.CreateTableResponse: Response from the API
    """
    try:
        client = get_client(client)
        table_args: Dict[str, Any] = {
            "table_name": table_name,
            "time_column_name": time_column_name,
            "entity_key_column_name": entity_key_column_name,
        }
        if grouping_id is not None:
            table_args["grouping_id"] = grouping_id
        if subsort_column_name is not None:
            table_args["subsort_column_name"] = wrappers.StringValue(
                value=subsort_column_name
            )
        req = table_pb.CreateTableRequest(table=table_pb.Table(**table_args))
        logger.debug(f"Create Tables Request: {req}")
        return client.table_stub.CreateTable(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def delete_table(
    table: Union[
        table_pb.Table, table_pb.CreateTableResponse, table_pb.GetTableResponse, str
    ],
    client: Optional[Client] = None,
    force: bool = False,
) -> table_pb.DeleteTableResponse:
    """
    Deletes a table referenced by name

    Args:
        table (Union[Table, CreateTableResponse, GetTableResponse, str]): The target table object
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        table_pb.DeleteTableResponse: Response from the API
    """

    try:
        table_name = get_table_name(table)
        client = get_client(client)
        req = table_pb.DeleteTableRequest(table_name=table_name, force=force)
        logger.debug(f"Delete Tables Request: {req}")
        return client.table_stub.DeleteTable(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def load(
    table_name: str,
    file: str,
    client: Optional[Client] = None,
) -> table_pb.LoadDataResponse:
    """Loads a local file to a table. The type of file is inferred from the extension.

    Args:
        table_name (str): The name of the target table
        file (str): The path to a file (absolute or relative)
        client (Optional[Client], optional): The Kaskada Client. Defaults to None.

    Returns:
        table_pb.LoadDataResponse: Response from API
    """
    try:
        client = get_client(client)
        path = Path(file).absolute()
        extension = path.suffix.lower()
        file_type = ""
        if extension == ".parquet":
            file_type = "FILE_TYPE_PARQUET"
        elif extension == ".csv":
            file_type = "FILE_TYPE_CSV"
        else:
            raise ValueError(
                "invalid file type provided. only .parquet or .csv accepted"
            )

        input = common_pb.FileInput(
            file_type=file_type,
            uri=f"file://{path}",
        )
        req = table_pb.LoadDataRequest(table_name=table_name, file_input=input)
        logger.debug(f"Load Tables Request: {req}")
        return client.table_stub.LoadData(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def load_dataframe(
    table_name: str,
    dataframe: pd.DataFrame,
    client: Optional[Client] = None,
    engine: str = "pyarrow",
) -> table_pb.LoadDataResponse:
    """
    Loads a dataframe to a table.

    This converts the dataframe to a Parquet file first, and then loads that file.
    If your dataframe was loaded from a Parquet file (or other supported format),
    it would be better to load that directly with :func:`~kaskada.table.load`

    Args:
        table_name (str): The name of the target table
        dataframe (pd.DataFrame): The target dataframe to load
        client (Optional[Client], optional): The Kaskada Client. Defaults to None.
        engine (str, optional): The engine to convert the dataframe to parquet. Defaults to 'pyarrow'.

    Returns:
        table_pb.LoadDataResponse: Response from the API.
    """
    import tempfile

    temp_file = tempfile.NamedTemporaryFile(
        prefix="kaskada_", suffix=".parquet", delete=False
    )
    logger.debug(f"Created temporary file at: {temp_file.name}")
    dataframe.to_parquet(
        path=temp_file,
        engine=engine,
        allow_truncated_timestamps=True,
        use_deprecated_int96_timestamps=True,
    )
    temp_file.close()
    return load(table_name=table_name, file=temp_file.name, client=client)
