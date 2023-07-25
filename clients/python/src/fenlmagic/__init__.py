"""Integration with IPython (Notebooks) for Fenl.

    Raises:
        UsageError: when improper arguments are provided
"""
# pylint: disable=no-member
from __future__ import print_function

import logging
import os
import sys
from typing import Optional

import IPython
import pandas
from google.protobuf import wrappers_pb2 as wrappers
from IPython.core.error import UsageError
from IPython.core.magic import Magics, cell_magic, line_cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

import kaskada.kaskada.v1alpha.query_service_pb2 as query_pb
from fenlmagic.utils import arg_to_response_type
from kaskada import client, query

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryResult(object):
    dataframe: pandas.DataFrame = None

    def __init__(
        self, expression: str, query_response: query_pb.CreateQueryResponse  # type: ignore[valid-type]
    ):
        self.expression = expression
        self.query_response = query_response

    def set_dataframe(self, dataframe: pandas.DataFrame):
        self.dataframe = dataframe


@magics_class
class FenlMagics(Magics):
    def __init__(self, shell, client: Optional[client.Client]):
        super(FenlMagics, self).__init__(shell)
        logger.info("extension loaded")
        self.client = client

    @magic_arguments()
    @argument(
        "--changed-since-time",
        help="Time bound (inclusive) after which results will be produced.",
    )
    @argument(
        "--data-token",
        help="A data token to run queries against. Enables repeatable queries.",
    )
    @argument("--debug", default=False, help="Shows debugging information")
    @argument(
        "--dry-run",
        default=False,
        help="When `True`, the query is validated and if there are no errors, the resultant schema is returned. \
            No actual computation of results is performed.",
    )
    @argument(
        "--experimental",
        help="When `True`, then experimental features are allowed. \
            Data returned when using this flag is not guaranteed to be correct.",
    )
    @argument(
        "--output",
        default="parquet",
        help='Output format for the query results. One of "parquet", or "csv".',
    )
    @argument(
        "--dataframe",
        default=True,
        help="When `True`, will render the results as a dataframe. Otherwise only returns the response.",
    )
    @argument(
        "--preview-rows",
        help="Produces a preview of the data with at least this many rows.",
    )
    @argument(
        "--result-behavior",
        help='Determines which results are returned. \
            Either "all-results" (default), or "final-results" which returns only the final values for each entity.',
    )
    @argument(
        "--final-time",
        help="Produces final values for each entity at the given timestamp.",
    )
    @argument("--var", help="Assigns the body to a local variable with the given name")
    @cell_magic
    @line_cell_magic
    def fenl(self, arg, cell=None):
        "fenl query magic"

        args = parse_argstring(self.fenl, arg)
        dry_run = test_arg(clean_arg(str(args.dry_run)), "true")
        render_dataframe = test_arg(clean_arg(str(args.dataframe)), "true")
        response_as = arg_to_response_type(clean_arg(args.output))
        preview_rows = clean_arg(args.preview_rows)
        var = clean_arg(args.var)

        final_result_time = clean_arg(args.final_time)
        result_behavior = clean_arg(args.result_behavior)

        # Assert the result behaviors are valid
        if final_result_time is not None:
            if result_behavior is not None and not test_arg(
                result_behavior, "final-results"
            ):
                raise UsageError(
                    '--final-time must be used with result-behavior "final-results"'
                )
            result_behavior = "final-results"

        if cell is None:
            expression = arg
        else:
            expression = cell.strip()

        limits = {}
        if preview_rows:
            limits["preview_rows"] = int(preview_rows)

        data_token_id = clean_arg(args.data_token)
        data_token_id = (
            wrappers.StringValue(value=data_token_id) if data_token_id else None
        )

        try:
            resp = query.create_query(
                expression=expression,
                result_behavior=result_behavior,
                dry_run=dry_run,
                experimental=test_arg(clean_arg(str(args.experimental)), "true"),
                data_token_id=data_token_id,
                changed_since_time=clean_arg(args.changed_since_time),
                final_result_time=clean_arg(args.final_time),
                limits=query_pb.Query.Limits(**limits),
                response_as=response_as,
                client=self.client,
            )
            query_result = QueryResult(expression, resp)
            if not dry_run and render_dataframe:
                output_paths = (
                    query_result.query_response.destination.object_store.output_paths.paths
                )
                if len(output_paths) > 0:
                    dataframes = []
                    for parquet_output_path in output_paths:
                        if response_as == query.ResponseType.FILE_TYPE_PARQUET:
                            df = pandas.read_parquet(
                                parquet_output_path, engine="pyarrow"
                            )
                            dataframes.append(df)
                        elif response_as == query.ResponseType.FILE_TYPE_CSV:
                            df = pandas.read_csv(parquet_output_path, engine="pyarrow")
                            dataframes.append(df)
                        else:
                            raise NotImplementedError(
                                f"unknown response type: {response_as}", response_as
                            )
                    query_result.set_dataframe(pandas.concat(dataframes))

            if var is not None:
                IPython.get_ipython().push({var: query_result})

            return query_result
        except Exception as e:
            raise UsageError(e)


def load_ipython_extension(ipython, client: Optional[client.Client] = None):
    magics = FenlMagics(ipython, client)
    ipython.register_magics(magics)


def clean_arg(arg: str) -> str:
    """
    Strips quotes and double-quotes from passed arguments

    Args:
        arg (str): input argument

    Returns:
        str: cleaned output
    """
    if arg is not None:
        return arg.strip('"').strip("'")
    else:
        return None


def test_arg(arg: str, val: str) -> bool:
    """
    Compares an argument against a test value.
    Returns true when the two strings are equal in a case-insensitive and dash/underscore-insensitive manner.

    Args:
        arg (str): the argument to test
        val (str): the comparison value

    Returns:
        bool: The comparision result.
    """
    if arg is not None and val is not None:
        return arg.replace("_", "-").lower() == val.replace("_", "-").lower()
