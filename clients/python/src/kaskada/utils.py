import datetime
import re
import unicodedata
from typing import Union

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from google.rpc import error_details_pb2
from grpc_status import rpc_status

import kaskada.kaskada.v1alpha.fenl_diagnostics_pb2 as kaskada_error_details_pb2


def normalize_col_name(column: str) -> str:
    """
    Normalize column name by replacing invalid characters with underscore
    strips accents and make lowercase
    """
    n = re.sub(r"[ ,;{}()\n\t=]+", "_", column.lower())
    return unicodedata.normalize("NFKD", n).encode("ASCII", "ignore").decode()


def get_timestamp(time: Union[str, datetime.datetime, None]):
    timestamp = None
    if isinstance(time, str):
        timestamp = Timestamp()
        timestamp.FromJsonString(time)
    elif isinstance(time, datetime.datetime):
        timestamp = Timestamp()
        timestamp.FromDatetime(time)
    elif time is not None:
        raise Exception(
            "Invalid type for timestamp. Expected `str` or `datetime.datetime`."
        )
    return timestamp


def handleGrpcError(rpc_error: grpc.Call):
    """
    All methods calling `handleGrpcError` do so after detecting a RpcError.  Currently
    all our grpc calls perform a `UnaryUnaryMultiCallable` method, docs linked here:
    https://grpc.github.io/grpc/python/_modules/grpc.html#UnaryUnaryMultiCallable

    Calling a `UnaryUnaryMultiCallable` method results in one of the following:
        Returns:
            The response value for the RPC.

        Raises:
            RpcError: Indicating that the RPC terminated with non-OK status. The
            raised RpcError will also be a grpc.Call for the RPC affording the RPC's
            metadata, status code, and details.

    Methods on a `grpc.Call` object include the following.  Full docs here:
    https://grpc.github.io/grpc/python/_modules/grpc.html#Call

        code(): The StatusCode value for the RPC.
        details(): The details string of the RPC.

    The `grpc_status.rpc_status.from_call(call)` method helps extract details
    objects from `grpc.Call` objects. Docs here:
    https://grpc.github.io/grpc/python/_modules/grpc_status/rpc_status.html#from_call

        Args:
            call: A grpc.Call instance.

        Returns:
            A google.rpc.status.Status message representing the status of the RPC.

        Raises:
            ValueError: If the gRPC call's code or details are inconsistent with the
            status code and message inside of the google.rpc.status.Status.
    """
    errorMessage = "An error occurred in your request.\n\tError Code: {}\n".format(
        rpc_error.code().name
    )
    try:
        status = rpc_status.from_call(rpc_error)
        if status:
            unpacked = []
            errorMessage += "\tError Message: {}\n".format(status.message)
            for detail in status.details:
                try:
                    val = unpack_details(detail)
                    unpacked.append(val)
                except Exception:
                    # maybe report back to wren in the future
                    pass
            if len(unpacked) > 0:
                errorMessage += "Details: \n"
                for val in unpacked:
                    errorMessage += "\t{}\n".format(val)
            raise Exception(errorMessage) from None
    except ValueError:
        # maybe report back to wren in the future
        pass
    errorMessage += "\tError Message: {}\n".format(rpc_error.details())
    raise Exception(errorMessage) from None


def unpack_details(grpc_detail):
    """Unpack a grpc status detail field (which is a 'google.protobuf.Any' type).
    `Unpack()` checks the descriptor of the passed-in message object against the stored one
    and returns False if they don't match and does not attempt any unpacking; True otherwise.
        Source:
            https://github.com/protocolbuffers/protobuf/blob/master/python/google/protobuf/internal/well_known_types.py#L81
            https://github.com/protocolbuffers/protobuf/blob/master/python/google/protobuf/internal/python_message.py#L1135

        Raises:
            google.protobuf.message.DecodeError: If it can't deserialize the message object

    `Is()` checks if the `Any` message represents the given protocol buffer type.
    """
    if grpc_detail.Is(error_details_pb2.BadRequest.DESCRIPTOR):
        val = error_details_pb2.BadRequest()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(error_details_pb2.PreconditionFailure.DESCRIPTOR):
        val = error_details_pb2.PreconditionFailure()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(error_details_pb2.RetryInfo.DESCRIPTOR):
        val = error_details_pb2.RetryInfo()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(error_details_pb2.DebugInfo.DESCRIPTOR):
        val = error_details_pb2.DebugInfo()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(error_details_pb2.QuotaFailure.DESCRIPTOR):
        val = error_details_pb2.QuotaFailure()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(error_details_pb2.RequestInfo.DESCRIPTOR):
        val = error_details_pb2.RequestInfo()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(error_details_pb2.ResourceInfo.DESCRIPTOR):
        val = error_details_pb2.ResourceInfo()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(error_details_pb2.Help.DESCRIPTOR):
        val = error_details_pb2.Help()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(error_details_pb2.LocalizedMessage.DESCRIPTOR):
        val = error_details_pb2.LocalizedMessage()
        grpc_detail.Unpack(val)
        return val
    elif grpc_detail.Is(kaskada_error_details_pb2.FenlDiagnostics.DESCRIPTOR):
        val = kaskada_error_details_pb2.FenlDiagnostics()
        grpc_detail.Unpack(val)
        error_msgs = []
        for fenl_diagnostic in val.fenl_diagnostics:
            error_msgs.append(fenl_diagnostic.formatted.replace("\n", "\n\t"))
        return "\n".join(error_msgs)
    else:
        raise ValueError(grpc_detail.type_url)


def handleException(e: Exception):
    raise Exception("An exception occurred: {}".format(e)) from None
