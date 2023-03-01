import logging
import sys
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

import grpc

import kaskada.kaskada.v1alpha.common_pb2 as common_pb
import kaskada.kaskada.v1alpha.materialization_service_pb2 as material_pb
from kaskada.client import Client, SliceFilter, get_client
from kaskada.utils import handleException, handleGrpcError

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class Destination(ABC):
    @abstractmethod
    def to_request(self) -> Dict[str, Any]:
        pass


class RedisDestination(Destination):
    def __init__(
        self,
        host_name: str,
        port: int,
        use_tls: bool,
        database_number: int,
        password: str,
        tls_cert: str,
        tls_key: str,
        tls_ca_cert: str,
        insecure_skip_verify: bool,
    ) -> None:
        super().__init__()
        self._host_name = host_name
        self._port = port
        self._use_tls = use_tls
        self._database_number = database_number
        self._password = password
        self._tls_cert = tls_cert
        self._tls_key = tls_key
        self._tls_ca_cert = tls_ca_cert
        self._insecure_skip_verify = insecure_skip_verify

    def to_request(self) -> Dict[str, Any]:
        return {
            "host_name": self._host_name,
            "port": self._port,
            "use_tls": self._use_tls,
            "database_number": self._database_number,
            "password": self._password,
            "tls_cert": self._tls_cert,
            "tls_key": self._tls_key,
            "tls_ca_cert": self._tls_ca_cert,
            "insecure_skip_verify": self._insecure_skip_verify,
        }


class FileType(Enum):
    FILE_TYPE_UNSPECIFIED = 0
    FILE_TYPE_PARQUET = 1
    FILE_TYPE_CSV = 2


class ObjectStoreDestination(Destination):
    def __init__(self, file_type: FileType, output_prefix_uri: str):
        self._file_type = file_type
        self._output_prefix_uri = output_prefix_uri

    def to_request(self) -> Dict[str, Any]:
        return {
            "file_type": self._file_type.name,
            "output_prefix_uri": self._output_prefix_uri,
        }


class PulsarDestination(Destination):
    def __init__(
        self,
        tenant: str = "public",
        namespace: str = "default",
        topic_name: Optional[str] = None,
        broker_service_url: str = "pulsar://127.0.0.1:6650",
    ):
        """
        Pulsar Materialization Destination

        Args:
            tenant (str): pulsar tenant. defaults to "public".
            namespace (str): pulsar namespace. defaults to "default".
            topic_name (str): final part of topic url. defaults to a randomly generated uuid
            broker_service_url (str): url to connect to pulsar broker. defaults to "pulsar://127.0.0.1:6650"
        """
        self._tenant = tenant
        self._namespace = namespace
        self._topic_name = (
            topic_name
            if topic_name and len(topic_name.strip()) > 0
            else str(uuid.uuid4())
        )
        self._broker_service_url = broker_service_url

    def to_request(self) -> Dict[str, Any]:
        return {
            "tenant": self._tenant,
            "namespace": self._namespace,
            "topic_name": self._topic_name,
            "broker_service_url": self._broker_service_url,
        }


class MaterializationView(object):
    def __init__(self, name: str, expression: str):
        """
        Kaskada Materialization View

        Args:
            name (str): The name of the view
            expression (str): The fenl expression to compute
        """
        self._name = name
        self._expression = expression


def create_materialization(
    name: str,
    query: str,
    destination: Destination,
    views: List[MaterializationView],
    slice_filter: SliceFilter = None,
    client: Client = None,
) -> material_pb.CreateMaterializationResponse:
    try:
        slice_request = None
        if slice_filter is not None:
            slice_request = common_pb.SliceRequest(**slice_filter.to_request())

        materialization = {
            "materialization_name": name,
            "query": query,
            "with_views": to_with_views(views),
            "slice": slice_request,
        }
        if isinstance(destination, ObjectStoreDestination):
            materialization["destination"] = {"object_store": destination.to_request()}
        elif isinstance(destination, RedisDestination):
            materialization["destination"] = {"redis": destination.to_request()}
        elif isinstance(destination, PulsarDestination):
            materialization["destination"] = {"pulsar": destination.to_request()}
        else:
            raise ValueError("invalid destination supplied")

        req = material_pb.CreateMaterializationRequest(
            **{"materialization": materialization}
        )
        logger.debug(f"Create Materialization Request: {req}")
        client = get_client(client)
        return client.materialization_stub.CreateMaterialization(
            req, metadata=client.get_metadata()
        )
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def delete_materialization(
    name: str, client: Client = None
) -> material_pb.DeleteMaterializationResponse:
    try:
        client = get_client(client)
        req = material_pb.DeleteMaterializationRequest(materialization_name=name)
        logger.debug(f"Delete Materialization Request: {req}")
        return client.materialization_stub.DeleteMaterialization(
            req, metadata=client.get_metadata()
        )
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def get_materialization(
    name: str, client: Client = None
) -> material_pb.GetMaterializationResponse:
    try:
        client = get_client(client)
        req = material_pb.GetMaterializationRequest(materialization_name=name)
        logger.debug(f"Get Materialization Request: {req}")
        return client.materialization_stub.GetMaterialization(
            req, metadata=client.get_metadata()
        )
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def list_materializations(
    search: Optional[str] = None, client: Client = None
) -> material_pb.ListMaterializationsResponse:
    try:
        client = get_client(client)
        req = material_pb.ListMaterializationsRequest(
            search=search,
        )
        logger.debug(f"List Materialization Request: {req}")
        return client.materialization_stub.ListMaterializations(
            req, metadata=client.get_metadata()
        )
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def to_with_views(views: List[MaterializationView]) -> List[material_pb.WithView]:
    with_views = []
    for v in views:
        with_views.append(material_pb.WithView(name=v._name, expression=v._expression))
    return with_views
