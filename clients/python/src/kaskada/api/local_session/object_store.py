from enum import Enum
from typing import Optional


class ObjectStoreType(Enum):
    LOCAL = 1
    S3 = 2
    GS = 3


class ObjectStoreConfig:
    def __init__(
        self,
        object_store_type: ObjectStoreType,
        path: str,
        bucket: Optional[str] = None,
        disable_ssl: Optional[bool] = None,
        endpoint: Optional[str] = None,
        force_path_style: Optional[bool] = None,
    ):
        """Defines the configuration details for the object store

        Args:
            object_store_type (ObjectStoreType): The type of the object store (local, s3, or gs)
            path (str): The path to use within the object store
            bucket (Optional[str]): The name of the target bucket (if object store type is s3 or gs)
            disable_ssl (Optional[bool], optional): Disable SSL when connecting to object store. Defaults to None.
            endpoint (Optional[str], optional): Set the target endpoint for the object store. Useful for localstack or minio scenarios. Defaults to None.
            force_path_style (Optional[bool], optional): Use the path style for paths. Defaults to None.
        """
        self.object_store_type = object_store_type
        self.bucket = bucket
        self.path = path
        self.disable_ssl = disable_ssl
        self.endpoint = endpoint
        self.force_path_style = force_path_style
