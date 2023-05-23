from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, call, patch

import pytest

from kaskada.api.local_session.local_service import KaskadaLocalService

service_name = "test_service"
binary_path = "some_path"
binary_execute_cmd = "some param"
std_err_log_path = Path("./some_err.log")
std_out_log_path = Path("./some_out.log")
configs: Dict[str, Any] = {"-some-flag": 12345, "--another-flag": "true"}


@patch("kaskada.api.local_session.local_service.SubprocessFactory")
def test_kaskada_local_service(subprocess_factory):
    kaskada_local_service = KaskadaLocalService(
        service_name,
        binary_path,
        binary_execute_cmd,
        std_err_log_path,
        std_out_log_path,
        configs,
        subprocess_factory=subprocess_factory,
    )
    assert kaskada_local_service.execute_cmd == [
        "some_path",
        "-some-flag=12345",
        "--another-flag=true",
        "some param",
    ]
