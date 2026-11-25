"""A missing Connection/Variable fails without retry; other errors keep retrying."""

import pytest
from airflow.sdk.exceptions import (
    AirflowFailException,
    AirflowNotFoundException,
    AirflowRuntimeError,
    ErrorType,
)
from airflow.sdk.execution_time.comms import ErrorResponse

from plugins.datalake.config import fail_fast_on_missing_config


def _raising(error):
    @fail_fast_on_missing_config
    def task():
        raise error

    return task


@pytest.mark.parametrize(
    "error",
    [
        AirflowNotFoundException("The conn_id `fgv_dados` isn't defined"),
        AirflowRuntimeError(ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"k": "v"})),
        AirflowRuntimeError(ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND, detail={"k": "v"})),
    ],
)
def test_missing_config_fails_without_retry(error):
    with pytest.raises(AirflowFailException, match="configuração ausente"):
        _raising(error)()


def test_other_errors_pass_through_to_be_retried():
    with pytest.raises(ConnectionError):
        _raising(ConnectionError("timeout"))()


def test_keeps_the_return_value_and_the_name():
    @fail_fast_on_missing_config
    def extract(x):
        return x * 2

    assert extract(2) == 4 and extract.__name__ == "extract"
