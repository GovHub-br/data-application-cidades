"""Connection or Variable missing in Airflow: fail at once, without retries.

Retrying does not create the configuration, it only delays the failure by the backoff. In
Airflow 3 a missing Connection raises AirflowNotFoundException (hooks) or an
AirflowRuntimeError CONNECTION_NOT_FOUND (SDK), and a missing Variable an AirflowRuntimeError
VARIABLE_NOT_FOUND.
"""

import functools

from airflow.sdk.exceptions import (
    AirflowFailException,
    AirflowNotFoundException,
    AirflowRuntimeError,
    ErrorType,
)

MISSING = {ErrorType.CONNECTION_NOT_FOUND, ErrorType.VARIABLE_NOT_FOUND}


def fail_fast_on_missing_config(func):
    """Below `@task`: a missing Connection/Variable becomes AirflowFailException (no retry)."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AirflowNotFoundException as error:
            raise AirflowFailException(f"configuração ausente no Airflow: {error}") from error
        except AirflowRuntimeError as error:
            if error.error.error not in MISSING:
                raise
            raise AirflowFailException(f"configuração ausente no Airflow: {error}") from error

    return wrapper
