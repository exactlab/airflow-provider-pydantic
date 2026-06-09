import inspect
import logging
from typing import Callable

from airflow.operators.python import PythonOperator
from airflow.sdk.bases.decorator import DecoratedOperator
from airflow.sdk.bases.decorator import task_decorator_factory
from airflow.sdk.bases.decorator import TaskDecorator
from pydantic import ValidationError


logger = logging.getLogger(__name__)


class PydanticPythonOperator(PythonOperator):
    def __init__(self, python_callable: Callable, *args, **kwargs):
        super().__init__(python_callable=python_callable, *args, **kwargs)

    def execute(self, context):
        # Get the function arguments from the operator
        args = self.op_args
        kwargs = self.op_kwargs

        # Get the function signature and parameters
        func_signature = inspect.signature(self.python_callable)
        func_params = func_signature.parameters

        _args = [
            self.deserialize_input(arg, param)
            for arg, param in zip(args, func_params.values())
        ]
        _kwargs = {
            key: self.deserialize_input(value, func_params[key])
            for key, value in kwargs.items()
        }

        # If the function signature has a `**kwargs`` parameter add context
        if any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in func_params.values()
        ):
            _kwargs = {**_kwargs, **context}

        # Call the original Python callable
        result = self.python_callable(*_args, **_kwargs)

        # Serialize the result before returning it (if it's a Pydantic model)
        return self.serialize_output(result)

    def deserialize_input(self, input_value, parameter: inspect.Parameter):
        """Deserialize input values, for example, Pydantic models."""
        try:
            return parameter.annotation.model_validate_json(input_value)
        except ValidationError as e:
            logger.error(
                "Could not deserialise %s = %s", parameter, input_value
            )
            logger.error(e)
            raise e
        except AttributeError:
            return input_value

    def serialize_output(self, output_value):
        """Serialize output values (e.g., convert Pydantic model back to
        dict)."""
        try:
            return output_value.model_dump_json()
        except AttributeError:
            return output_value


class DecoratedPydanticPythonOperator(
    PydanticPythonOperator, DecoratedOperator
):
    custom_operator_name: str = "@task.pydantic"


def pydantic_python_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=PydanticPythonOperator,
        **kwargs,
    )
