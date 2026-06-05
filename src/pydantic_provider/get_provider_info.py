def get_provider_info():
    return {
        "package-name": "pydantic-provider",
        "name": "airflow_provider_pydantic",
        "description": "Automatic (de)serialisation",
        "version": "0.2.1",
        "task-decorators": [
            {
                "name": "pydantic",
                "class-name": "pydantic_provider.operators.pydantic_python_task",
            }
        ],
    }
