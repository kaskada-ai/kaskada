import kaskada.compute


def arg_to_response_type(arg: str) -> kaskada.compute.ResponseType:
    arg = arg.lower()
    if arg == "csv":
        return kaskada.compute.ResponseType.FILE_TYPE_CSV
    elif arg == "parquet":
        return kaskada.compute.ResponseType.FILE_TYPE_PARQUET
    else:
        raise ValueError(
            'invalid response type provided. only "csv" or "parquet" supported'
        )
