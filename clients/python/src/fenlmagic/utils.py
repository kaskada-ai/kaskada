import kaskada.query


def arg_to_response_type(arg: str) -> kaskada.query.ResponseType:
    arg = arg.lower()
    if arg == "csv":
        return kaskada.query.ResponseType.FILE_TYPE_CSV
    elif arg == "parquet":
        return kaskada.query.ResponseType.FILE_TYPE_PARQUET
    else:
        raise ValueError(
            'invalid response type provided. only "csv" or "parquet" supported'
        )
