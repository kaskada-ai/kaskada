from datetime import datetime

import pandas
from domonic.html import b, pre, td, tr
from google.protobuf.message import Message

import kaskada.kaskada.v1alpha.schema_pb2 as schema_pb2

"""
NOTES:

Use `hasattr(obj, prop) to test if an object has a property`
Use `obj.HasField(prop) to test if an object has a specific one-of`
"""

kaskada_fenl_primitive_types = {
    schema_pb2.DataType.PRIMITIVE_TYPE_BOOL: "bool",
    schema_pb2.DataType.PRIMITIVE_TYPE_DURATION_MICROSECOND: "duration_us",
    schema_pb2.DataType.PRIMITIVE_TYPE_DURATION_MILLISECOND: "duration_ms",
    schema_pb2.DataType.PRIMITIVE_TYPE_DURATION_NANOSECOND: "duration_ns",
    schema_pb2.DataType.PRIMITIVE_TYPE_DURATION_SECOND: "duration_s",
    schema_pb2.DataType.PRIMITIVE_TYPE_F16: "f16",
    schema_pb2.DataType.PRIMITIVE_TYPE_F32: "f32",
    schema_pb2.DataType.PRIMITIVE_TYPE_F64: "f64",
    schema_pb2.DataType.PRIMITIVE_TYPE_I16: "i16",
    schema_pb2.DataType.PRIMITIVE_TYPE_I32: "i32",
    schema_pb2.DataType.PRIMITIVE_TYPE_I64: "i64",
    schema_pb2.DataType.PRIMITIVE_TYPE_I8: "i8",
    schema_pb2.DataType.PRIMITIVE_TYPE_INTERVAL_DAY_TIME: "interval_days",
    schema_pb2.DataType.PRIMITIVE_TYPE_INTERVAL_YEAR_MONTH: "interval_months",
    schema_pb2.DataType.PRIMITIVE_TYPE_NULL: "null",
    schema_pb2.DataType.PRIMITIVE_TYPE_STRING: "string",
    schema_pb2.DataType.PRIMITIVE_TYPE_TIMESTAMP_MICROSECOND: "timestamp_us",
    schema_pb2.DataType.PRIMITIVE_TYPE_TIMESTAMP_MILLISECOND: "timestamp_ms",
    schema_pb2.DataType.PRIMITIVE_TYPE_TIMESTAMP_NANOSECOND: "timestamp_ns",
    schema_pb2.DataType.PRIMITIVE_TYPE_TIMESTAMP_SECOND: "timestamp_s",
    schema_pb2.DataType.PRIMITIVE_TYPE_U16: "u16",
    schema_pb2.DataType.PRIMITIVE_TYPE_U32: "u32",
    schema_pb2.DataType.PRIMITIVE_TYPE_U64: "u64",
    schema_pb2.DataType.PRIMITIVE_TYPE_U8: "u8",
}


def get_item(obj, keys, default=""):
    """
    Safely recursively gets an item from a dict, otherwise returns the default value

    Args:
        obj (dict of {str : Any}): Dictionary of (nested) key item pairs
        keys (list of str): The list of keys use to navigate to the desired item in the dictionary.
        default (str, optional): The default value to return if the item isn't found. Defaults to "".

    Returns:
        string: Either the found value or the default.
    """
    while len(keys) > 0:
        key = keys.pop(0)
        if key in obj:
            obj = obj[key]
        else:
            return default
    return obj


def appendChildIfNotNone(parent, child):
    if child is not None:
        parent.appendChild(child)


def appendChildren(parent, children):
    for child in children:
        appendChildIfNotNone(parent, child)


def html_obj_table_row(key, value):
    """
    Returns a html table row, where the key is bolded, and the value is output as code.
    """
    if value is None or value == "":
        return None
    return tr(td(b(key)), td(pre(value)))


def html_table_row(key, value):
    """
    Returns a html table row, where the key is bolded, and the value is output as is.
    """
    return tr(td(b(key)), td(value))


def html_obj_id_row(index, obj, props):
    obj_type = type(obj).__name__.lower()
    obj_name_key = f"{obj_type}_name"
    obj_name = get_item(props, [obj_name_key])
    return tr(td(pre(index)), td(pre(obj_name)))


def appendHtmlObjTableRowIfAttrExists(parent, obj, attr):
    if hasattr(obj, attr):
        elem = html_obj_table_row(attr, getattr(obj, attr))
        if elem is not None:
            parent.appendChild(elem)


def appendHtmlObjTableRowIfHasField(parent, obj, field):
    if obj.HasField(field):
        elem = html_obj_table_row(field, getattr(obj, field))
        if elem is not None:
            parent.appendChild(elem)


def convert_fenl_schema_field(field):
    """
    Converts a fenl field to a string representation.

    Args:
        field (schema_pb2.Schema_Field) : the input field

    Returns:
        str: The string representation of the fenl Field
    """
    return field.name, convert_fenl_datatype(field.data_type)


def convert_fenl_datatype(data_type):
    """
    Converts a fenl dataType to a string representation.

    Args:
        data_type (schema_pb2.DataType) : the input dataType

    Returns:
        str: The string representation of the fenl DataType
    """
    type_string = None
    if data_type.HasField("primitive"):
        type_string = kaskada_fenl_primitive_types.get(data_type.primitive, "<unknown>")
    elif data_type.HasField("struct"):
        type_string = "<struct>"
    elif data_type.HasField("window"):
        type_string = "<window>"
    return type_string


def get_schema_dataframe(obj):
    """
    Gets a dataframe from a schema object if it exists

    Args:
        obj (kaskada.fenl.v1alpha.schema_pb2.Schema | Any):
            Either a schema object or an object that has a schema or analysis object at the root level

    Returns:
        pandas.dataframe | None:
            Either a dataframe describing the schemea or None if no schema exists in the passed object.
    """
    if hasattr(obj, "schema"):
        obj = obj.schema

    if (not hasattr(obj, "fields")) or len(obj.fields) == 0:
        return None
    fields = {}
    for idx, field in enumerate(obj.fields):
        name, typeString = convert_fenl_schema_field(field)
        if len(name) > 0:
            fields[idx] = [name, typeString]
    return pandas.DataFrame.from_dict(
        data=fields, orient="index", columns=["column_name", "column_type"]
    )


def get_datetime(pb2_timestamp):
    """
    Converts a google.protobuf.timestamp_pb2 into a python datetime object.

    Args:
        pb2_timestamp (google.protobuf.timestamp_pb2): The protobuf timestamp.

    Returns:
        datetime: The python datetime.
    """
    return datetime.fromtimestamp(pb2_timestamp.seconds + pb2_timestamp.nanos / 1e9)


def get_classname(obj):
    return f"{obj.__module__}.{obj.__name__}"


def get_properties(obj):
    """
    Converts a protobuf response into a simple python dictionary.

    Args:
        obj (Any): A deserialized protobuf object.

    Returns:
        dict of {str : Any}: A nested set of keys and values with structure matching the protobuf input.
    """
    results = {}
    if isinstance(obj, Message):
        for field in obj.ListFields():
            name = field[0].name
            field_type = get_classname(type(field[1]))
            prop_value = field[1]
            if field_type in (int, float, bool):
                results[name] = prop_value
            elif field_type == "str":
                if prop_value != "":
                    results[name] = prop_value
            elif field_type == "google._upb._message.RepeatedCompositeContainer":
                if len(prop_value) > 0:
                    results[name] = [get_properties(sub_obj) for sub_obj in prop_value]
            elif field_type == "google.protobuf.timestamp_pb2.Timestamp":
                results[name] = get_datetime(prop_value).isoformat()
            elif field_type.startswith("pandas."):
                results[name] = prop_value
            elif isinstance(prop_value, Message):
                results[name] = get_properties(prop_value)
            else:
                results[name] = prop_value
    return results
