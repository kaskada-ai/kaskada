from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
DISABLED_FIELD_NUMBER: _ClassVar[int]
HTTP_HEADER_NAME: KnownRegex
HTTP_HEADER_VALUE: KnownRegex
IGNORED_FIELD_NUMBER: _ClassVar[int]
REQUIRED_FIELD_NUMBER: _ClassVar[int]
RULES_FIELD_NUMBER: _ClassVar[int]
UNKNOWN: KnownRegex
disabled: _descriptor.FieldDescriptor
ignored: _descriptor.FieldDescriptor
required: _descriptor.FieldDescriptor
rules: _descriptor.FieldDescriptor

class AnyRules(_message.Message):
    __slots__ = ["not_in", "required"]
    IN_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    not_in: _containers.RepeatedScalarFieldContainer[str]
    required: bool
    def __init__(self, required: bool = ..., not_in: _Optional[_Iterable[str]] = ..., **kwargs) -> None: ...

class BoolRules(_message.Message):
    __slots__ = ["const"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    const: bool
    def __init__(self, const: bool = ...) -> None: ...

class BytesRules(_message.Message):
    __slots__ = ["const", "contains", "ignore_empty", "ip", "ipv4", "ipv6", "len", "max_len", "min_len", "not_in", "pattern", "prefix", "suffix"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    CONTAINS_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    IPV4_FIELD_NUMBER: _ClassVar[int]
    IPV6_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    LEN_FIELD_NUMBER: _ClassVar[int]
    MAX_LEN_FIELD_NUMBER: _ClassVar[int]
    MIN_LEN_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    SUFFIX_FIELD_NUMBER: _ClassVar[int]
    const: bytes
    contains: bytes
    ignore_empty: bool
    ip: bool
    ipv4: bool
    ipv6: bool
    len: int
    max_len: int
    min_len: int
    not_in: _containers.RepeatedScalarFieldContainer[bytes]
    pattern: str
    prefix: bytes
    suffix: bytes
    def __init__(self, const: _Optional[bytes] = ..., len: _Optional[int] = ..., min_len: _Optional[int] = ..., max_len: _Optional[int] = ..., pattern: _Optional[str] = ..., prefix: _Optional[bytes] = ..., suffix: _Optional[bytes] = ..., contains: _Optional[bytes] = ..., not_in: _Optional[_Iterable[bytes]] = ..., ip: bool = ..., ipv4: bool = ..., ipv6: bool = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class DoubleRules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: float
    gt: float
    gte: float
    ignore_empty: bool
    lt: float
    lte: float
    not_in: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, const: _Optional[float] = ..., lt: _Optional[float] = ..., lte: _Optional[float] = ..., gt: _Optional[float] = ..., gte: _Optional[float] = ..., not_in: _Optional[_Iterable[float]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class DurationRules(_message.Message):
    __slots__ = ["const", "gt", "gte", "lt", "lte", "not_in", "required"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    const: _duration_pb2.Duration
    gt: _duration_pb2.Duration
    gte: _duration_pb2.Duration
    lt: _duration_pb2.Duration
    lte: _duration_pb2.Duration
    not_in: _containers.RepeatedCompositeFieldContainer[_duration_pb2.Duration]
    required: bool
    def __init__(self, required: bool = ..., const: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., lt: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., lte: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., gt: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., gte: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., not_in: _Optional[_Iterable[_Union[_duration_pb2.Duration, _Mapping]]] = ..., **kwargs) -> None: ...

class EnumRules(_message.Message):
    __slots__ = ["const", "defined_only", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    DEFINED_ONLY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    defined_only: bool
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., defined_only: bool = ..., not_in: _Optional[_Iterable[int]] = ..., **kwargs) -> None: ...

class FieldRules(_message.Message):
    __slots__ = ["any", "bool", "bytes", "double", "duration", "enum", "fixed32", "fixed64", "float", "int32", "int64", "map", "message", "repeated", "sfixed32", "sfixed64", "sint32", "sint64", "string", "timestamp", "uint32", "uint64"]
    ANY_FIELD_NUMBER: _ClassVar[int]
    BOOL_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    ENUM_FIELD_NUMBER: _ClassVar[int]
    FIXED32_FIELD_NUMBER: _ClassVar[int]
    FIXED64_FIELD_NUMBER: _ClassVar[int]
    FLOAT_FIELD_NUMBER: _ClassVar[int]
    INT32_FIELD_NUMBER: _ClassVar[int]
    INT64_FIELD_NUMBER: _ClassVar[int]
    MAP_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    REPEATED_FIELD_NUMBER: _ClassVar[int]
    SFIXED32_FIELD_NUMBER: _ClassVar[int]
    SFIXED64_FIELD_NUMBER: _ClassVar[int]
    SINT32_FIELD_NUMBER: _ClassVar[int]
    SINT64_FIELD_NUMBER: _ClassVar[int]
    STRING_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    UINT32_FIELD_NUMBER: _ClassVar[int]
    UINT64_FIELD_NUMBER: _ClassVar[int]
    any: AnyRules
    bool: BoolRules
    bytes: BytesRules
    double: DoubleRules
    duration: DurationRules
    enum: EnumRules
    fixed32: Fixed32Rules
    fixed64: Fixed64Rules
    float: FloatRules
    int32: Int32Rules
    int64: Int64Rules
    map: MapRules
    message: MessageRules
    repeated: RepeatedRules
    sfixed32: SFixed32Rules
    sfixed64: SFixed64Rules
    sint32: SInt32Rules
    sint64: SInt64Rules
    string: StringRules
    timestamp: TimestampRules
    uint32: UInt32Rules
    uint64: UInt64Rules
    def __init__(self, message: _Optional[_Union[MessageRules, _Mapping]] = ..., float: _Optional[_Union[FloatRules, _Mapping]] = ..., double: _Optional[_Union[DoubleRules, _Mapping]] = ..., int32: _Optional[_Union[Int32Rules, _Mapping]] = ..., int64: _Optional[_Union[Int64Rules, _Mapping]] = ..., uint32: _Optional[_Union[UInt32Rules, _Mapping]] = ..., uint64: _Optional[_Union[UInt64Rules, _Mapping]] = ..., sint32: _Optional[_Union[SInt32Rules, _Mapping]] = ..., sint64: _Optional[_Union[SInt64Rules, _Mapping]] = ..., fixed32: _Optional[_Union[Fixed32Rules, _Mapping]] = ..., fixed64: _Optional[_Union[Fixed64Rules, _Mapping]] = ..., sfixed32: _Optional[_Union[SFixed32Rules, _Mapping]] = ..., sfixed64: _Optional[_Union[SFixed64Rules, _Mapping]] = ..., bool: _Optional[_Union[BoolRules, _Mapping]] = ..., string: _Optional[_Union[StringRules, _Mapping]] = ..., bytes: _Optional[_Union[BytesRules, _Mapping]] = ..., enum: _Optional[_Union[EnumRules, _Mapping]] = ..., repeated: _Optional[_Union[RepeatedRules, _Mapping]] = ..., map: _Optional[_Union[MapRules, _Mapping]] = ..., any: _Optional[_Union[AnyRules, _Mapping]] = ..., duration: _Optional[_Union[DurationRules, _Mapping]] = ..., timestamp: _Optional[_Union[TimestampRules, _Mapping]] = ...) -> None: ...

class Fixed32Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class Fixed64Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class FloatRules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: float
    gt: float
    gte: float
    ignore_empty: bool
    lt: float
    lte: float
    not_in: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, const: _Optional[float] = ..., lt: _Optional[float] = ..., lte: _Optional[float] = ..., gt: _Optional[float] = ..., gte: _Optional[float] = ..., not_in: _Optional[_Iterable[float]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class Int32Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class Int64Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class MapRules(_message.Message):
    __slots__ = ["ignore_empty", "keys", "max_pairs", "min_pairs", "no_sparse", "values"]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    MAX_PAIRS_FIELD_NUMBER: _ClassVar[int]
    MIN_PAIRS_FIELD_NUMBER: _ClassVar[int]
    NO_SPARSE_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    ignore_empty: bool
    keys: FieldRules
    max_pairs: int
    min_pairs: int
    no_sparse: bool
    values: FieldRules
    def __init__(self, min_pairs: _Optional[int] = ..., max_pairs: _Optional[int] = ..., no_sparse: bool = ..., keys: _Optional[_Union[FieldRules, _Mapping]] = ..., values: _Optional[_Union[FieldRules, _Mapping]] = ..., ignore_empty: bool = ...) -> None: ...

class MessageRules(_message.Message):
    __slots__ = ["required", "skip"]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    SKIP_FIELD_NUMBER: _ClassVar[int]
    required: bool
    skip: bool
    def __init__(self, skip: bool = ..., required: bool = ...) -> None: ...

class RepeatedRules(_message.Message):
    __slots__ = ["ignore_empty", "items", "max_items", "min_items", "unique"]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    MAX_ITEMS_FIELD_NUMBER: _ClassVar[int]
    MIN_ITEMS_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_FIELD_NUMBER: _ClassVar[int]
    ignore_empty: bool
    items: FieldRules
    max_items: int
    min_items: int
    unique: bool
    def __init__(self, min_items: _Optional[int] = ..., max_items: _Optional[int] = ..., unique: bool = ..., items: _Optional[_Union[FieldRules, _Mapping]] = ..., ignore_empty: bool = ...) -> None: ...

class SFixed32Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class SFixed64Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class SInt32Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class SInt64Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class StringRules(_message.Message):
    __slots__ = ["address", "const", "contains", "email", "hostname", "ignore_empty", "ip", "ipv4", "ipv6", "len", "len_bytes", "max_bytes", "max_len", "min_bytes", "min_len", "not_contains", "not_in", "pattern", "prefix", "strict", "suffix", "uri", "uri_ref", "uuid", "well_known_regex"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CONST_FIELD_NUMBER: _ClassVar[int]
    CONTAINS_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    IPV4_FIELD_NUMBER: _ClassVar[int]
    IPV6_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    LEN_BYTES_FIELD_NUMBER: _ClassVar[int]
    LEN_FIELD_NUMBER: _ClassVar[int]
    MAX_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_LEN_FIELD_NUMBER: _ClassVar[int]
    MIN_BYTES_FIELD_NUMBER: _ClassVar[int]
    MIN_LEN_FIELD_NUMBER: _ClassVar[int]
    NOT_CONTAINS_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    STRICT_FIELD_NUMBER: _ClassVar[int]
    SUFFIX_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    URI_REF_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    WELL_KNOWN_REGEX_FIELD_NUMBER: _ClassVar[int]
    address: bool
    const: str
    contains: str
    email: bool
    hostname: bool
    ignore_empty: bool
    ip: bool
    ipv4: bool
    ipv6: bool
    len: int
    len_bytes: int
    max_bytes: int
    max_len: int
    min_bytes: int
    min_len: int
    not_contains: str
    not_in: _containers.RepeatedScalarFieldContainer[str]
    pattern: str
    prefix: str
    strict: bool
    suffix: str
    uri: bool
    uri_ref: bool
    uuid: bool
    well_known_regex: KnownRegex
    def __init__(self, const: _Optional[str] = ..., len: _Optional[int] = ..., min_len: _Optional[int] = ..., max_len: _Optional[int] = ..., len_bytes: _Optional[int] = ..., min_bytes: _Optional[int] = ..., max_bytes: _Optional[int] = ..., pattern: _Optional[str] = ..., prefix: _Optional[str] = ..., suffix: _Optional[str] = ..., contains: _Optional[str] = ..., not_contains: _Optional[str] = ..., not_in: _Optional[_Iterable[str]] = ..., email: bool = ..., hostname: bool = ..., ip: bool = ..., ipv4: bool = ..., ipv6: bool = ..., uri: bool = ..., uri_ref: bool = ..., address: bool = ..., uuid: bool = ..., well_known_regex: _Optional[_Union[KnownRegex, str]] = ..., strict: bool = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class TimestampRules(_message.Message):
    __slots__ = ["const", "gt", "gt_now", "gte", "lt", "lt_now", "lte", "required", "within"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    GT_NOW_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    LT_NOW_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    WITHIN_FIELD_NUMBER: _ClassVar[int]
    const: _timestamp_pb2.Timestamp
    gt: _timestamp_pb2.Timestamp
    gt_now: bool
    gte: _timestamp_pb2.Timestamp
    lt: _timestamp_pb2.Timestamp
    lt_now: bool
    lte: _timestamp_pb2.Timestamp
    required: bool
    within: _duration_pb2.Duration
    def __init__(self, required: bool = ..., const: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., lt: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., lte: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., gt: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., gte: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., lt_now: bool = ..., gt_now: bool = ..., within: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class UInt32Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class UInt64Rules(_message.Message):
    __slots__ = ["const", "gt", "gte", "ignore_empty", "lt", "lte", "not_in"]
    CONST_FIELD_NUMBER: _ClassVar[int]
    GTE_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_EMPTY_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    LTE_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    NOT_IN_FIELD_NUMBER: _ClassVar[int]
    const: int
    gt: int
    gte: int
    ignore_empty: bool
    lt: int
    lte: int
    not_in: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, const: _Optional[int] = ..., lt: _Optional[int] = ..., lte: _Optional[int] = ..., gt: _Optional[int] = ..., gte: _Optional[int] = ..., not_in: _Optional[_Iterable[int]] = ..., ignore_empty: bool = ..., **kwargs) -> None: ...

class KnownRegex(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
