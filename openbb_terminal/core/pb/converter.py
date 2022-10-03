
# IMPORTATION STANDARD
from typing import Optional

# IMPORTATION THIRD PARTY
import pandas as pd
from google.protobuf.message import Message
from google.protobuf.internal.well_known_types import Any
from google.protobuf import json_format
from google.protobuf.empty_pb2 import Empty
from google.protobuf.struct_pb2 import Struct, Value
from google.protobuf.wrappers_pb2 import (
    BoolValue,
    DoubleValue,
    Int64Value,
    StringValue,
)

PB_WRAPPERS_PATH = "google/protobuf/wrappers.proto"
PY_TO_PB_TABLE = {
    bool: BoolValue,
    dict: Struct,
    int: Int64Value,
    float: DoubleValue,
    str: StringValue,
}


def pb_to_py(obj) -> Optional[Any]:
    value = None

    if isinstance(obj, Empty):
        value = None
    elif obj.DESCRIPTOR.file.name == PB_WRAPPERS_PATH:
        value = obj.value
    else:
        value = obj

    return value


def py_to_pb(obj) -> Any:    
    value = None
    obj_type = type(obj)

    if obj is None:
        value = Empty()
    elif isinstance(obj, pd.DataFrame):
        value = Value(string_value=obj.to_json(orient="records"))
    elif obj_type in PY_TO_PB_TABLE:
        value = json_format.ParseDict(
            js_dict=obj,
            message=PY_TO_PB_TABLE[obj_type](),
            ignore_unknown_fields=True,
            descriptor_pool=None,
        )
    else:
        value = obj

    return value

def message_to_dict(message: Message) -> dict:
    return json_format.MessageToDict(
        message=message,
        including_default_value_fields=True,
        preserving_proto_field_name=True,
        use_integers_for_enums=True,
        descriptor_pool=None,
        float_precision=None,
    )


def dict_to_struct(json_dict: dict) -> Struct:
    return json_format.ParseDict(
        js_dict=json_dict,
        message=Struct(),
        ignore_unknown_fields=True,
        descriptor_pool=None,
    )
