# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: metrics.proto
# Protobuf Python Version: 5.29.3
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    3,
    '',
    'metrics.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rmetrics.proto\"$\n\x05Label\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t\"8\n\x06Sample\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x01\x12\x11\n\ttimestamp\x18\x03 \x01(\x03\">\n\nTimeSeries\x12\x16\n\x06labels\x18\x01 \x03(\x0b\x32\x06.Label\x12\x18\n\x07samples\x18\x02 \x03(\x0b\x32\x07.Sample\"/\n\x0cWriteRequest\x12\x1f\n\ntimeseries\x18\x01 \x03(\x0b\x32\x0b.TimeSeriesb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'metrics_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_LABEL']._serialized_start=17
  _globals['_LABEL']._serialized_end=53
  _globals['_SAMPLE']._serialized_start=55
  _globals['_SAMPLE']._serialized_end=111
  _globals['_TIMESERIES']._serialized_start=113
  _globals['_TIMESERIES']._serialized_end=175
  _globals['_WRITEREQUEST']._serialized_start=177
  _globals['_WRITEREQUEST']._serialized_end=224
# @@protoc_insertion_point(module_scope)
