'use strict';

/*
 * Wire protocol constants for AeroStream.
 *
 * Sizes below are the ACTUAL packed C struct sizes used by the server
 * (verified against sizeof in proto.h), which differ from some figures in
 * docs/PROTOCOL.md — notably as_stream_record_hdr is 22 bytes, not 18.
 */

// as_proto header: version(1) + type(1) + size(6, big-endian) = 8 bytes.
const PROTO_VERSION = 2;
const HEADER_SZ = 8;

// Message types (proto->type).
const TYPE = {
  PRODUCE:  10,
  PROD_ACK: 11,
  CONSUME:  12,
  RECORD:   13,
  ACK:      14,
  SEEK:     15,
  SUB:      16,
  UNSUB:    17,
};

const TYPE_NAME = Object.fromEntries(
  Object.entries(TYPE).map(([k, v]) => [v, k])
);

// Seek types for CONSUME / SEEK.
const SEEK = {
  LATEST:    0x00,
  EARLIEST:  0x01,
  OFFSET:    0x02,
  TIMESTAMP: 0x03,
};

// Produce ack modes.
const ACK_MODE = {
  NONE:   0x00, // fire-and-forget, no PROD_ACK
  LEADER: 0x01, // ack after leader write
  ALL:    0x02, // ack after all replicas
};

// Unsub types.
const UNSUB_TYPE = {
  CONSUME_SESSION: 0x00,
  PUBSUB:          0x01,
};

// Response status codes (PROD_ACK.status and error bodies).
const STATUS = {
  OK:              0x00,
  ERR_NOT_FOUND:   0x01,
  ERR_STORAGE:     0x02,
  ERR_OOO_ACK:     0x03,
  ERR_MAX_IN_FLIGHT: 0x04,
  ERR_INVALID_SEEK: 0x05,
  ERR_GROUP_NOT_FOUND: 0x06,
  ERR_AUTH:        0x07,
};

const STATUS_NAME = Object.fromEntries(
  Object.entries(STATUS).map(([k, v]) => [v, k])
);

// Fixed-width name fields (stream_name, group_name, topic, partition_key).
const NAME_SZ = 64;

// partition_id sentinel meaning "all partitions".
const ALL_PARTITIONS = 0xFFFFFFFF;

// Packed struct sizes (bytes) — match server proto.h exactly.
const SIZE = {
  STREAM_HDR:   72,  // correlation_id(8) + stream_name(64)
  RECORD_HDR:   22,  // offset(8) + timestamp_ns(8) + headers_count(2) + payload_size(4)
  PRODUCE_FIXED: 159, // STREAM_HDR + partition_key(64) + ack_mode(1) + RECORD_HDR
  PROD_ACK:     29,  // corr(8) + offset(8) + partition_id(4) + ts(8) + status(1)
  CONSUME:      153, // STREAM_HDR + group(64) + partition_id(4) + seek_type(1) + seek_value(8) + max_in_flight(4)
  RECORD_FIXED: 34,  // corr(8) + partition_id(4) + RECORD_HDR
  ACK:          148, // STREAM_HDR + group(64) + partition_id(4) + offset(8)
  SEEK:         149, // STREAM_HDR + group(64) + partition_id(4) + seek_type(1) + seek_value(8)
  SUB:          136, // STREAM_HDR + topic(64)
  UNSUB:        73,  // STREAM_HDR + unsub_type(1)
};

module.exports = {
  PROTO_VERSION,
  HEADER_SZ,
  TYPE,
  TYPE_NAME,
  SEEK,
  ACK_MODE,
  UNSUB_TYPE,
  STATUS,
  STATUS_NAME,
  NAME_SZ,
  ALL_PARTITIONS,
  SIZE,
};
