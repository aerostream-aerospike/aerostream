'use strict';

/*
 * Wire encoding/decoding for AeroStream messages.
 *
 * All multi-byte integers are big-endian. Correlation IDs, offsets and
 * timestamps are 64-bit and handled as BigInt to avoid precision loss
 * (timestamp_ns in particular exceeds Number.MAX_SAFE_INTEGER).
 */

const C = require('./constants');

// ---------------------------------------------------------------------------
// Low-level field helpers
// ---------------------------------------------------------------------------

// Write the 8-byte as_proto header: version(1) + type(1) + size(6, BE).
function encodeHeader(type, bodySize) {
  const h = Buffer.alloc(C.HEADER_SZ);
  h.writeUInt8(C.PROTO_VERSION, 0);
  h.writeUInt8(type, 1);
  h.writeUIntBE(bodySize, 2, 6); // 48-bit big-endian size
  return h;
}

function parseHeader(buf) {
  return {
    version: buf.readUInt8(0),
    type: buf.readUInt8(1),
    size: buf.readUIntBE(2, 6),
  };
}

// Write a null-padded fixed-width string field (e.g. stream_name[64]).
function writeName(buf, offset, str, width = C.NAME_SZ) {
  buf.fill(0, offset, offset + width);
  const bytes = Buffer.from(String(str), 'utf8');
  if (bytes.length >= width) {
    throw new RangeError(
      `value "${str}" is ${bytes.length} bytes; max ${width - 1} (field is ${width} incl. null)`
    );
  }
  bytes.copy(buf, offset);
}

// Read a null-terminated string from a fixed-width field.
function readName(buf, offset, width = C.NAME_SZ) {
  let end = offset;
  const limit = offset + width;
  while (end < limit && buf[end] !== 0) end++;
  return buf.toString('utf8', offset, end);
}

// ---------------------------------------------------------------------------
// Encoders (client -> server)
// ---------------------------------------------------------------------------

/*
 * STREAM_PRODUCE (type 10).
 * Body: stream_hdr(72) + partition_key(64) + ack_mode(1) + record_hdr(22) + payload
 */
function encodeProduce({ correlationId, stream, partitionKey, ackMode, payload }) {
  const pay = payload == null
    ? Buffer.alloc(0)
    : Buffer.isBuffer(payload) ? payload : Buffer.from(payload);

  const bodySize = C.SIZE.PRODUCE_FIXED + pay.length;
  const buf = Buffer.alloc(C.HEADER_SZ + bodySize);

  encodeHeader(C.TYPE.PRODUCE, bodySize).copy(buf, 0);
  let p = C.HEADER_SZ;

  // stream_hdr
  buf.writeBigUInt64BE(BigInt(correlationId), p); p += 8;
  writeName(buf, p, stream);                      p += C.NAME_SZ;
  // partition_key
  writeName(buf, p, partitionKey == null ? '' : partitionKey); p += C.NAME_SZ;
  // ack_mode
  buf.writeUInt8(ackMode & 0xff, p);              p += 1;
  // record_hdr: offset=-1 (server assigns), ts=0, headers_count=0, payload_size
  buf.writeBigInt64BE(-1n, p);                    p += 8;  // offset
  buf.writeBigUInt64BE(0n, p);                    p += 8;  // timestamp_ns
  buf.writeUInt16BE(0, p);                        p += 2;  // headers_count
  buf.writeUInt32BE(pay.length, p);               p += 4;  // payload_size
  // payload
  pay.copy(buf, p);

  return buf;
}

/*
 * STREAM_CONSUME (type 12).
 * Body: stream_hdr(72) + group(64) + partition_id(4) + seek_type(1) + seek_value(8) + max_in_flight(4)
 */
function encodeConsume({ correlationId, stream, group, partitionId, seekType, seekValue, maxInFlight }) {
  const buf = Buffer.alloc(C.HEADER_SZ + C.SIZE.CONSUME);
  encodeHeader(C.TYPE.CONSUME, C.SIZE.CONSUME).copy(buf, 0);
  let p = C.HEADER_SZ;

  buf.writeBigUInt64BE(BigInt(correlationId), p); p += 8;
  writeName(buf, p, stream);                      p += C.NAME_SZ;
  writeName(buf, p, group);                        p += C.NAME_SZ;
  buf.writeUInt32BE(partitionId >>> 0, p);         p += 4;
  buf.writeUInt8(seekType & 0xff, p);              p += 1;
  buf.writeBigInt64BE(BigInt(seekValue), p);       p += 8;
  buf.writeUInt32BE(maxInFlight >>> 0, p);         p += 4;

  return buf;
}

/*
 * STREAM_ACK (type 14).
 * Body: stream_hdr(72) + group(64) + partition_id(4) + offset(8)
 */
function encodeAck({ correlationId, stream, group, partitionId, offset }) {
  const buf = Buffer.alloc(C.HEADER_SZ + C.SIZE.ACK);
  encodeHeader(C.TYPE.ACK, C.SIZE.ACK).copy(buf, 0);
  let p = C.HEADER_SZ;

  buf.writeBigUInt64BE(BigInt(correlationId), p); p += 8;
  writeName(buf, p, stream);                      p += C.NAME_SZ;
  writeName(buf, p, group);                        p += C.NAME_SZ;
  buf.writeUInt32BE(partitionId >>> 0, p);         p += 4;
  buf.writeBigInt64BE(BigInt(offset), p);          p += 8;

  return buf;
}

/*
 * STREAM_SEEK (type 15).
 * Body: stream_hdr(72) + group(64) + partition_id(4) + seek_type(1) + seek_value(8)
 */
function encodeSeek({ correlationId, stream, group, partitionId, seekType, seekValue }) {
  const buf = Buffer.alloc(C.HEADER_SZ + C.SIZE.SEEK);
  encodeHeader(C.TYPE.SEEK, C.SIZE.SEEK).copy(buf, 0);
  let p = C.HEADER_SZ;

  buf.writeBigUInt64BE(BigInt(correlationId), p); p += 8;
  writeName(buf, p, stream);                      p += C.NAME_SZ;
  writeName(buf, p, group);                        p += C.NAME_SZ;
  buf.writeUInt32BE(partitionId >>> 0, p);         p += 4;
  buf.writeUInt8(seekType & 0xff, p);              p += 1;
  buf.writeBigInt64BE(BigInt(seekValue), p);       p += 8;

  return buf;
}

/*
 * STREAM_SUB (type 16).
 * Body: stream_hdr(72) + topic(64)
 * (stream_name in the header is unused for sub; topic carries the key.)
 */
function encodeSub({ correlationId, topic }) {
  const buf = Buffer.alloc(C.HEADER_SZ + C.SIZE.SUB);
  encodeHeader(C.TYPE.SUB, C.SIZE.SUB).copy(buf, 0);
  let p = C.HEADER_SZ;

  buf.writeBigUInt64BE(BigInt(correlationId), p); p += 8;
  writeName(buf, p, '');                           p += C.NAME_SZ; // stream_name (unused)
  writeName(buf, p, topic);                        p += C.NAME_SZ;

  return buf;
}

/*
 * STREAM_UNSUB (type 17).
 * Body: stream_hdr(72) + unsub_type(1)
 * For pub/sub unsub, the topic goes in the stream_name header field
 * (matches the server, which keys pub/sub unsub off hdr.stream_name).
 */
function encodeUnsub({ correlationId, name, unsubType }) {
  const buf = Buffer.alloc(C.HEADER_SZ + C.SIZE.UNSUB);
  encodeHeader(C.TYPE.UNSUB, C.SIZE.UNSUB).copy(buf, 0);
  let p = C.HEADER_SZ;

  buf.writeBigUInt64BE(BigInt(correlationId), p); p += 8;
  writeName(buf, p, name == null ? '' : name);     p += C.NAME_SZ;
  buf.writeUInt8(unsubType & 0xff, p);             p += 1;

  return buf;
}

// ---------------------------------------------------------------------------
// Decoders (server -> client). Input `body` excludes the 8-byte header.
// ---------------------------------------------------------------------------

/*
 * STREAM_PROD_ACK (type 11).
 * corr(8) + offset(8) + partition_id(4) + timestamp_ns(8) + status(1)
 */
function decodeProdAck(body) {
  return {
    correlationId: body.readBigUInt64BE(0),
    offset: body.readBigInt64BE(8),
    partitionId: body.readUInt32BE(16),
    timestampNs: body.readBigUInt64BE(20),
    status: body.readUInt8(28),
  };
}

/*
 * STREAM_RECORD (type 13).
 * corr(8) + partition_id(4) + record_hdr(22) [offset(8)+ts(8)+hdrcount(2)+paysize(4)]
 * + header entries + payload
 */
function decodeRecord(body) {
  const correlationId = body.readBigUInt64BE(0);
  const partitionId = body.readUInt32BE(8);
  const offset = body.readBigInt64BE(12);
  const timestampNs = body.readBigUInt64BE(20);
  const headersCount = body.readUInt16BE(28);
  const payloadSize = body.readUInt32BE(30);

  let p = C.SIZE.RECORD_FIXED; // 34

  // Header entries: each is key_size(2) + val_size(2) + key + val.
  const headers = {};
  for (let i = 0; i < headersCount; i++) {
    const keySize = body.readUInt16BE(p); p += 2;
    const valSize = body.readUInt16BE(p); p += 2;
    const key = body.toString('utf8', p, p + keySize); p += keySize;
    const val = body.slice(p, p + valSize); p += valSize;
    headers[key] = val;
  }

  const payload = body.slice(p, p + payloadSize);

  return { correlationId, partitionId, offset, timestampNs, headersCount, headers, payload };
}

module.exports = {
  encodeHeader,
  parseHeader,
  writeName,
  readName,
  encodeProduce,
  encodeConsume,
  encodeAck,
  encodeSeek,
  encodeSub,
  encodeUnsub,
  decodeProdAck,
  decodeRecord,
};
