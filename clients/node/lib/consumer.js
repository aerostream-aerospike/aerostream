'use strict';

const { Connection } = require('./connection');
const proto = require('./proto');
const C = require('./constants');

/*
 * Consumer — durable consumer-group reader with a server-push session.
 *
 * After consume(), the server pushes STREAM_RECORD messages on this connection.
 * Each is surfaced as a 'record' event. The consumer must ACK records to
 * advance the committed group offset and release in-flight slots.
 *
 * Events:
 *   'record', record   record = { stream, group, partitionId, offset (BigInt),
 *                                  timestampNs (BigInt), payload (Buffer),
 *                                  headers, ack() }
 *
 * One Consumer connection may run multiple consume() sessions; records are
 * tagged with their originating stream/group via the echoed correlation_id.
 */
class Consumer extends Connection {
  constructor(opts = {}) {
    super(opts);
    // correlation_id (string) -> { stream, group }
    this._sessions = new Map();
    this.autoAck = opts.autoAck === true;
  }

  _handleFrame(type, body) {
    if (type !== C.TYPE.RECORD) {
      // 1-byte status frame (e.g. back-pressure). The server echoes the
      // message type it pertains to plus a single status byte.
      if (body.length === 1) {
        const status = body.readUInt8(0);
        const name = C.STATUS_NAME[status] || `0x${status.toString(16)}`;
        if (status === C.STATUS.ERR_MAX_IN_FLIGHT) {
          // Paused: you have max_in_flight unacked records. ACK to resume.
          this.emit('backpressure');
        }
        this.emit('status', { status, name });
        return;
      }
      this.emit('frame', type, body);
      return;
    }

    const rec = proto.decodeRecord(body);
    const session = this._sessions.get(rec.correlationId.toString());
    const stream = session ? session.stream : undefined;
    const group = session ? session.group : undefined;

    const self = this;
    const record = {
      stream,
      group,
      partitionId: rec.partitionId,
      offset: rec.offset,
      timestampNs: rec.timestampNs,
      headers: rec.headers,
      payload: rec.payload,
      ack() {
        return self.ack({
          stream,
          group,
          partitionId: rec.partitionId,
          offset: rec.offset,
        });
      },
    };

    this.emit('record', record);

    if (this.autoAck) {
      record.ack().catch((err) => this.emit('error', err));
    }
  }

  /**
   * Open a consumer-group push session.
   * @param {object} o
   * @param {string} o.stream
   * @param {string} o.group
   * @param {number} [o.partition=C.ALL_PARTITIONS]
   * @param {number} [o.seekType=C.SEEK.LATEST]
   * @param {number|BigInt} [o.seekValue=0]
   * @param {number} [o.maxInFlight=10]
   * @returns {Promise<BigInt>} the session correlation_id
   */
  async consume({
    stream,
    group,
    partition = C.ALL_PARTITIONS,
    seekType = C.SEEK.LATEST,
    seekValue = 0,
    maxInFlight = 10,
  }) {
    const correlationId = this.nextCorrelationId();
    this._sessions.set(correlationId.toString(), { stream, group });

    const frame = proto.encodeConsume({
      correlationId, stream, group,
      partitionId: partition, seekType, seekValue, maxInFlight,
    });
    await this.send(frame);
    return correlationId;
  }

  /**
   * Commit a consumer-group offset (fire-and-forget; server sends no reply).
   */
  ack({ stream, group, partitionId, offset }) {
    const correlationId = this.nextCorrelationId();
    const frame = proto.encodeAck({ correlationId, stream, group, partitionId, offset });
    return this.send(frame);
  }

  /**
   * Reset a group's position on a partition for replay.
   * @param {object} o
   * @param {string} o.stream
   * @param {string} o.group
   * @param {number} o.partitionId
   * @param {number} o.seekType
   * @param {number|BigInt} o.seekValue
   */
  seek({ stream, group, partitionId, seekType, seekValue }) {
    const correlationId = this.nextCorrelationId();
    const frame = proto.encodeSeek({ correlationId, stream, group, partitionId, seekType, seekValue });
    return this.send(frame);
  }

  /**
   * Tear down the consume session(s) on this connection.
   */
  unsubscribe({ stream } = {}) {
    const correlationId = this.nextCorrelationId();
    const frame = proto.encodeUnsub({
      correlationId,
      name: stream,
      unsubType: C.UNSUB_TYPE.CONSUME_SESSION,
    });
    return this.send(frame);
  }
}

module.exports = { Consumer };
