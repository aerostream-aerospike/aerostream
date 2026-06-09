'use strict';

const { Connection } = require('./connection');
const proto = require('./proto');
const C = require('./constants');

/*
 * Producer — appends records to streams.
 *
 * produce() with ackMode > 0 returns a promise that resolves when the matching
 * STREAM_PROD_ACK arrives (correlated by correlation_id). With ackMode NONE the
 * write is fire-and-forget and the promise resolves once the bytes are flushed.
 */
class Producer extends Connection {
  constructor(opts = {}) {
    super(opts);
    // correlation_id (string) -> { resolve, reject, timer }
    this._pending = new Map();
    this._ackTimeoutMs = opts.ackTimeoutMs || 10000;
  }

  _handleFrame(type, body) {
    if (type !== C.TYPE.PROD_ACK) {
      this.emit('frame', type, body); // unexpected; surface for debugging
      return;
    }

    const ack = proto.decodeProdAck(body);
    const key = ack.correlationId.toString();
    const waiter = this._pending.get(key);
    if (!waiter) return; // late/duplicate ack — ignore

    this._pending.delete(key);
    clearTimeout(waiter.timer);

    if (ack.status === C.STATUS.OK) {
      waiter.resolve({
        offset: ack.offset,
        partitionId: ack.partitionId,
        timestampNs: ack.timestampNs,
        status: ack.status,
      });
    } else {
      const name = C.STATUS_NAME[ack.status] || `0x${ack.status.toString(16)}`;
      waiter.reject(new Error(`produce rejected: ${name}`));
    }
  }

  /**
   * Append one record.
   * @param {object} o
   * @param {string} o.stream
   * @param {string} [o.partitionKey='']  used for consistent-hash partition routing
   * @param {Buffer|string} o.payload
   * @param {object|Array} [o.headers]    per-record metadata; { key: value } or [{key, value}]
   * @param {number} [o.ackMode=C.ACK_MODE.LEADER]
   * @returns {Promise<{offset:BigInt, partitionId:number, timestampNs:BigInt, status:number}|null>}
   */
  produce({ stream, partitionKey = '', payload, headers, ackMode = C.ACK_MODE.LEADER }) {
    const correlationId = this.nextCorrelationId();
    const frame = proto.encodeProduce({ correlationId, stream, partitionKey, ackMode, payload, headers });

    if (ackMode === C.ACK_MODE.NONE) {
      return this.send(frame).then(() => null);
    }

    return new Promise((resolve, reject) => {
      const key = correlationId.toString();
      const timer = setTimeout(() => {
        this._pending.delete(key);
        reject(new Error(`produce ack timeout after ${this._ackTimeoutMs}ms`));
      }, this._ackTimeoutMs);
      this._pending.set(key, { resolve, reject, timer });

      this.send(frame).catch((err) => {
        this._pending.delete(key);
        clearTimeout(timer);
        reject(err);
      });
    });
  }

  async close() {
    for (const [, w] of this._pending) {
      clearTimeout(w.timer);
      w.reject(new Error('producer closed'));
    }
    this._pending.clear();
    return super.close();
  }
}

module.exports = { Producer };
