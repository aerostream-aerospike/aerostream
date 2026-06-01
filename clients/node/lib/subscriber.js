'use strict';

const { Connection } = require('./connection');
const proto = require('./proto');
const C = require('./constants');

/*
 * Subscriber — ephemeral pub/sub. No offsets, no durability. Records produced
 * to a stream whose name matches the subscribed topic are fanned out in real
 * time. Anything sent while disconnected is lost.
 *
 * Events:
 *   'message', msg   msg = { topic, partitionId, offset (BigInt),
 *                            timestampNs (BigInt), payload (Buffer), headers }
 */
class Subscriber extends Connection {
  constructor(opts = {}) {
    super(opts);
    // correlation_id (string) -> topic
    this._topics = new Map();
  }

  _handleFrame(type, body) {
    if (type !== C.TYPE.RECORD) {
      this.emit('frame', type, body);
      return;
    }

    const rec = proto.decodeRecord(body);
    const topic = this._topics.get(rec.correlationId.toString());

    this.emit('message', {
      topic,
      partitionId: rec.partitionId,
      offset: rec.offset,
      timestampNs: rec.timestampNs,
      headers: rec.headers,
      payload: rec.payload,
    });
  }

  /**
   * Subscribe to a topic (= stream name). Delivery starts on the next produce.
   * @param {object} o
   * @param {string} o.topic
   * @returns {Promise<BigInt>} the subscription correlation_id
   */
  async subscribe({ topic }) {
    const correlationId = this.nextCorrelationId();
    this._topics.set(correlationId.toString(), topic);
    const frame = proto.encodeSub({ correlationId, topic });
    await this.send(frame);
    return correlationId;
  }

  /**
   * Remove a subscription. The server keys pub/sub unsub off the topic carried
   * in the stream_name header field.
   * @param {object} o
   * @param {string} o.topic
   */
  unsubscribe({ topic }) {
    const correlationId = this.nextCorrelationId();
    const frame = proto.encodeUnsub({
      correlationId,
      name: topic,
      unsubType: C.UNSUB_TYPE.PUBSUB,
    });
    return this.send(frame);
  }
}

module.exports = { Subscriber };
