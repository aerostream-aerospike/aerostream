'use strict';

const net = require('net');
const tls = require('tls');
const { EventEmitter } = require('events');

const C = require('./constants');
const proto = require('./proto');

/*
 * Connection — a single TCP (or TLS) socket to an AeroStream server with
 * frame reassembly and correlation-id allocation.
 *
 * AeroStream rides the Aerospike wire protocol: every message is an 8-byte
 * header (version, type, 6-byte big-endian size) followed by a body of that
 * size. TCP gives us an arbitrary byte stream, so we buffer and slice out
 * complete frames before dispatching.
 *
 * Subclasses (Producer/Consumer/Subscriber) implement _handleFrame(type, body).
 *
 * Events:
 *   'connect'        — socket established
 *   'close'          — socket closed
 *   'error', err     — socket or protocol error
 */
class Connection extends EventEmitter {
  /**
   * @param {object} opts
   * @param {string} [opts.host='127.0.0.1']
   * @param {number} [opts.port=3000]
   * @param {boolean|object} [opts.tls=false] - true or a tls.connect options object
   * @param {number} [opts.connectTimeoutMs=5000]
   */
  constructor(opts = {}) {
    super();
    this.host = opts.host || '127.0.0.1';
    this.port = opts.port || 3000;
    this.tls = opts.tls || false;
    this.connectTimeoutMs = opts.connectTimeoutMs || 5000;

    this._socket = null;
    this._buf = Buffer.alloc(0);
    this._corr = 0n;
    this._connected = false;
  }

  nextCorrelationId() {
    this._corr = (this._corr + 1n) & 0xFFFFFFFFFFFFFFFFn;
    if (this._corr === 0n) this._corr = 1n;
    return this._corr;
  }

  connect() {
    if (this._connected) return Promise.resolve(this);

    return new Promise((resolve, reject) => {
      const onError = (err) => {
        cleanup();
        reject(err);
      };
      const onConnect = () => {
        this._connected = true;
        this._socket.removeListener('error', onError);
        this._socket.setNoDelay(true);
        this._socket.on('data', (chunk) => this._onData(chunk));
        this._socket.on('error', (err) => this.emit('error', err));
        this._socket.on('close', () => {
          this._connected = false;
          this.emit('close');
        });
        this.emit('connect');
        resolve(this);
      };
      const cleanup = () => {
        if (this._socket) {
          this._socket.removeListener('error', onError);
        }
      };

      if (this.tls) {
        const tlsOpts = typeof this.tls === 'object' ? this.tls : {};
        this._socket = tls.connect(
          { host: this.host, port: this.port, ...tlsOpts },
          onConnect
        );
      } else {
        this._socket = net.connect({ host: this.host, port: this.port }, onConnect);
      }

      this._socket.setTimeout(this.connectTimeoutMs, () => {
        // Only relevant pre-connect; clear once connected.
        if (!this._connected) {
          this._socket.destroy(new Error('connect timeout'));
        }
      });
      this._socket.once('error', onError);
      this._socket.once('connect', () => this._socket.setTimeout(0));
    });
  }

  _onData(chunk) {
    this._buf = this._buf.length === 0 ? chunk : Buffer.concat([this._buf, chunk]);

    // Slice out every complete frame currently buffered.
    for (;;) {
      if (this._buf.length < C.HEADER_SZ) return;

      const { type, size } = proto.parseHeader(this._buf);
      const frameLen = C.HEADER_SZ + size;

      if (this._buf.length < frameLen) return; // wait for the rest

      const body = this._buf.subarray(C.HEADER_SZ, frameLen);

      // Hold the body before advancing the buffer window.
      const bodyCopy = Buffer.from(body);
      this._buf = this._buf.subarray(frameLen);

      try {
        this._handleFrame(type, bodyCopy);
      } catch (err) {
        this.emit('error', err);
      }
    }
  }

  // Override in subclasses.
  _handleFrame(type, body) {
    this.emit('frame', type, body);
  }

  send(buf) {
    if (!this._socket || !this._connected) {
      return Promise.reject(new Error('not connected'));
    }
    return new Promise((resolve, reject) => {
      this._socket.write(buf, (err) => (err ? reject(err) : resolve()));
    });
  }

  close() {
    return new Promise((resolve) => {
      if (!this._socket) return resolve();
      this._socket.end(() => {
        this._socket.destroy();
        this._connected = false;
        resolve();
      });
    });
  }
}

module.exports = { Connection };
