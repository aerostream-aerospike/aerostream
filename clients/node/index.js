'use strict';

/*
 * aerostream — Node.js client for the AeroStream engine (Aerospike + streams).
 *
 * Standalone client over a raw TCP/TLS socket; does not depend on the native
 * Aerospike Node addon. Speaks the 8 AeroStream wire message types on port 3000.
 */

const constants = require('./lib/constants');
const proto = require('./lib/proto');
const { Connection } = require('./lib/connection');
const { Producer } = require('./lib/producer');
const { Consumer } = require('./lib/consumer');
const { Subscriber } = require('./lib/subscriber');

module.exports = {
  Producer,
  Consumer,
  Subscriber,
  Connection,

  // Protocol constants for callers (SEEK, ACK_MODE, STATUS, ALL_PARTITIONS, ...).
  ...constants,

  // Low-level codec, exposed for advanced/debug use.
  proto,
};
