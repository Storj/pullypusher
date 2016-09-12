'use strict';

const config = require('config');
const merge = require('merge');
const es = require('elasticsearch');

function Es(options) {
  if (!(this  instanceof Es)) {
    return new Es(options);
  }

  if (!options) {
    options = {};
  }

  options = merge(config.elasticsearch, options);

  this._init(options);
}

Es.prototype._init = function _init(options) {
  this.host = options.host;
  this.port = options.port;
  this.ssl = options.ssl;
  this.user = options.user;
  this.pass = options.pass;
  this.log = options.log;
  this.index = options.index;
  this.type = options.type;
  this.date = options.date;
  this.uri = this.buildURI();

  console.log('host: %s', this.uri);

  this.client = new es.Client({
    host: this.uri,
    log: this.log
  });

};

Es.prototype.buildURI = function buildURI() {
  var URI = '';

  if (this.ssl) {
    URI = 'https://';
  } else {
    URI = 'http://';
  }

  if (this.user && this.pass) {
    URI += this.user + ':' + this.pass + '@';
  }

  URI += this.host + ':' + this.port;

  return URI;
};

Es.prototype.pull = function pull(options, callback) {
  this.client.search({
    index: options.index || this.index,
    type: options.type || this.type,
    body: options.body
  }, function(err, response) {
    if (err) {
      console.log('Got error while pulling: %s', err);
      return callback(err, null);
    }

    return callback(null, response);
  });
};

Es.prototype.push = function push(options, callback) {
  // Add option to add custom tags here
  options.tags = [ 'pullypusher' ];

  if (!options['@timestamp']) {
    options['@timestamp'] = new Date();
  }

  if (!options.timestamp) {
    options.timestamp = new Date();
  }

  //console.log('options.timestamp: %s', options.timestamp);

  var date = options.timestamp;
  var indexDate = date.getFullYear() + '.' + (
    '0' + (date.getMonth() + 1)
  ).slice(-2) + '.' + ('0' + date.getDate()).slice(-2);

  //console.log('index name %s', this.index + '-' + indexDate);

  this.client.create({
    index: this.index + '-' + indexDate,
    type: this.type,
    body: options
  }, function(err, response) {
    //console.log('Created ES client');

    if (err) {
      console.log('Got error: ' + err);
      return callback(err);
    }

    //console.log("Done sending to ES, calling callback");

    callback(null, response);
  });
};


Es.prototype.close = function close(callback) {
  this.client.close();
  return callback();
};


module.exports = Es;
