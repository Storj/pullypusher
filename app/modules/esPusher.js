'use strict';

const config = require('config');
const merge = require('merge');
const es = require('elasticsearch');

function EsPusher(options) {
  if (!(this  instanceof EsPusher)) {
    return new EsPusher(options);
  }

  if (!options) {
    options = {};
  }

  options = merge(config.elasticsearch, options);

  this._init(options);
}

EsPusher.prototype._init = function _init(options) {
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

EsPusher.prototype.buildURI = function buildURI() {
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

EsPusher.prototype.push = function push(options, callback) {
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


EsPusher.prototype.close = function close(callback) {
  this.client.close();
  return callback();
};


module.exports = EsPusher;
