'use strict';

function EsPusher(data) {
  this.es = require('elasticsearch');
  this.host = data.host || '127.0.0.1';
  this.port = data.port || 9200;
  this.ssl = data.ssl || false;
  this.user = data.user;
  this.pass = data.pass;
  this.log = data.log;
  this.index = data.index || 'pushypuller';
  this.type = data.type || 'stat';

  this.buildURI = function buildURI() {
    var URI = "";

    if (this.ssl) {
      URI = "https://";
    } else {
      URI = "http://";
    }

    if (this.user && this.pass) {
      URI += this.user + ":" + this.pass + "@";
    }

    URI += this.host + ":" + this.port;

    return URI;
  }

  this.uri = this.buildURI();

  this.client = new this.es.Client({
    host: this.uri,
    log: this.log
  });

  this.push = function push(data, callback) {
    console.log("Data: ", data);

    data.tags = [ 'pullypusher' ];
    data.timestamp = new Date();

    this.client.create({
      index: this.index,
      type: this.type,
      body: data
    }, function(err, response) {
      if (err) {
        console.log("Got error: " + err);
        return callback(err);
      }

      console.log("Done sending to ES, calling callback");

      callback(null, response);
    });
  };

  this.close = function close(callback) {
    this.client.close();
    return callback();
  };

};

module.exports = EsPusher;
