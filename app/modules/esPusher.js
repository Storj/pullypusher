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
  this.date = data.date;


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

  console.log('host: %s', this.uri);

  this.client = new this.es.Client({
    host: this.uri,
    log: this.log
  });

  this.push = function push(data, callback) {
    var data = data;
    //console.log("Data: ", data);

  //console.log('ES pusher config: host: %s, port %s, ssl: %s, user: %s pass: %s, index: %s, type: %s, date: %s', this.host, this.port, this.ssl, this.user, this.pass, this.index, this.type, this.date);

    // Add option to add custom tags here
    data.tags = [ 'pullypusher' ];

    if (!data["@timestamp"]) {
      data["@timestamp"] = new Date();
    }

    if (!data.timestamp) {
      data.timestamp = new Date();
    }

    //console.log('data.timestamp: %s', data.timestamp);

    var date = data.timestamp;
    var indexDate = date.getFullYear() + '.' + ("0" + (date.getMonth() + 1)).slice(-2) + '.' + ("0" + date.getDate()).slice(-2);

    //console.log('index name %s', this.index + '-' + indexDate);

    this.client.create({
      index: this.index + "-" + indexDate,
      type: this.type,
      body: data
    }, function(err, response) {
      //console.log('Created ES client');

      if (err) {
        console.log("Got error: " + err);
        return callback(err);
      }

      //console.log("Done sending to ES, calling callback");

      callback(null, response);
    });
  };

  this.close = function close(callback) {
    this.client.close();
    return callback();
  };

};

module.exports = EsPusher;
