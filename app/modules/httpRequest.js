'use strict';

var http = require('http');

const httpRequest = function httpRequest(data, callback) {
  var host = data.host;
  var port = data.port || 80;
  var path = data.path || '/';
  http.get({
    hostname: host,
    port: port,
    path: path,
    agent: false
  }, function(res) {
    var body = '';

    res.on("data", function(chunk) {
      body += chunk;
    });

    res.on("end", function() {
      var responseData = JSON.parse(body);

      callback(null, responseData);
    });
  }).on('error', function(e) {
    console.log("Got error: " + e.message);
    return callback(err, null);
  });
};

module.exports = httpRequest;
