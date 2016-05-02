'use strict';
/**
 * @module pullypusher
 */

const fs = require('fs');
const async = require('async');
const config = require('config');
const CronJob = require('cron').CronJob;

// Should load all modules dynamically
var httpRequest = require('./app/modules/httpRequest');
var EsPusher = require('./app/modules/esPusher');
var esPusher = new EsPusher({
  host: config.elasticsearch.HOST,
  port: config.elasticsearch.PORT,
  ssl: config.elasticsearch.SSL,
  log: config.elasticsearch.LOG,
  index: config.elasticsearch.INDEX,
  type: config.elasticsearch.TYPE,
  user: config.elasticsearch.USER,
  pass: config.elasticsearch.PASS
});
var MongoPuller = require('./app/modules/mongoPuller');
var mongoPuller = new MongoPuller({
  host: config.mongodb.HOST,
  dbName: config.mongodb.DB_NAME,
  port: config.mongodb.PORT,
  ssl: config.mongodb.SSL,
  sslValidate: config.mongodb.SSL_VALIDATE,
  user: config.mongodb.USER,
  pass: config.mongodb.PASS
});


/*
httpRequest({ host: 'status.driveshare.org', path: '/api/total'}, function(err, response) {
  console.log("httpRequest callback: ", response);
});
*/

var totalFarmerData = function totalFarmerData() {
  httpRequest({
    host: config.http.HOST,
    path: config.http.PATH
  }, function(responseData) {
    var esData = {
      "totalTB": responseData.total_TB,
      "totalFarmers": responseData.total_farmers
    };
    esPusher.push(esData, function(err) {
      console.log("Done sending totalFarmerData to ES");
    });
  });
};

var pullFromMongo = function pullFromMongo(data) {
  console.log("Pulling data from MongoDB");

  var apiStatsData = {};
  var apiFileData = {};

  async.parallel([
    function(callback) {
      mongoPuller.pull({
        collection: 'contacts',
        method: 'count'
      }, function(err, count) {
        console.log("contacts: " + count);
        apiStatsData.contactsCount = count;
        callback(err, 'contacts');
      });
    },
    function(callback) {
      mongoPuller.pull({
        collection: 'shards',
        method: 'count'
      }, function(err, count) {
        console.log("shards: " + count);
        apiStatsData.shardsCount = count;
        callback(err, 'shards');
      });
    },
    function(callback) {
      mongoPuller.pull({
        collection: 'users',
        method: 'count'
      }, function(err, count) {
        console.log("users: " + count);
        apiStatsData.usersCount = count;
        callback(err, 'users');
      });
    },
    function(callback) {
      mongoPuller.pull({
        collection: 'publickeys',
        method: 'count'
      }, function(err, count) {
        console.log("publickeys: " + count);
        apiStatsData.publickeysCount = count;
        callback(err, 'publickeys');
      });
    },
    function(callback) {
      mongoPuller.pull({
        collection: 'tokens',
        method: 'count',
        query: { operation: 'PUSH' }
      }, function(err, count) {
        console.log("tokens PUSH: " + count);
        apiStatsData.tokensPush = count;
        callback(err, 'tokensPush');
      });
    },
    function(callback) {
      mongoPuller.pull({
        collection: 'tokens',
        method: 'count',
        query: { operation: 'PULL' }
      }, function(err, count) {
        if (err) {
          console.log("Error while pulling tokens: " + err);
        }
        console.log("tokens PULL: " + count);
        apiStatsData.tokensPull = count;
        callback(err, 'tokensPull');
      });
    },
    function(callback) {
      mongoPuller.pull({
        collection: 'files',
        method: 'aggregate',
        query: [{ $group: { _id: null, total: { $sum: "$size" }}}]
      }, function(err, result) {
        if (err) {
          console.log("Error while pulling aggregations: " + err);
        }
        apiStatsData.totalSize = result[0].total;
        console.log("Aggregation: ", result[0].total);
        callback(err, 'aggregate');
      });
    }
  ], function(err, result) {
      finish(apiStatsData);
      console.log("Started finish...");
    }
  );

  function finish(apiStatsData) {
    console.log("Running FINISH");

    console.log("apiStatsData: ", apiStatsData);

    console.log("Sending apiStatsData to ES: ", apiStatsData);

    esPusher.push(apiStatsData, function(err) {
      if (err) {
        return console.log("Error writing API file data to ES: " + err);
      }

      console.log("Wrote File data to ES");
    });
  }
}

new CronJob('* 0,10,20,30,40,50 * * * *', function() {
  console.log("[CRON] Running pullFromMongo()");
  pullFromMongo();
}, function() {
  console.log("[CRON] Done running pullFromMongo()");
}, true);

process.on('SIGINT', function() {
  exitGracefully();
});

process.on('SIGTERM', function() {
  exitGracefully();
});

const exitGracefully = function exitGracefully() {
  console.log("Exiting...");

  mongoPuller.close(function() {
    console.log("[INDEX] Closed Mongo connection");

    esPusher.close(function() {
      console.log("[INDEX] Closed ES connection");

      process.exit();
    });
  });
};
