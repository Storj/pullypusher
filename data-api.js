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


var pullFromMongo = function pullFromMongo(data) {
  console.log("Pulling data from MongoDB");

  var statusReportData = {};

  async.parallel([
    function(callback) {
      mongoPuller.pull({
        collection: 'reports',
        method: 'count'
      }, function(err, count) {
        console.log("reports: " + count);
        statusReportData.reportCount = count;
        callback(err, 'reports');
      });
    },
    function(callback) {
      var dateFromMS = ( new Date() - 60000*10 );
      var dateFrom = new Date(dateFromMS);
      var dateTo = new Date();

      mongoPuller.pull({
        collection: 'reports',
        method: 'aggregate',
        query: [ { $match: { timestamp: { $gt: dateFrom, $lt: dateTo } } }, { $group: { _id: "$nodeID", storage_used: { $max: "$storage.used" } } }, {  $group: { _id: "$nodeID", total_storage_used: { $sum: "$storage_used" } } } ]

      }, function(err, result) {
        if (err) {
          console.log("Error while pulling aggregations: " + err);
        }
        statusReportData.storageUsed = result[0].total_storage_used;
        console.log("Aggregation (storage used): ", result[0].total_storage_used);
        callback(err, 'aggregate');
      });
    },
    function(callback) {
      var dateFromMS = ( new Date() - 60000*10 );
      var dateFrom = new Date(dateFromMS);
      var dateTo = new Date();

      mongoPuller.pull({
        collection: 'reports',
        method: 'aggregate',
        query: [ { $match: { timestamp: { $gt: dateFrom, $lt: dateTo } } }, { $group: { _id: "$nodeID", storage_free: { $max: "$storage.free" } } }, {  $group: { _id: "$nodeID", total_storage_free: { $sum: "$storage_free" } } } ]

      }, function(err, result) {
        if (err) {
          console.log("Error while pulling aggregations: " + err);
        }
        statusReportData.storageFree = result[0].total_storage_free;
        console.log("Aggregation (storage free): ", result[0].total_storage_free);
        callback(err, 'aggregate');
      });
    }
  ], function(err, result) {
      finish(statusReportData);
      console.log("Started finish...");
    }
  );

  function finish(statusReportData) {
    console.log("Running FINISH");

    console.log('Done with Mongo, trying to close connection...');

    mongoPuller.close(function(err) {
      if (err) {
        console.log('Error closing mongo connection: %s', err);
      }

      console.log('Closed mongo connection');
    });

    statusReportData.totalStorage = ( statusReportData.storageFree + statusReportData.storageUsed );

    console.log("Sending statusReportData to ES: ", statusReportData);

    esPusher.push(statusReportData, function(err) {
      if (err) {
        return console.log("Error writing API file data to ES: " + err);
      }

      console.log("Wrote File data to ES");
    });
  }
}

new CronJob('*/2 * * * *', function() {
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
