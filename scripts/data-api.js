'use strict';
/**
 * @module pullypusher
 */

const async = require('async');
const config = require('config');
const CronJob = require('cron').CronJob;

// Should load all modules dynamically
// var httpRequest = require('../app/modules/httpRequest');
var EsPusher = require('../app/modules/esPusher');
var MongoPuller = require('../app/modules/mongoPuller');

var esPusher = new EsPusher({
  host: config.elasticsearch.host,
  port: config.elasticsearch.port,
  ssl: config.elasticsearch.ssl,
  log: config.elasticsearch.log,
  index: config.elasticsearch.index,
  type: config.elasticsearch.type,
  user: config.elasticsearch.user,
  pass: config.elasticsearch.pass
});

var mongoPullerConfig = {
  host: config.mongodb.host,
  dbName: config.mongodb.db_name,
  port: config.mongodb.port,
  ssl: config.mongodb.ssl,
  sslValidate: config.mongodb.ssl_validate,
  user: config.mongodb.user,
  pass: config.mongodb.pass
};

var mongoPuller = new MongoPuller(mongoPullerConfig);

var pullFromMongo = function pullFromMongo() {
  console.log('Pulling data from MongoDB');

  mongoPuller.open(function(err) {
    if (err) {
      return console.log('Error connecting to mongo: %s', err);
    }

    console.log('Opened mongo connection from data-api.js');

    function finish(statusReportData) {
      console.log('Running FINISH');

      console.log('Done with Mongo, trying to close connection...');

      mongoPuller.close(function(err) {
        if (err) {
          console.log('Error closing mongo connection: %s', err);
        }

        console.log('Closed mongo connection');
      });

      statusReportData.totalStorage = (
        statusReportData.storageFree + statusReportData.storageUsed
      );

      console.log('Sending statusReportData to ES: %s', statusReportData);

      esPusher.push(statusReportData, function(err) {
        if (err) {
          return console.log('Error writing API file data to ES: %s', err);
        }

        console.log('Wrote File data to ES');
      });
    }

    var statusReportData = {};

    async.parallel([
      function(callback) {
        mongoPuller.pull({
          collection: 'reports',
          method: 'count'
        }, function(err, count) {
          console.log('reports: %s', count);
          statusReportData.reportCount = count;
          callback(err, 'reports');
        });
      },
      function(callback) {
        var dateFromMS = ( new Date() - 60000*60 );
        var dateFrom = new Date(dateFromMS);
        var dateTo = new Date();

        mongoPuller.pull({
          collection: 'reports',
          method: 'aggregate',
          query: [
            { $match:
              { timestamp:
                { $gt: dateFrom, $lt: dateTo }
              }
            },
            { $group:
              { _id: '$nodeID', storage_used:
                { $max: '$storage.used' }
              }
            },
            {  $group:
              { _id: '$nodeID', total_storage_used:
                { $sum: '$storage_used' }
              }
            }
          ]

        }, function(err, result) {
          if (err) {
            console.log('Error while pulling aggregations: %s', err);
          }
          statusReportData.storageUsed = result[0].total_storage_used;
          var storageUsed = result[0].total_storage_used;

          console.log('Aggregation (storage used): %s', storageUsed);
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
          query: [
            { $match:
              { timestamp:
                { $gt: dateFrom, $lt: dateTo }
              }
            },
            { $group:
              { _id: '$nodeID', storage_free:
                { $max: '$storage.free' }
              }
            },
            {  $group:
              { _id: '$nodeID', total_storage_free:
                { $sum: '$storage_free' }
              }
            }
          ]
        }, function(err, result) {
          if (err) {
            console.log('Error while pulling aggregations: %s', err);
          }
          statusReportData.storageFree = result[0].total_storage_free;
          var storageFree = result[0].total_storage_free;
          console.log('Aggregation (storage free): %s', storageFree);
          callback(err, 'aggregate');
        });
      }
    ], function(err, result) {
        finish(statusReportData);
        console.log('Started finish (from %s)...', result);
      }
    );
  });
};

var start = function start() {
  var myCron = new CronJob('*/10 * * * *', function() {
    console.log('[CRON] Running pullFromMongo()');
    pullFromMongo();
  }, function() {
    console.log('[CRON] Done running pullFromMongo()');
  }, true);
  myCron.start();
};

const exitGracefully = function exitGracefully() {
  console.log('Exiting...');

  mongoPuller.close(function() {
    console.log('[INDEX] Closed Mongo connection');

    esPusher.close(function() {
      console.log('[INDEX] Closed ES connection');

      process.exit();
    });
  });
};

start();

process.on('SIGINT', function() {
  exitGracefully();
});

process.on('SIGTERM', function() {
  exitGracefully();
});
