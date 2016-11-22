'use strict';
/**
 * @module pullypusher
 */

const async = require('async');
const config = require('config');
const CronJob = require('cron').CronJob;
const MongoPuller = require('../app/modules/mongoPuller');
const reportOutputFile = './telemetryReport.json';
const fs = require('fs');
const mongo = require('mongodb');
const ObjectID = mongo.ObjectID;

const mongoPullerConfig = {
  host: config.mongodb.host,
  dbName: config.mongodb.db_name,
  port: config.mongodb.port,
  ssl: config.mongodb.ssl,
  sslValidate: config.mongodb.ssl_validate,
  user: config.mongodb.user,
  pass: config.mongodb.pass
};

var mongoPuller = new MongoPuller(mongoPullerConfig);

function objectIDWithTimestamp(timestamp) {
    // Convert string date to Date object (otherwise assume timestamp is a date)
    if (typeof(timestamp) == 'string') {
        timestamp = new Date(timestamp);
    }

    // Convert date object to hex seconds since Unix epoch
    var hexSeconds = Math.floor(timestamp/1000).toString(16);

    // Create an ObjectId with that hex timestamp
    var constructedObjectID = ObjectID(hexSeconds + "0000000000000000");

    return constructedObjectID
}

var pullFromMongo = function pullFromMongo() {
  console.log('Pulling data from MongoDB');

  mongoPuller.open(function(err) {
    if (err) {
      return console.log('Error connecting to mongo: %s', err);
    }

    console.log('Opened mongo connection from data-api.js');

    function finish(telemetryReport) {
      console.log('Running FINISH');

      console.log('Done with Mongo, trying to close connection...');

      mongoPuller.close(function(err) {
        if (err) {
          console.log('Error closing mongo connection: %s', err);
        }

        console.log('Closed mongo connection');
      });

      fs.writeFile(reportOutputFile, JSON.stringify(telemetryReport, null, 2), function(err) {
        if (err) {
          return console.log('Error writing telemetry report');
        }

        console.log('Wrote telemetry report to %s', reportOutputFile);
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
        var startDateId = objectIDWithTimestamp('2016-10-01T00:00:00.0Z');
        var endDateId = objectIDWithTimestamp('2016-11-01T00:00:00.0Z');

        console.log('Running report aggregation for start: %s, end: %s', startDateId, endDateId);

        mongoPuller.pull({
          collection: 'reports',
          method: 'aggregate',
          query: [{
            $match: {
              _id: {
                '$gte': startDateId,
                '$lt': endDateId
              }
            }
          },
          {
            $group: {
              _id: {
                'node_id': '$contact.nodeID'
              },
              reportCount: {
                '$sum': 1
              },
              payment: {
                $first: '$payment'
              },
              signature: {
                $first: '$signature'
              },
              storage: {
                $first: '$storage'
              },
              bandwidth: {
                $first: '$bandwidth'
              },
              contact: {
                $first: '$contact'
              }
            }
          },
          {
            $group: {
              _id: {
                'node_id': '$_id.node_id'
              },
              totalCount: {
                '$sum': '$reportCount'
              },
              distinctCount: {
                '$sum': 1
              },
              payment: {
                '$first': '$payment'
              },
              signature: {
                '$first': '$signature'
              },
              storage: {
                '$first': '$storage'
              },
              bandwidth: {
                '$first': '$bandwidth'
              },
              contact: {
                '$first': '$contact'
              }
            }
          }]
        }, function(err, telemetryReport) {
          if (err) {
            console.log('Error while pulling aggregations: %s', err);
            return callback(err);
          }

          callback(err, telemetryReport);
        });
      }
    ], function(err, telemetryReport) {
        finish(telemetryReport);
        console.log('Kickihng off finish...');
      }
    );
  });
};

var start = function start() {
  console.log('[CRON] Running pullFromMongo()');
  pullFromMongo();
};

const exitGracefully = function exitGracefully() {
  console.log('Exiting...');

  mongoPuller.close(function() {
    console.log('[INDEX] Closed Mongo connection');
  });
};

start();

process.on('SIGINT', function() {
  exitGracefully();
});

process.on('SIGTERM', function() {
  exitGracefully();
});
