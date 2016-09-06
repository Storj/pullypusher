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

/*
 * Leaving here for example
httpRequest({
  host: 'status.driveshare.org',
  path: '/api/total'
}, function(err, response) {
  console.log("httpRequest callback: ", response);
});
*/

/*
 * Leaving here for example
var totalFarmerData = function totalFarmerData() {
  httpRequest({
    host: config.http.HOST,
    path: config.http.PATH
  }, function(responseData) {
    var esData = {
      'totalTB': responseData.total_TB,
      'totalFarmers': responseData.total_farmers
    };
    esPusher.push(esData, function(err) {
      console.log('Done sending totalFarmerData to ES');
    });
  });
};
*/

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

var pullFromMongo = function pullFromMongo() {
  console.log('Pulling data from MongoDB');

  mongoPuller.open(function(err) {
    if (err) {
      return console.log('Error opening mongodb connection: %s', err);
    }

    console.log('Opened mongo connection from index.js');

    function finish(apiStatsData) {
      console.log('Running FINISH');

      console.log('Done with Mongo, trying to close connection...');

      mongoPuller.close(function(err) {
        if (err) {
          console.log('Error closing mongo connection: %s', err);
        }

        console.log('Closed mongo connection');
      });

      console.log('apiStatsData: ', apiStatsData);

      console.log('Sending apiStatsData to ES: ', apiStatsData);

      esPusher.push(apiStatsData, function(err) {
        if (err) {
          return console.log('Error writing API file data to ES: ', err);
        }

        console.log('Wrote File data to ES');
      });
    }

    var apiStatsData = {};

    async.parallel([
      function(callback) {
        mongoPuller.pull({
          collection: 'contacts',
          method: 'count'
        }, function(err, count) {
          console.log('contacts: ' + count);
          apiStatsData.contactsCount = count;
          callback(err, 'contacts');
        });
      },
      function(callback) {
        mongoPuller.pull({
          collection: 'shards',
          method: 'count'
        }, function(err, count) {
          console.log('shards: ', count);
          apiStatsData.shardsCount = count;
          callback(err, 'shards');
        });
      },
      function(callback) {
        mongoPuller.pull({
          collection: 'frames',
          method: 'count'
        }, function(err, count) {
          console.log('frames: ', count);
          apiStatsData.framesCount = count;
          callback(err, 'frames');
        });
      },
      function(callback) {
        mongoPuller.pull({
          collection: 'bucketentries',
          method: 'count'
        }, function(err, count) {
          console.log('bucketentries: ', count);
          apiStatsData.bucketentriesCount = count;
          callback(err, 'bucketentries');
        });
      },
      function(callback) {
        mongoPuller.pull({
          collection: 'users',
          method: 'count'
        }, function(err, count) {
          console.log('users: ', count);
          apiStatsData.usersCount = count;
          callback(err, 'users');
        });
      },
      function(callback) {
        mongoPuller.pull({
          collection: 'publickeys',
          method: 'count'
        }, function(err, count) {
          console.log('publickeys: ', count);
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
          console.log('tokens PUSH: ', count);
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
            console.log('Error while pulling tokens: ', err);
          }
          console.log('tokens PULL: ', count);
          apiStatsData.tokensPull = count;
          callback(err, 'tokensPull');
        });
      },
      function(callback) {
        mongoPuller.pull({
          collection: 'frames',
          method: 'aggregate',
          query: [
            { $unwind: '$shards' },
            { $group: { _id: null, total: { $sum: '$shards.size' }}}
          ]
        }, function(err, result) {
          if (err) {
            console.log('Error while pulling aggregations: ', err);
          }
          apiStatsData.totalSize = result[0].total;
          console.log('Aggregation: ', result[0].total);
          callback(err, 'aggregate');
        });
      }
    ], function(err, result) {
      if (err) {
        return console.log('Error returned from %s', result);
      }

      finish(apiStatsData);
      console.log('Started finish...');
    });
  });
};


var start = function start() {
  var cronJob = new CronJob('*/1 * * * *', function() {
    console.log('[CRON] Running pullFromMongo()');
    pullFromMongo();
  }, function() {
    console.log('[CRON] Done running pullFromMongo()');
  }, true);
  cronJob.start();
};

start();

process.on('SIGINT', function() {
  exitGracefully();
});

process.on('SIGTERM', function() {
  exitGracefully();
});

