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
var mongo = require('mongodb');

var mongoPullerConfig = {
  host: config.mongodb.HOST,
  dbName: config.mongodb.DB_NAME,
  port: config.mongodb.PORT,
  ssl: config.mongodb.SSL,
  sslValidate: config.mongodb.SSL_VALIDATE,
  user: config.mongodb.USER,
  pass: config.mongodb.PASS
};

var userDataFilePath = './userDataFile.json';

var mongoPuller = new MongoPuller(mongoPullerConfig);

var start = function start() {
  mongoPuller.open(function(err) {
    if (err) {
      return console.log('Error opening mongo connection');
    }

    pullFromMongo();
  });
};

var pullFromMongo = function pullFromMongo(data) {
  console.log("Pulling data from MongoDB");

  var bridgeUsers = {};

  async.series([
    function(callback) {
      // Grab all users from the bridge
      mongoPuller.pull({
        collection: 'users',
        method: 'find',
        query: {}

      }, function(err, usersResult) {
        var self = this;
        if (err) {
          console.log("Error while pulling aggregations: " + err);
        }

        if (!usersResult) {
          console.log('Found end of cursor, finishing up');
          return callback(err, 'addusers');
        }

        // Get the count of users so we know when we're done processing them
        // (probably a better way, checking for null on cursor result or something)
        usersResult.count(function(err, userCount) {
          var count = 0;
          console.log('Found %s users', userCount);

          // Loop through each resulting user
          usersResult.each(function(err, user) {
            count++;
            if (err) {
              console.log('Error iterating over users');
              return callback(err, null);
            }

            if (user === null) {
              console.log('Reached end of cursor for users');
              return callback(null, null);
            }

            // Add each user to the bridgeUsers object along with existing desired data
            console.log('[ %s of %s ] Adding user %s to userlist, created is %s', count, userCount, user._id, user.created.toString());
            bridgeUsers[user._id] = {
              email: user._id,
              activated: user.activated,
              created: user.created.toString()
            };
          });
        });
      });
    },
    function(callback) {
      var totalUserCount = Object.keys(bridgeUsers).length;
      var totalFrameCount = 0;
      var totalShardCount = 0;
      var userCounter = 0;
      var frameCounter = 0;
      var shardCounter = 0;

      console.log('Starting to calculate data for storage used for %s users', totalUserCount);

      Object.keys(bridgeUsers).forEach(function(userID) {
        // Pull all frames for that user so that we can calculate storage space
        // and transfer used for each

        bridgeUsers[userID].shards = [];
        bridgeUsers[userID].totalDataStored = 0;
        userCounter++;

        console.log('[USERS][ %s of %s ] Processing storage used for %s', userCounter, totalUserCount, userID);

        mongoPuller.pull({
          collection: 'frames',
          method: 'find',
          query: { user: userID }
        }, function(err, framesResult) {
          if (err) {
            return console.log('Error pulling frames for user %s', userID);
          }

          if (!framesResult) {
            return console.log('No frames found for user %s', userID);
          }

          // For each frame, add the size and save as totalDataStored to bridgeUsers for appropriate user
          framesResult.count(function(err, frameCount) {
            if (err) {
              return console.log('Error getting frame count: %s', err);
            }

            if (!frameCount) {
              process.stdout.write('x');
              return;
            }

            console.log('Adding %s frames to totalFrameCount of %s', frameCount, totalFrameCount);
            totalFrameCount += frameCount;

            framesResult.each(function(err, frame) {
              if (err) {
                return console.log('Error processing frame for user %s', userID);
              }

              if (frame === null) {
                // This is end of cursor
                return console.log('End of framesResult cursor for user %s', userID);
              }

              var shards = frame.shards;

              frameCounter++;
              bridgeUsers[userID].totalDataStored += frame.size;
              totalShardCount += frame.shards.length;

              //console.log('Processing %s shards', frame.shards.length);

              shards.forEach(function(shard) {
                shardCounter++;

                //console.log('[SHARDS][ %s of %s ] Processing shards for user %s', shardCounter, shardCount, userID);
                console.log('Pushing shard %s to user %s');
                bridgeUsers[userID].shards.push(shard.toString());

              });

              console.log('Shard Counter: [ %s / %s ]  Frame Counter: [ %s / %s ]  User Counter: [ %s / %s ]', shardCounter, totalShardCount, frameCounter, totalFrameCount, userCounter, totalUserCount);
              if ((shardCounter === totalShardCount) && (frameCounter === totalFrameCount) && (userCounter === totalUserCount)) {
                console.log('Processed last shard, %s out of %s', shardCounter, totalShardCount);
                return callback(null, null);
              }

              //console.log('Calculated total data stored %s, for %s, adding to total', totalDataStored, userID);
              //console.log('[FRAMES][ %s of %s ] totalDataStored for %s is %s with %s shards', frameCounter, totalFrameCount, userID, bridgeUsers[userID].totalDataStored, bridgeUsers[userID].shards.length);
            });
          });
        });
      });
    },
    function(callback) {
      // Iterate over each user and its shards and find the size and download count to create download total
      var self = this;
      var totalUserCount = Object.keys(bridgeUsers).length;
      var totalShardCount = 0;
      var totalContractCount = 0;
      var userCounter = 0;
      var shardCounter = 0;
      var contractCounter = 0;

      console.log('Starting transfer calculations...');

      Object.keys(bridgeUsers).forEach(function(userID) {
        bridgeUsers[userID].totalDataTransferred = 0;
        userCounter++;

        var totalDataDownloaded = 0;
        totalShardCount += bridgeUsers[userID].shards.length;

        console.log('Looking through %s shards for user %s', totalShardCount, userID);

        bridgeUsers[userID].shards.forEach(function(shardId) {
          shardCounter++;
          console.log('Working on shard %s for user %s', shardCounter, userID);

          //console.log('Searching for shard with id %s', shardId);
          var shardObjectId = new mongo.ObjectID(shardId);

          mongoPuller.pull({
            collection: 'shards',
            method: 'findOne',
            query: { '_id': shardObjectId }
          }, function(err, shard) {
            if (err) {
              return console.log('Error finding shard: %s', err);
            }

            console.log('Users: [ %s / %s ]  Shards: [ %s / %s ]  Contracts: [ %s / %s ]', userCounter, totalUserCount, shardCounter, totalShardCount, contractCounter, totalContractCount);

            if (!shard) {
              return console.log('Shard %s for user %s not found', shardId, userID);
            }

            console.log('Found shard %s for user %s', shardId, userId);

            totalContractCount += shard.contracts.length;
            var contractMap = {};
            var totalDataDownloaded = 0;

            console.log('shard: ', shard);

            // Add each of the contracts to a contractMap (key: id)
            shard.contracts.forEach(function(contract) {
              contractMap[contract.nodeID] = contract.contract;
            });

            shard.meta.forEach(function(metaItem) {
              var shardContractDownloaded = ( metaItem.meta.downloadCount * contractMap[metaItem.nodeID].data_size );
              totalDataDownloaded += shardContractDownloaded;
            });

            bridgeUsers[userID].totalDataDownloaded += totalDataDownloaded;
            console.log('totalDataDownloaded for %s is %s', userID, totalDataDownloaded);

            if (shardCounter === totalShardCount) {
              console.log('Done processing all shards. Calling callback.');
              return callback(null, null);
            }
          });
        });
      });
    }
  ], function(err, result) {
      finish(bridgeUsers);
      console.log("Started finish...");
    }
  );

  function finish(data) {
    console.log("Running FINISH");

    console.log("Saving userlist to file");

    fs.writeFile(userDataFilePath, JSON.stringify(data, null, 2), function(err) {
      if (err) {
        console.log('Error writing file: %s', err);
      }

      if (!err) {
        console.log('User data file written to %s', userDataFilePath);
      }
    })

    console.log('Done with Mongo, trying to close connection...');

    mongoPuller.close(function(err) {
      if (err) {
        console.log('Error closing mongo connection: %s', err);
      }

      console.log('Closed mongo connection');
    });
  }
};

start();

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
