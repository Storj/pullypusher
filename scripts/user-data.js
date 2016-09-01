'use strict';
/**
 * @module pullypusher
 */

const fs = require('fs');
const async = require('async');
const config = require('config');
const CronJob = require('cron').CronJob;

// Should load all modules dynamically
var httpRequest = require('../app/modules/httpRequest');
var EsPusher = require('../app/modules/esPusher');
var MongoPuller = require('../app/modules/mongoPuller');
var userDataFilePath = '../data-out/userDataFile.json';
var mongo = require('mongodb');

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

var mongoPullerConfig = {
  host: config.mongodb.HOST,
  dbName: config.mongodb.DB_NAME,
  port: config.mongodb.PORT,
  ssl: config.mongodb.SSL,
  sslValidate: config.mongodb.SSL_VALIDATE,
  user: config.mongodb.USER,
  pass: config.mongodb.PASS
};

var mongoPuller = new MongoPuller(mongoPullerConfig);

var start = function start() {
  pullFromMongo();
};

var pullFromMongo = function pullFromMongo(data) {
  console.log("Pulling data from MongoDB");

  var bridgeUsers = {};

  mongoPuller.open(function(err) {
    if (err) {
      return console.log('Error opening MongoDB connection: %s', err);
    }

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
        var totalPointerCount = 0;
        var userCounter = 0;
        var frameCounter = 0;
        var pointerCounter = 0;

        console.log('Starting to calculate data for storage used for %s users', totalUserCount);

        Object.keys(bridgeUsers).forEach(function(userID) {
          // Pull all frames for that user so that we can calculate storage space
          // and transfer used for each

          bridgeUsers[userID].pointers = [];
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

                var pointers = frame.shards;

                frameCounter++;
                //bridgeUsers[userID].totalDataStored += frame.size;
                totalPointerCount += frame.shards.length;

                //console.log('Processing %s shards from frame %s', frame.shards.length, JSON.stringify(frame));

                pointers.forEach(function(pointer) {
                  pointerCounter++;

                  //console.log('[SHARDS][ %s of %s ] Processing shards for user %s', shardCounter, shardCount, userID);
                  //console.log('Pushing shard %s to user %s', shard, userID);
                  bridgeUsers[userID].pointers.push(pointer);
                });

                console.log('Shard Counter: [ %s / %s ]  Frame Counter: [ %s / %s ]  User Counter: [ %s / %s ]', pointerCounter, totalPointerCount, frameCounter, totalFrameCount, userCounter, totalUserCount);
                if ((pointerCounter === totalPointerCount) && (frameCounter === totalFrameCount) && (userCounter === totalUserCount)) {
                  console.log('Processed last shard, %s out of %s', pointerCounter, totalPointerCount);
                  return callback(null, null);
                }
              });
            });
          });
        });
      },
      function(callback) {
        // Iterate over each user and its pointers and find the size and download count to create download total
        var self = this;
        var totalUserCount = Object.keys(bridgeUsers).length;
        var totalPointerCount = 0;
        var totalContractCount = 0;
        var userCounter = 0;
        var pointerCounter = 0;
        var contractCounter = 0;

        console.log('Starting transfer calculations...');

        // We have...
        // - pointer ID's for all users frames
        // Need...
        // - hash from the pointer with which we find the shard via hash field
        //
        // Here we should be pulling the pointers for each user and...
        // - Grabbing the size and saving to the bridgeUsers object

        Object.keys(bridgeUsers).forEach(function(userID) {
          bridgeUsers[userID].totalDataDownloaded = 0;
          bridgeUsers[userID].pointerMap = [];
          userCounter++;

          var totalDataDownloaded = 0;
          var userPointerCount = bridgeUsers[userID].pointers.length;
          totalPointerCount += userPointerCount;

          console.log('Looking through %s pointers for user %s', userPointerCount, userID);

          bridgeUsers[userID].pointers.forEach(function(pointerId) {
            //console.log('Working on pointer %s for user %s', pointerId, userID);

            var pointerObjectId = new mongo.ObjectID(pointerId);

            mongoPuller.pull({
              collection: 'pointers',
              method: 'findOne',
              query: { '_id': pointerObjectId }
            }, function(err, pointer) {
              pointerCounter++;

              if (err) {
                return console.log('Error finding shard: %s', err);
              }

              console.log('Users: [ %s / %s ]  Pointers: [ %s / %s ]', userCounter, totalUserCount, pointerCounter, totalPointerCount);

              if (!pointer) {
                return console.log('Shard %s for user %s not found', pointerId, userID);
              }

              console.log('Found shard %s for user %s', pointerId, userID);

              // Collect sizes and hash for each pointer
              bridgeUsers[userID].pointerMap[pointerId] = {
                size: pointer.size,
                hash: pointer.hash
              };

              if (( pointerCounter === totalPointerCount ) && ( userCounter === totalUserCount )) {
                console.log('Done processing all pointers. Calling callback.');

                // THIS CALLBACK IS BEING CALLED MORE THAN ONCE
                return callback(null, null);
              }
            });
          });
        });
      },
      function(callback) {
        // Pull shards for each pointer via hash

        // Iterate through each user in bridgeUsers
          // For each pointer in the pointerMap
            // Find the associated shard by hash
              // Multiply the download count (shard.meta.each(metaObj).meta.download_count)
              //   BY the number of contracts (Object.keys(shard.contracts).length
              //   BY the data size (shard.contracts[0].)

        var totalUserCount = Object.keys(bridgeUsers).length;
        var totalShardCount = 0;
        var totalContractCount = 0;
        var userCounter = 0;
        var shardCounter = 0;
        var contractCounter = 0;

        console.log('Starting find shards and calculate data...');

        Object.keys(bridgeUsers).forEach(function(userID) {
          bridgeUsers[userID].totalDataStored = 0;
          bridgeUsers[userID].totalDownloadCount = 0;
          userCounter++;

          Object.keys(bridgeUsers[userID].pointerMap).forEach(function(pointerId) {
            totalShardCount++;

            var pointer = bridgeUsers[userID].pointerMap[pointerId];

            mongoPuller.pull({
              collection: 'shards',
              method: 'findOne',
              query: { 'hash': pointer.hash }
            }, function(err, shard) {
              shardCounter++;

              totalContractCount += shard.contracts.length;
              var contractMap = {};
              var totalDataDownloaded = 0;
              var totalDownloadCount = 0;

              // Add each of the contracts to a contractMap (key: id)
              shard.contracts.forEach(function(contract) {
                contractMap[contract.nodeID] = contract.contract;
                contractCounter++;
              });

              // Get the download count for each shard
              shard.meta.forEach(function(metaItem) {
                var shardDownloadCount = metaItem.meta.downloadCount;
                var shardDataSize = contractMap[metaItem.nodeID].data_size;
                var shardContractCount = shard.contracts.length;

                var shardContractDownloaded = ( shardDownloadCount * shardDataSize );
                bridgeUsers[userID].totalDataStored += ( shardDataSize * shardContractCount );


                totalDataDownloaded += shardContractDownloaded;
                totalDownloadCount += shardDownloadCount;
              });

              bridgeUsers[userID].totalDataDownloaded += totalDataDownloaded;

              if (totalDataDownloaded > 0) {
                //console.log('shard %s totalDataDownloaded is %s', shard._id, totalDataDownloaded);
              }

              if (totalDownloadCount > 0) {
                bridgeUsers[userID].totalDownloadCount += totalDownloadCount;
              }

              //console.log('Users: [ %s / %s ]  Shards: [ %s / %s ]  Contracts: [ %s / %s ]', userCounter, totalUserCount, shardCounter, totalShardCount, contractCounter, totalContractCount);

              if ((userCounter === totalUserCount) && (shardCounter === totalShardCount) && (contractCounter === totalContractCount)) {
                console.log('Done processing download counts... Calling callback.');
                callback(null, null);
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

      // Clean up the data before saving to file
      Object.keys(data).forEach(function(user) {
        delete data[user].pointers;
      });

      fs.writeFile(userDataFilePath, JSON.stringify(data, null, 2), function(err) {
        if (err) {
          console.log('Error writing file: %s', err);
        }

        if (!err) {
          console.log('User data file written to %s', userDataFilePath);
        }
      });

      console.log('Done with Mongo, trying to close connection...');

      mongoPuller.close(function(err) {
        if (err) {
          console.log('Error closing mongo connection: %s', err);
        }

        console.log('Closed mongo connection');
      });
    }
  });
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
