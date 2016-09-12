'use strict';
/**
 * @module pullypusher
 */

const fs = require('fs');
const async = require('async');
const config = require('config');
const path = require('path');

// Should load all modules dynamically
var Es = require('../app/modules/es');
var MongoPuller = require('../app/modules/mongoPuller');
var userDataFilePath = path.join(__dirname, '../data-out/userDataFile.json');
var mongo = require('mongodb');

var es = new Es({
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


function UserData() {
  console.log('Initializing UserData Generator');

  this.bridgeUsers = {};
  this.mongoPuller = new MongoPuller(mongoPullerConfig);
  this.totalUserCount = 0;
  this.totalShardCount = 0;
  this.totalContractCount = 0;
  this.userCounter = 0;
  this.shardCounter = 0;
  this.contractCounter = 0;


}

UserData.prototype.finish = function finish(data) {
  console.log('Running FINISH');

  console.log('Saving userlist to file');

  // Clean up the data before saving to file
  Object.keys(data).forEach(function(user) {
    delete data[user].pointers;
  });

  fs.writeFile(
    userDataFilePath,
    JSON.stringify(data, null, 2),
    function(err) {
      if (err) {
        console.log('Error writing file: %s', err);
      }

      if (!err) {
        console.log('User data file written to %s', userDataFilePath);
      }
    }
  );

  console.log('Done with Mongo, trying to close connection...');

  this.mongoPuller.close(function(err) {
    if (err) {
      console.log('Error closing mongo connection: %s', err);
    }

    console.log('Closed mongo connection');
  });
};

UserData.prototype.processUsers = function processUsers() {
  var self = this;

  self.mongoPuller.open(function(err) {
    if (err) {
      return console.log('Error opening MongoDB connection: %s', err);
    }

    console.log('Conneciton to MongoDB opened successfully...');

    async.series([
      function(callback) {
        self.getUsers(callback);
      },
      function(callback) {
        self.calcStorageUsed(callback);
      },
      function(callback) {
        self.calcTransfer(callback);
      },
      function(callback) {
        self.calcDownloadCounts(callback);
      }
    ], function(err) {
      if (err) {
        return console.log('Got an error returned... sad face...');
      }

      self.finish(self.bridgeUsers);
      console.log('Started finish...');
    });
  });
};

UserData.prototype.calcStorageUsed = function calcStorageUsed(callback) {
  var self = this;

  self.totalUserCount = Object.keys(this.bridgeUsers).length;
  self.totalFrameCount = 0;
  self.totalPointerCount = 0;
  self.userCounter = 0;
  self.frameCounter = 0;
  self.pointerCounter = 0;

  console.log('Calculating storage used for %s users', self.totalUserCount);

  Object.keys(self.bridgeUsers).forEach(function(userID) {
    // Pull all frames for that user so
    // that we can calculate storage space
    // and transfer used for each

    self.bridgeUsers[userID].pointers = [];
    self.userCounter++;

    console.log('[USERS][ %s of %s ] Processing storage used for %s',
      self.userCounter,
      self.totalUserCount,
      userID
    );

    self.mongoPuller.pull({
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

      // For each frame, add the size and save as totalDataStored
      // to bridgeUsers for appropriate user
      framesResult.count(function(err, frameCount) {
        if (err) {
          return console.log('Error getting frame count: %s', err);
        }

        if (!frameCount) {
          process.stdout.write('x');
          return;
        }

        console.log('Adding %s frames to totalFrameCount of %s',
          frameCount,
          self.totalFrameCount
        );

        self.totalFrameCount += frameCount;

        framesResult.each(function(err, frame) {
          if (err) {
            return console.log('Error processing frame for user %s',
              userID
            );
          }

          if (frame === null) {
            // This is end of cursor
            return console.log('End of framesResult cursor for user %s',
              userID
            );
          }

          var pointers = frame.shards;

          self.frameCounter++;
          self.totalPointerCount += frame.shards.length;

          pointers.forEach(function(pointer) {
            self.pointerCounter++;

            self.bridgeUsers[userID].pointers.push(pointer);
          });

          console.log('Shard Counter: [ %s / %s ]  Frame Counter: [ %s / %s ]  User Counter: [ %s / %s ]',
                      self.pointerCounter,
                      self.totalPointerCount,
                      self.frameCounter,
                      self.totalFrameCount,
                      self.userCounter,
                      self.totalUserCount
          );

          var pointersDone = (self.pointerCounter === self.totalPointerCount);
          var framesDone = (self.frameCounter === self.totalFrameCount);
          var usersDone = (self.userCounter === self.totalUserCount);

          if (pointersDone && framesDone && usersDone) {
            console.log('Processed last shard, %s out of %s',
                        self.pointerCounter,
                        self.totalPointerCount
            );

            return callback(null, null);
          }
        });
      });
    });
  });
};

UserData.prototype.getUsers = function getusers(callback) {
  var self = this;

  // Grab all users from the bridge
  this.mongoPuller.pull({
    collection: 'users',
    method: 'find',
    query: {}

  }, function(err, usersResult) {
    if (err) {
      console.log('Error while pulling aggregations: %s', err);
    }

    if (!usersResult) {
      console.log('Found end of cursor, finishing up');
      return callback(err, 'addusers');
    }

    // Get the count of users so we know when we're done processing them
    // (probably a better way, checking for
    // null on cursor result or something)
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

        // Add each user to the bridgeUsers object
        // along with existing desired data
        console.log('[ %s of %s ] Adding user %s, created is %s',
          count,
          userCount,
          user._id,
          user.created.toString()
        );

        self.bridgeUsers[user._id] = {
          email: user._id,
          activated: user.activated,
          created: user.created.toString()
        };
      });
    });
  });
};

UserData.prototype.calcTransfer = function calcTransfer(callback) {
  var self = this;
  // Iterate over each user and its pointers and find the
  // size and download count to create download total
  var totalUserCount = Object.keys(this.bridgeUsers).length;
  var totalPointerCount = 0;
  var userCounter = 0;
  var pointerCounter = 0;

  console.log('Starting transfer calculations...');

  // We have...
  // - pointer ID's for all users frames
  // Need...
  // - hash from the pointer with which we find the shard via hash field
  //
  // Here we should be pulling the pointers for each user and...
  // - Grabbing the size and saving to the bridgeUsers object

  Object.keys(self.bridgeUsers).forEach(function(userID) {
    self.bridgeUsers[userID].totalDataDownloaded = 0;
    self.bridgeUsers[userID].pointerMap = [];
    userCounter++;

    var userPointerCount = self.bridgeUsers[userID].pointers.length;
    totalPointerCount += userPointerCount;

    console.log('Looking through %s pointers for user %s',
                userPointerCount,
                userID
    );

    self.bridgeUsers[userID].pointers.forEach(function(pointerId) {
      var pointerObjectId = new mongo.ObjectID(pointerId);

      self.mongoPuller.pull({
        collection: 'pointers',
        method: 'findOne',
        query: { '_id': pointerObjectId }
      }, function(err, pointer) {
        pointerCounter++;

        if (err) {
          return console.log('Error finding shard: %s', err);
        }

        console.log('Users: [ %s / %s ]  Pointers: [ %s / %s ]',
                    self.userCounter,
                    self.totalUserCount,
                    self.pointerCounter,
                    self.totalPointerCount
        );

        if (!pointer) {
          return console.log('Shard %s for user %s not found',
                             pointerId,
                             userID
          );
        }

        console.log('Found shard %s for user %s', pointerId, userID);

        // Collect sizes and hash for each pointer
        self.bridgeUsers[userID].pointerMap[pointerId] = {
          size: pointer.size,
          hash: pointer.hash
        };

        var pointersDone = ( pointerCounter === totalPointerCount );
        var usersDone = ( userCounter === totalUserCount );

        if ( pointersDone && usersDone ) {
          console.log('Done processing all pointers. Calling callback.');

          return callback(null, null);
        }
      });
    });
  });
};

UserData.prototype.calcDownloadCounts = function calcDownloadCounts(callback) {
  var self = this;

  // Pull shards for each pointer via hash
  // Iterate through each user in bridgeUsers
    // For each pointer in the pointerMap
      // Find the associated shard by hash
        // Multiply the download count
        //   (shard.meta.each(metaObj).meta.download_count)
        // BY the number of contracts (Object.keys(shard.contracts).length
        //   BY the data size (shard.contracts[0].)

  this.totalUserCount = Object.keys(this.bridgeUsers).length;
  this.totalShardCount = 0;
  this.totalContractCount = 0;
  this.userCounter = 0;
  this.shardCounter = 0;
  this.contractCounter = 0;

  console.log('Starting find shards and calculate data...');

  Object.keys(this.bridgeUsers).forEach(function(userID) {
    self.bridgeUsers[userID].totalDataStored = 0;
    self.bridgeUsers[userID].totalDownloadCount = 0;
    self.userCounter++;

    var pointerMap = self.bridgeUsers[userID].pointerMap;

    Object.keys(pointerMap).forEach(function(pointerId) {
      self.totalShardCount++;

      var pointer = self.bridgeUsers[userID].pointerMap[pointerId];

      self.mongoPuller.pull({
        collection: 'shards',
        method: 'findOne',
        query: { 'hash': pointer.hash }
      }, function(err, shard) {
        if (err) {
          return console.log('Error returned while pulling');
        }

        self.shardCounter++;

        self.totalContractCount += shard.contracts.length;
        var totalDataDownloaded = 0;
        var totalDownloadCount = 0;

        self.processShard(shard, userID, function(dataStored, dataDownload, downloadCount) {

          totalDataDownloaded += dataDownload;
          totalDownloadCount += downloadCount;

          self.bridgeUsers[userID].totalDataDownloaded += totalDataDownloaded;
          self.bridgeUsers[userID].totalDataStored += dataStored;

          if (totalDownloadCount > 0) {
            self.bridgeUsers[userID].totalDownloadCount += totalDownloadCount;
          }

          var usersDone = ( self.userCounter === self.totalUserCount );
          var shardsDone = ( self.shardCounter === self.totalShardCount );
          var contractsDone = ( self.contractCounter === self.totalContractCount );

          if (usersDone && shardsDone && contractsDone) {
            console.log('Done processing download counts...');
            callback(null, null);
          }
        });
      });
    });
  });
};

UserData.prototype.processShard = function processShard(
  shard,
  userID,
  callback
) {
  var self = this;
  // Add each of the contracts to a contractMap (key: id)
  var contractMap = {};
  var dataStored = 0;
  var downloadCount = 0;
  var dataDownload = 0;

  shard.contracts.forEach(function(contract) {
    contractMap[contract.nodeID] = contract.contract;
    self.contractCounter++;
  });

  // Get the download count for each shard
  shard.meta.forEach(function(metaItem) {
    var dataSize = contractMap[metaItem.nodeID].data_size;
    var contractCount = shard.contracts.length;

    downloadCount = metaItem.meta.downloadCount;
    dataStored = ( dataSize * contractCount );
    dataDownload = ( downloadCount * dataSize );
  });

  callback(dataStored, dataDownload, downloadCount);
};

process.on('SIGINT', function() {
  exitGracefully();
});

process.on('SIGTERM', function() {
  exitGracefully();
});

const exitGracefully = function exitGracefully() {
  console.log('Exiting...');

  mongoPuller.close(function() {
    console.log('[INDEX] Closed Mongo connection');

    es.close(function() {
      console.log('[INDEX] Closed ES connection');

      process.exit();
    });
  });
};

var start = function start() {
  var userData = new UserData();

  console.log('Beginning user processing.');

  userData.processUsers();
};

start();
