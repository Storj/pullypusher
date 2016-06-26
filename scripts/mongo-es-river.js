'use strict';
/**
 * @module mongo-es-river
 */

const config = require('config');
const async = require('async');

function River() {
  var self = this;
  var cursor;
  var queue;
  var lastDocDate;
  var batchSize = 20;
  var count = 0;
  var finished = false;

  var EsPusher = require('../app/modules/esPusher');
  this.esPusher = new EsPusher({
    host: config.elasticsearch.HOST,
    port: config.elasticsearch.PORT,
    ssl: config.elasticsearch.SSL,
    log: config.elasticsearch.LOG,
    index: config.elasticsearch.INDEX,
    type: config.elasticsearch.TYPE,
    user: config.elasticsearch.USER,
    pass: config.elasticsearch.PASS
  });

  var MongoPuller = require('../app/modules/mongoPuller');
  this.mongoPuller = new MongoPuller({
    host: config.mongodb.HOST,
    dbName: config.mongodb.DB_NAME,
    port: config.mongodb.PORT,
    ssl: config.mongodb.SSL,
    sslValidate: config.mongodb.SSL_VALIDATE,
    user: config.mongodb.USER,
    pass: config.mongodb.PASS
  });


  this.run = function run() {
    console.log("Starting Mongo to ES River");

    self.cursor = cursor;
    console.log('Got mongo cursor');
    console.log('Starting queue function');

    self.queue = async.queue(self.processNextItem, 10);

    self.queue.drain = function() {
      process.stdout.write('X');
      self.count = 0;
    };

    console.log('Filling queue for the first time...');

    async.until(function() {
      // Until mongo stops returning documents
      return finished;
    }, function(cb) {
      process.nextTick(function() {
        self.getNextBatch(function(batchArray) {
          self.fillQueue(batchArray, function() {
            return cb();
          });
        });
      });
    }, function(err){
    });
  };

  this.fillQueue = function fillQueue(batchArray, callback) {
    async.until(function() {
      return (self.count === self.batchSize);
    }, function(cb) {
      async.each(batchArray, function(item) {
        self.pushItem(function(item) {
          self.count++;
          console.log('Count: %s Batch Size: %s', self.count, self.batchSize);
          cb();
        });
      });
    }, function(err) {
      callback();
    });
  };

  this.pushItem = function pushItem(item, callback) {
    self.queue.push(item, function() {
      callback();
    });
    process.stdout.write('+');
  };

  this.processNextItem = function processNextItem(myItem, cb) {
    self.processItem(myItem, function(processedItem) {
      self.esPusher.push(processedItem, function(err, response) {
        if (err) {
          return cb(err);
        }

        process.stdout.write('-');
        return cb();
      });
    });
  };

  this.getNextBatch = function getNextBatch(callback) {
    // Query: If no lastDocDate, sort the docs and start at the beginning and limit to 100
    var query;

    /*
    if (!lastDocDate) {
      query = [ [{}], [{ "sort": "timestamp", "limit": 100 } ];
    } else {
      query = [ { 'timestamp': { $gt: lastDocDate } }, { "sort": "timestamp", "limit": 100 } ];
    }
    */

    // Query If lastDocDate, find all documents newer than that and limit to 100
    this.config = {
      collection: 'reports',
      method: 'getCursor',
    };

    console.log('Trying to get cursor from mongoPuller');

    this.mongoPuller.pull(this.config, function(err, cursor) {
      console.log('Got result array length: ', resultArray.length);
      if (err) {
        return console.log('Got error while iterating cursor: ', err);
      }

      cursor.sort({'timestamp': 1});

      if (resultArray.length > 0) {
        // Set lastDocDate from last doc in array

        return callback(resultArray);
      } else {
        self.finished = true;
        return callback(null);
      }
    });
  };

  /*
  this.getNextMongoItem = function getNextMongoItem(cursor, callback) {
    console.log('Getting next mongo item from cursor');
    cursor.nextObject(function(err, item) {
      if (err) {
        return console.log('Error pulling from MongoDB: %s', err);
      }

      if (!item) {
        return console.log('No item found...');
      }

      return callback(item);
    });
  };
  */

  this.processItem = function processItem(item, callback) {
    var processedItem = item;
    if (processedItem._id) {
      delete processedItem._id;
    }

    return callback(processedItem);
  };
}

var river = new River();
river.run();

