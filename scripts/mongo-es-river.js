'use strict';
/**
 * @module mongo-es-river
 */

const config = require('config');
const async = require('async');

function River() {
  var self = this;
  var cursor;
  var lastDocDate;
  self.queue;
  self.batchSize = 20;
  self.count = 0;
  self.finished = false;

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

  this.initQueue = function initQueue(callback) {
    var queue = async.queue(self.processNextItem, 10);
    self.queue = this.queue;

    queue.drain = function() {
      process.stdout.write('X');
      self.count = 0;
      self.finished = false;
      self.getCursor(function(cursor) {
        self.cursor = cursor;
        self.fillQueue(function(err) {
          if (err) {
            console.log('ERROR Refilling queue: ', err);
            return callback(null);
          }

          // Why is queue here empty???
          //console.log('Queue: ', self.queue);

          console.log('Refilled queue');
        });
      });
    };

    return callback(queue);
  };

  this.run = function run() {
    console.log("Starting Mongo to ES River");
    console.log('Got mongo cursor');
    console.log('Starting queue function');
    console.log('Filling queue for the first time...');

    self.getCursor(function(cursor) {
      self.cursor = cursor;

      self.initQueue(function(queue) {
        self.queue = queue;

        console.log('Got cursor');

        self.finished = false;

        self.fillQueue(function(err) {
          if (err) {
            console.log('Error filling queue: %s', err);
          }

          console.log('Filled queue for the first time');
        });
      });
    });
  };

  this.fillQueue = function fillQueue(callback) {
    console.log('Running async to fill queue');
    async.until(function() {
      console.log('self.finished is ', self.finished);

      return self.finished;
    }, function(cb) {
      console.log('before next tick');

      process.nextTick(function() {
        console.log('Checking if cursor is closed: ', self.cursor.isClosed(), ' self.finished(): ', self.finished);
        if (!self.cursor.isClosed()) {
          console.log('Filling queue');
          self.cursor.sort({ timestamp: 1 });
          self.cursor.limit(self.batchSize);

          self.cursor.each(function(err, item) {
            //console.log('Item: %s', JSON.stringify(item));

            if (err) {
              console.log('ERROR looping: %s', err);
              return cb(err);
            }

            if (item === null) {
              console.log('Setting finished to true');
              self.finished = true;
              return cb();
            }

            self.pushItem(item, function() {
              //console.log('Count: %s Batch Size: %s', self.count, self.batchSize);
              self.count++;
            });
          });
        }
      });
    }, function(err) {
      console.log('Finished');
      callback(err);
    });
  };

  this.pushItem = function pushItem(item, callback) {
    //console.log('item: ', item);

    self.queue.push(item, function() {
      callback();
    });

    process.stdout.write('+');
  };

  this.processNextItem = function processNextItem(myItem, cb) {
    //console.log('ITEM: ', JSON.stringify(myItem));
    console.log('Item Date: %s', myItem.timestamp);

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

  this.getCursor = function getCursor(callback) {
    this.config = {
      collection: 'reports',
      method: 'getCursor',
    };

    console.log('Trying to get cursor from mongoPuller');

    this.mongoPuller.pull(this.config, function(err, cursor) {
      if (err) {
        return console.log('Got error while iterating cursor: ', err);
      }

      return callback(cursor);
    });
  };

  this.processItem = function processItem(item, callback) {
    var processedItem = item;
    if (processedItem && processedItem._id) {
      delete processedItem._id;
    } else {
      console.log('No item to process');
    }

    return callback(processedItem);
  };
}

var river = new River();
river.run();
