'use strict';
/**
 * @module mongo-es-river
 */

const config = require('config');
const async = require('async');

function River() {
  this.batchSize = 20;
  this.count = 0;
  this.finished = false;

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
}

River.prototype.initQueue = function initQueue(callback) {
  var self = this;

  this.queue = async.queue(this.processNextItem, 10);

  // Need to either iterate on the cursor (maybe stream cursor) or
  // get a new cursor starting with the right date item (where we left off)

  this.queue.drain = function() {
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

        //console.log('Queue: ', self.queue);

        console.log('Refilled queue');
      });
    });
  };

  return callback(this.queue);
};

River.prototype.run = function run() {
  var self = this;

  console.log('Starting Mongo to ES River');

  this.getCursor(function(cursor) {
    self.cursor = cursor;

    self.initQueue(function(queue) {
      self.queue = queue;

      console.log('Got cursor');

      self.finished = false;

      console.log('Filling queue for the first time...');
      self.fillQueue(function(err) {
        if (err) {
          console.log('Error filling queue: %s', err);
        }

        console.log('Filled queue for the first time');
      });
    });
  });
};

River.prototype.fillQueue = function fillQueue(callback) {
  var self = this;

  async.until(function() {
    return self.finished;
  }, function(cb) {
    process.nextTick(function() {
      if (!self.cursor.isClosed()) {
        self.cursor.sort({ timestamp: 1 });
        self.cursor.limit(self.batchSize);

        self.cursor.each(function(err, item) {

          if (err) {
            console.log('ERROR looping: %s', err);
            return cb(err);
          }

          if (item === null) {
            self.finished = true;
            return cb();
          }

          if (self.count === self.batchSize) {
            console.log('Last Doc Date: %s', item.timestamp);

            self.lastDocDate = item.timestamp;
          }

          self.pushItem(item, function() {
            console.log('Item Date: %s Count: %s BatchSize: %s',
              item.timestamp,
              self.count,
              self.batchSize
            );

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

River.prototype.pushItem = function pushItem(item, callback) {
  this.queue.push(item, function() {
    callback();
  });

  process.stdout.write('+');
};

River.prototype.processNextItem = function processNextItem(myItem, cb) {
  //console.log('ITEM: ', JSON.stringify(myItem));

  this.processItem(myItem, function(processedItem) {
    this.esPusher.push(processedItem, function(err) {
      if (err) {
        return cb(err);
      }

      process.stdout.write('-');
      return cb();
    });
  });
};

River.prototype.getCursor = function getCursor(callback) {
  this.config = {
    collection: 'reports',
    method: 'getCursor',
    startDate: this.lastDocDate
  };

  console.log('Trying to get cursor from mongoPuller');

  this.mongoPuller.open(function(err) {
    if (err) {
      return console.log('Error opening connection to MongoDB: %s', err);
    }

    this.mongoPuller.pull(this.config, function(err, cursor) {
      if (err) {
        return console.log('Got error while iterating cursor: ', err);
      }

      return callback(cursor);
    });
  });
};

River.prototype.processItem = function processItem(item, callback) {
  var processedItem = item;
  if (processedItem && processedItem._id) {
    delete processedItem._id;
  } else {
    console.log('No item to process');
  }

  return callback(processedItem);
};

var river = new River();
river.run();
