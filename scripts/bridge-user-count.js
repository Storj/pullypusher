'use strict';
/**
 * @module mongo-es-river
 */

const config = require('config');
const async = require('async');

function River() {
  var MongoPuller = require('../app/modules/mongoPuller');
  var Es = require('../app/modules/esPusher');

  this.userCount = 0;

  this.batchSize = 20;
  this.finished = false;

  this.es = new Es({
    host: config.elasticsearch.host,
    port: config.elasticsearch.port,
    ssl: config.elasticsearch.ssl,
    log: config.elasticsearch.log,
    index: config.elasticsearch.index,
    type: config.elasticsearch.type,
    user: config.elasticsearch.user,
    pass: config.elasticsearch.pass
  });

  this.mongoPuller = new MongoPuller({
    host: config.mongodb.host,
    dbName: config.mongodb.db_name,
    port: config.mongodb.port,
    ssl: config.mongodb.ssl,
    sslValidate: config.mongodb.ssl_validate,
    user: config.mongodb.user,
    pass: config.mongodb.pass
  });
}

River.prototype.finish = function finish() {
  this.mongoPuller.close(function(err) {
    if (err) {
      return console.log('Error closing MongoDB connection: %s', err);
    }

    return console.log('Closed MongoDB connection. Done!');
  });
};

River.prototype.initQueue = function initQueue(callback) {
    var self = this;
    var queue = async.queue(this.processNextItem.bind(self), self.batchSize);

    queue.drain = function() {
      process.stdout.write('X');
      self.finished = false;
      self.getCursor(function(cursor) {
        self.cursor = cursor;

        self.fillQueue(queue, function(err) {
          if (err) {
            console.log('ERROR Refilling queue: ', err);
            return callback(null);
          }

          //console.log('Refilled queue');
        });
      });
    };

    return callback(queue);
};

River.prototype.run = function run() {
    var self = this;
    console.log('Starting Mongo to ES River');
    console.log('Filling queue for the first time...');

    self.mongoPuller.open(function(err) {
      if (err) {
        return console.log('Error opening connection to MongoDB: %s', err);
      }

      self.getCursor(function(cursor) {
        self.cursor = cursor;

        self.initQueue(function(queue) {
          self.queue = queue;

          console.log('Got cursor');

          self.finished = false;

          self.fillQueue(self.queue, function(err) {
            if (err) {
              console.log('Error filling queue: %s', err);
            }

            console.log('Filled queue for the first time');
          });
        });
      });
    });
};

River.prototype.fillQueue = function fillQueue(queue, callback) {
    var self = this;
    self.queue = queue;

    async.until(function() {
      // Iterate through each user¬
      // Create a counter for time period (day)
      //   (this should be an object with months, days and counters)¬
      // Increment counter for every user for the appropriate day¬
      // When finished iterating over all users kick off
      //   task to build daily ES entries from the object¬

      return self.finished;
    }, function(cb) {
      process.nextTick(function() {
        if (!self.cursor.isClosed()) {
          self.count = 0;
          //console.log('Filling queue');
          self.cursor.sort({ created: 1 });
          self.cursor.limit(self.batchSize);

          self.cursor.each(function(err, item) {
            //console.log('Item: %s', JSON.stringify(item));
            self.count++;
            self.userCount++;

            if (err) { console.log('ERROR looping: %s', err);
              return cb(err);
            }

            if (item === null || self.count === self.batchSize) {
              console.log('Last Doc Date: %s', self.lastDocDate);
              self.finished = true;
              if ( self.count === 0 ) {
                self.cursor.close();
              }
              return cb();
            }

            /*
            console.log(
              'Processed: %s BatchSize: %s userCount: %s',
              self.count,
              self.batchSize,
              self.userCount
            );
            */

            self.lastDocDate = item.created;

            self.pushItem(item, function(err) {
              if (err) {
                return console.log('Error processing item!');
              }

              //console.log('Count: %s Batch Size: %s',
              //self.count,
              //self.batchSize);

              // **************************
              // Count should be +1 to detect last item for date!!!
              // **************************
            });
          });
        }
      });
    }, function(err) {
      //console.log('Finished processing items in queue');
      callback(err);
    });
};

River.prototype.pushItem = function pushItem(item, callback) {
    //console.log('Pushing Item to queue: ', item);

    this.queue.push(item, function() {
      callback();
    });

    //process.stdout.write('+');
};

River.prototype.processItem = function processItem(item, callback) {
    var self = this;
    var processedItem = item;

    // Move the email address from the _id field to email field
    processedItem.email = item._id;
    // Duplicate the created field into the timestamp field for ES indexing
    processedItem['@timestamp'] = item.created;
    processedItem.timestamp = item.created;
    processedItem.userCount = self.userCount;

    if (processedItem && processedItem._id) {
      delete processedItem._id;
      delete processedItem.hashpass;
    } else {
      console.log('No item to process');
    }

    //console.log('Processed Item ID: ', processedItem.email);

    return callback(processedItem);
};

River.prototype.processNextItem = function processNextItem(myItem, cb) {
  var self = this;

  this.processItem(myItem, function(processedItem) {
    self.es.push(processedItem, function(err) {
      if (err) {
        return cb(err);
      }

      //process.stdout.write('-');
      return cb();
    });
  });
};

River.prototype.getCursor = function getCursor(callback) {
  var self = this;
  this.config = {
    collection: 'users',
    method: 'getCursor',
    startDate: this.lastDocDate
  };

  //console.log('Trying to get cursor from mongoPuller');

  self.mongoPuller.pull(self.config, function(err, cursor) {
    if (err) {
      return console.log('Got error while iterating cursor: ', err);
    }

    cursor.count(function(err, count) {
      if (count === 0) {
        console.log('Cursor count is 0, that means were done here');
        return self.finish();
      }

      console.log('cursor count: %s', count);

      return callback(cursor);
    });

  });
};

function start() {
  var river = new River();
  river.run();
}

start();

module.exports = River;

