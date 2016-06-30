'use strict';
/**
 * @module mongo-es-river
 */

const config = require('config');
const async = require('async');

function River() {
  var self = this;
  var cursor;
  this.lastDocDate;
  self.queue;
  self.batchSize = 20;
  //this.count = 0;
  self.finished = false;
  this.userCount = 0;

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
    var self = this;
    self.queue = async.queue(self.processNextItem, self.batchSize);

    self.queue.drain = function() {
      process.stdout.write('X');
      //this.count = 0;
      self.finished = false;
      self.getCursor(function(cursor) {
        self.cursor = cursor;
        self.fillQueue(self.queue, function(err) {
          if (err) {
            console.log('ERROR Refilling queue: ', err);
            return callback(null);
          }

          console.log('Refilled queue');
        });
      });
    };

    return callback(self.queue);
  };

  this.run = function run() {
    var self = this;
    console.log("Starting Mongo to ES River");
    console.log('Filling queue for the first time...');

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
  };

  this.fillQueue = function fillQueue(queue, callback) {
    var self = this;
    self.queue = queue;

    async.until(function() {
      // Iterate through each user¬
      // Create a counter for time period (day) (this should be an object with months, days and counters)¬
      // Increment counter for every user for the appropriate day¬
      // When finished iterating over all users kick off task to build daily ES entries from the object¬

      return self.finished;
    }, function(cb) {
      process.nextTick(function() {
        //console.log('Checking if cursor is closed: ', self.cursor.isClosed(), ' self.finished(): ', self.finished);
        if (!self.cursor.isClosed()) {
          self.count = 0;
          console.log('Filling queue');
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
                cursor.close();
              }
              return cb();
            }

            console.log('Processed: %s BatchSize: %s userCount: %s', self.count, self.batchSize, self.userCount);

            self.lastDocDate = item.created;

            self.pushItem(item, function() {
              //console.log('Count: %s Batch Size: %s', self.count, self.batchSize);

              // **************************
              // Count should be +1 to detect last item for date!!!
              // **************************
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
    //console.log('Pushing Item to queue: ', item);

    self.queue.push(item, function() {
      callback();
    });

    process.stdout.write('+');
  };

  this.processNextItem = function processNextItem(myItem, cb) {
    //console.log('ITEM: ', JSON.stringify(myItem));

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
      collection: 'users',
      method: 'getCursor',
      startDate: this.lastDocDate
    };

    console.log('Trying to get cursor from mongoPuller');

    /*
     * This causes a memory leak here! Need to clean these up after they're done
     * either in here or in mongoPuller
     */
    this.mongoPuller.pull(this.config, function(err, cursor) {
      if (err) {
        return console.log('Got error while iterating cursor: ', err);
      }

      return callback(cursor);
    });
  };

  this.processItem = function processItem(item, callback) {
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

    console.log('Processed Item ID: ', processedItem.email);

    return callback(processedItem);
  };
}

var river = new River();
river.run();
