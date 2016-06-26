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

  this.mongoConfig = {
    collection: 'reports',
    method: 'getAllFromCollection'
  };

  self.batchSize = 3;
  self.count = 0;

  this.run = function run() {
    console.log("Starting Mongo to ES River");

    this.getMongoCursor(this.mongoConfig, function(cursor) {
      self.cursor = cursor;
      console.log('Got mongo cursor');
      console.log('Starting queue function');

      self.queue = async.queue(self.processNextItem, 10);

      console.log('Cursor is closed -%s-', self.cursor.isClosed());

      self.queue.drain = function() {
        console.log('Queue is drained');
        process.stdout.write('X');
        console.log('Filling queue again...');
        this.fillQueue(self.cursor, self.queue);
      };

      console.log('Filling queue for the first time...');

      async.until(function() {
        return self.cursor.isClosed();
      }, function(cb) {
        var cb = cb;
        process.nextTick(function() {
          self.fillQueue(function() {
            cb();
          });
        });
      }, function(err){
        console.log('until is done');
      });
    });
  };

  this.fillQueue = function fillQueue(callback) {
    if (self.count < self.batchSize) {
      console.log('Count ', self.count);

      self.pushNext(function() {
        self.count++;
       return callback();
      });
    } else {
      return callback();
    }
  };

  this.pushNext = function pushNext(callback) {
    console.log('Getting next item from cursor');

    self.cursor.nextObject(function(err, item) {
      console.log('Pushing next item...');

      if (err) {
        console.log('Error getting next object: %s', err);
      }

      self.queue.push(item);
      process.stdout.write('+');
      callback();
    });
  };

  this.processNextItem = function processNextItem(myItem, cb) {
    console.log('Processing next item...');

    self.processItem(myItem, function(processedItem) {
      console.log('Got processed item returned...');
      self.esPusher.push(processedItem, function(err, response) {
        if (err) {
          return cb(err);
        }

        process.stdout.write('-');
        return cb();
      });
    });
  };

  this.getMongoCursor = function getMongoCursor(config, callback) {
    console.log('Trying to get cursor from mongoPuller');

    this.mongoPuller.pull(config, function(err, cursor) {
      console.log('Got cursor from mongoPuller');

      cursor.count(function(err, count) {
        console.log('Cursor.count: ', count);
      });

      if (err) {
        return console.log('Got error while iterating cursor: ', err);
      }

      return callback(cursor);

    });
  };

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

