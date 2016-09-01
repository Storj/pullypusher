'use strict'

var mongo = require('mongodb');
var config = require('config');
var merge = require('merge');

function MongoPuller(options) {
  if (!(this instanceof MongoPuller)) {
    return new MongoPuller(options);
  }

  if (!options) {
    options = {};
  }

  options = merge(config.mongodb, options);

  this._init(options);
}


MongoPuller.prototype._init = function _init(options) {
  this.host = options.host;
  this.dbName = options.dbName;
  this.port = options.port;
  this.ssl = options.ssl;
  this.sslValidate = options.sslValidate;
  this.user = options.user;
  this.pass = options.pass;

  this.server = new mongo.Server(this.host, this.port, {
    ssl: this.ssl,
    sslValidate: this.sslValidate,
  });

  this.db = new mongo.Db(this.dbName, this.server, { w: 1 });
};

MongoPuller.prototype.open = function open(callback) {
  console.log('Opening connection to DB');
  var self = this;

  self.db.open(function(err, db) {
    if (err) {
      console.log('Error while connectiong to DB', err);
      return callback(err);
    }
    // If we need to authenticate, do so now
    if (self.user && self.pass) {
      console.log('Attempting to auth to MongoDB - User: %s', self.user);

      self.db.authenticate(self.user, self.pass, function(err, authenticatedDb) {
        if (err) {
          console.log('Error while autehnticating to DB', err);
          return callback(err);
        }

        console.log('authenticated: ', authenticatedDb);

        callback();
      });
    } else {
      console.log('No auth provided');
      return callback(err);
    }
  });
};


MongoPuller.prototype.pull = function pull(options, callback) {
  var collectionName = options.collection;
  var method = options.method;
  var query = options.query;
  var startDate = options.startDate;
  var auth = { user: this.user, pass: this.pass };
  var collection = this.db.collection(collectionName);

  if (method == 'count') {
    if (query) {
      collection.count(query, function(err, count) {
        if (err) {
          return callback(err, null);
        }

        console.log("[MONGODB] Count with query done...");
        return callback(null, count);
      });
    } else {
      collection.count(function(err, count) {
        if (err) {
          return callback(err, null);
        }

        console.log("[MONGODB] Count done...");
        return callback(null, count);
      });
    }
  }

  if (method == 'find') {
    collection.find(query, function(err, resultCursor) {
      if (err) {
        console.log('Error running FIND: %s', err);
      }

      return callback(err, resultCursor);
    });
  }

  if (method == 'findOne') {
    collection.findOne(query, function(err, result) {
      if (err) {
        console.log('Error running FIND: %s', err);
      }

      return callback(err, result);
    });
  }

  if (method == 'getCursor') {
    var cursorQuery = {};

    if (startDate) {
      console.log('Finding with start date');
      // Should be passing this in by var...
      cursorQuery = { 'created': { $gt: startDate } };
    } else {
      console.log('Finding without start date');
    }

    collection.find(cursorQuery, function(err, cursor) {
      //console.log("[MONGODB] Find query done...");

      return callback(err, cursor);
    });
  }

  if (method == 'aggregate') {
    collection.aggregate(query).toArray(function(err, result) {
      console.log("[MONGODB] Aggregate done...");

      return callback(err, result);
    });
  }
};

MongoPuller.prototype.close = function close(callback) {
  this.db.close(function(err, result) {
    if (err) {
      console.log("Error occurred while closing mongo connection: " + err);
    }
  });
  return callback();
};

module.exports = MongoPuller;
