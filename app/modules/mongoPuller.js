function MongoPuller(data) {
  var mongo = require('mongodb');
  var host = data.host || '127.0.0.1';
  var dbName = data.dbName;
  var port = data.port || 27017;
  var ssl = data.ssl || false;
  var sslValidate = data.sslValidate || false;
  var user = data.user;
  var pass = data.pass;

  this.server = new mongo.Server(host, port, {
    ssl: ssl,
    sslValidate: sslValidate
  });

  this.db = new mongo.Db(dbName, this.server, { w: 1 });

  var self = this;

  this.pull = function pull(data, callback) {
    var collectionName = data.collection;
    var method = data.method;
    var query = data.query;

    console.log("Pulling MongoDB data from collection " + collectionName);

    // Pull this from config (which should be from ENV)
    var auth = { user: user, pass: pass };

    this.db.open(function(err, db) {
      if (err) throw err;
      console.log("MongoDB Connection opened");

      var collection = db.collection(collectionName);

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

      if (method == 'aggregate') {
        collection.aggregate(query).toArray(function(err, result) {
          if (err) {
            return callback(err, null);
          }

          console.log("[MONGODB] Aggregate done...");
          return callback(null, result);
        });
      };
    });
  };

  this.close = function close(callback) {
    self.db.close(function(err, result) {
      if (err) {
        console.log("Error occurred while closing mongo connection: " + err);
      }

      console.log("Error is: " + err);
      console.log("Close mongo result: " + result);
    });
    return callback();
  };
};

module.exports = MongoPuller;
