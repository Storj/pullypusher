'use strict';

const expect = require('chai').expect;
const MongoPuller = require('../app/modules/mongoPuller');
const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const mongoURL = 'mongodb://localhost:27017/test';
const EsPusher = require('../app/modules/esPusher');

var initDB = function(db, callback) {
  console.log('Inserting document for testing');

  db.collection('reports').drop(function(err) {
    if (err) {
      return console.log('Error dropping test collection');
    }

    db.collection('reports').insertOne({
      'type': 'report'
    }, function(err, result) {
      if (err) {
        return console.log('Error inserting test data');
      }

      assert.equal(err, null);
      console.log('document inserted: %s', result);
      callback();
    });
  });
};

describe('PullyPusher', () => {
  before((cb) => {
    // Add some data to mongo for us to work with
    console.log('Setting up mongodb for test');
    MongoClient.connect(mongoURL, function(err, db) {
      if (err) {
        return console.log('Error connecting to mongodb: ', err);
      }

      console.log('Should have a connection for mongodb here');

      assert.equal(null, err);
      initDB(db, function() {
        db.close();
        cb();
      });
    });
  });

  after((cb) => {
    // Clean up local test db after we're done with tests
    //mongo.connection.db.dropDatabase(() => {
    //  esPusher.close(cb);
    //  mongoPuller.close(cb);
    //});
    cb();
  });

  it('should open a connection to MongoDB', (cb) => {
    var mongoPuller = new MongoPuller();
    mongoPuller.open(function(err) {
      expect(err).to.equal(null);
      cb();
    });
  });

  it('should count documents in a colleciton correctly', (cb) => {
    var mongoPuller = new MongoPuller();
    console.log('trying to pull and count test');
    mongoPuller.open(function(err) {
      if (err) {
        cb(err);
      }

      mongoPuller.pull({
        collection: 'reports',
        method: 'count'
      }, function(err, count) {
        console.log('count: %s', count);
        expect(count).to.equal(1);
        cb();
      });
    });
  });
});

describe('esPusher', (cb) => {
  const esPusher = new EsPusher();

  before((cb) => {
    cb();
  });

  after((cb) => {
    cb();
  });

  it('should insert a document successfully', () => {
    var testData = {
      'test': 'data'
    };

    esPusher.push(testData, function(err) {
      expect(err).to.equal(null);
      cb();
    });
  });
});
