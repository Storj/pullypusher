'use strict';
/**
 * @module pullypusher
 */

const async = require('async');
const config = require('config');
const CronJob = require('cron').CronJob;

// Should load all modules dynamically
// var httpRequest = require('../app/modules/httpRequest');
var Es = require('../app/modules/es');

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

var pull = function pull() {
  var uploadResults = {
    total: 0,
    successPercent: null,
    totalSuccess: 0,
    totalFail: 0,
    totalTimeout: 0
  };

  var downloadResults = {
    total: 0,
    successPercent: null,
    totalSuccess: 0,
    totalFail: 0,
    totalTimeout: 0
  };

  var body = {
    size: 1000,
    query: {
      bool: {
        must: [
          { range: { 'timestamp': { 'gte': 'now-1h/d' } } },
          { match: { type: 'cli' } }
        ]
      }
    }
  };

  var options = {
    index: 'storj-*',
    type: 'stats',
    body: body
  };

  es.pull(options, function(err, response) {
    if (err) {
      return console.log('Error pulling data from ES', err);
    }

    console.log('Got %s results form ES', response.hits.hits.length);

    // loop through results and tally succcesses and failures
    response.hits.hits.forEach(function(entry) {
      if (entry._source.name === 'download') {
        downloadResults.total++;
        if (entry._source.success === '1') downloadResults.totalSuccess++;
        if (entry._source.timeout === '1') downloadResults.totalTimeout++;
        if (entry._source.success === '0') downloadResults.totalFail++;
      }

      if (entry._source.name === 'upload') {
        uploadResults.total++;
        if (entry._source.success === '1') uploadResults.totalSuccess++;
        if (entry._source.timeout === '1') uploadResults.totalTimeout++;
        if (entry._source.success === '0') uploadResults.totalFail++;
      }
    });

    // calculate percentage
    var uploadSuccessPercent = (( uploadResults.totalSuccess / uploadResults.total ) * 100 );
    var downloadSuccessPercent = (( downloadResults.totalSuccess / downloadResults.total ) * 100 );

    uploadResults.successPercent = Math.floor(uploadSuccessPercent);
    downloadResults.successPercent = Math.floor(downloadSuccessPercent);

    console.log('Upload results: ', uploadResults);
    console.log('Download results: ', downloadResults);

    uploadResults.name = 'upload_computed';
    uploadResults.type = 'cli';

    downloadResults.name = 'download_computed';
    downloadResults.type = 'cli';

    // Pass the percentage result to the push method
    es.push(uploadResults, function(err, response) {
      if (err) {
        return console.log('Error pushing upload results to ES: %s', err);
      }

      console.log('Pushed computed upload results to ES');
      console.log('ES Response: ', response);
    });

    es.push(downloadResults, function(err, response) {
      if (err) {
        return console.log('Error pushing download results to ES: %s', err);
      }

      console.log('pushed computed download results to ES');
      console.log('ES Response: ', response);
    });
  });
};

pull();

var start = function start() {
  var myCron = new CronJob('*/10 * * * *', function() {
    console.log('[CRON] Running pullFromMongo()');
    pull();
  }, function() {
    console.log('[CRON] Done running pullFromMongo()');
  }, true);
  myCron.start();
};

const exitGracefully = function exitGracefully() {
  console.log('Exiting...');

  es.close(function() {
    console.log('[INDEX] Closed ES connection');

    process.exit();
  });
};

start();

process.on('SIGINT', function() {
  exitGracefully();
});

process.on('SIGTERM', function() {
  exitGracefully();
});
