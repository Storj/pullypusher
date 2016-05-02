'use strict';

const server = require('../server');
const request = require('supertest');
const expect = require('chai').expect;
const uuid = require('node-uuid');
const config = require('config');
const MongoPuller = require('../app/modules/mongoPuller');
const mongoPuller = new MongoPuller();
const EsPusher = require('../app/modules/esPusher');
const esPusher = new EsPusher();

describe('PullyPusher', () => {
  before((cb) => {
  }

  after((cb) => {
    mongo.connection.db.dropDatabase(() => {
      esPusher.close(cb);
      mongoPuller.close(cb);
    });
  });

  describe('Successful Read from MongoDB', () => {
    const queryData

