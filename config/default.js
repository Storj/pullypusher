// DEFAULTS
// override this in `${environment}.js`

module.exports = {
  log: {
    timestamp: true,
    level: 'debug'
  },
  mongodb: {
    HOST: process.env.MONGO_HOST || 'mongodb://localhost:27017/pullypusher',
    PORT: process.env.MONGO_PORT || 3000,
    USER: process.env.MONGO_USER || 'dbuser',
    PASS: process.env.MONGO_PASS || 'pass',
    SSL: process.env.MONGO_SSL || true,
    SSL_VALIDATE: process.env.MONGO_SSL_VALIDATE || true
  },
  elasticsearch: {
    HOST: process.env.ES_HOST || 'localhost',
    PORT: process.env.ES_PORT || 9200,
    SSL: process.env.ES_SSL || false,
    USER: process.env.ES_USER || null,
    PASS: process.env.ES_PASS || null,
    INDEX: process.env.ES_INDEX || 'pullypusher',
    TYPE: process.env.ES_TYPE || 'stat',
    LOG: process.env.ES_LOG || 'info'
  },
  http: {
    HOST: process.env.HTTP_HOST || 'localhost',
    PATH: process.env.HTTP_PATH || '/'
  }
};
