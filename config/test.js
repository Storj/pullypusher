// DEFAULTS
// override this in `${environment}.js`

module.exports = {
  log: {
    timestamp: true,
    level: 'debug'
  },
  mongodb: {
    host: process.env.MONGO_HOST || 'localhost',
    db_name: process.env.MONGO_DB_NAME || 'pullypusher',
    port: process.env.MONGO_PORT || 3000,
    user: process.env.MONGO_USER,
    pass: process.env.MONGO_PASS,
    ssl: process.env.MONGO_SSL || true,
    ssl_validate: process.env.MONGO_SSL_VALIDATE || true
  },
  elasticsearch: {
    host: process.env.ES_HOST || 'localhost',
    port: process.env.ES_PORT || 9200,
    ssl: process.env.ES_SSL || false,
    user: process.env.ES_USER || null,
    pass: process.env.ES_PASS || null,
    index: process.env.ES_INDEX || 'pullypusher',
    type: process.env.ES_TYPE || 'stat',
    log: process.env.ES_LOG || 'info'
  },
  http: {
    host: process.env.HTTP_HOST || 'localhost',
    path: process.env.HTTP_PATH || '/'
  }
};
