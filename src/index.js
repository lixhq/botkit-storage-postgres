const pg = require('pg');
const co = require('co');
var url = require('url');

module.exports = function (config) {
  if (!config) {
    throw new Error('No config supplied!');
  }

  if(config.postgresUri) {
    var pg_data = url.parse(config.postgresUri,true);
    if(['postgres:','postgresql:'].indexOf(pg_data.protocol) == -1)
      throw new Error('Not a postgres url');
    var auth = pg_data.auth.split(':');
    config.user = auth[0];
    config.password = auth[1] || '';
    config.port = pg_data.port;
    config.host = pg_data.hostname;
    config.database = pg_data.pathname.slice(1);
    Object.assign(config, pg_data.query);
  }

  config = {
    user: config.user || process.env.BOTKIT_STORAGE_POSTGRES_USER || 'botkit',
    database: config.database || process.env.BOTKIT_STORAGE_POSTGRES_DATABASE || 'botkit',
    password: config.password || process.env.BOTKIT_STORAGE_POSTGRES_PASSWORD || 'botkit',
    host: config.host || process.env.BOTKIT_STORAGE_POSTGRES_HOST || 'localhost',
    port: config.port || process.env.BOTKIT_STORAGE_POSTGRES_PORT || '5432',
    max: config.maxClients || process.env.BOTKIT_STORAGE_POSTGRES_MAX_CLIENTS || '10',
    idleTimeoutMillis: config.idleTimeoutMillis || process.env.BOTKIT_STORAGE_POSTGRES_IDLE_TIMEOUT_MILLIS || '30000',
  };

  const promisedPool = co(function *() {
    const q = (client, qstr) => new Promise((resolve,reject) => client.query(qstr, [], (err, res) => err ? reject(err) : resolve(res)))
      .catch(err => {throw new Error(`Could not execute '${qstr}'. Error: ${err.stack || err}`)});
    const connect = (client) => new Promise((resolve,reject) => client.connect((err, done) => err ? reject(err) : resolve()))

    // Some providers don't allow access to template1.
    // Instead of failing hard, raise an error on the console, and attempt to connect to the database anyways.
    try {
      const noDbClient = new pg.Client(Object.assign({}, config, {database: 'template1'}));
      yield connect(noDbClient);
      const dbexistsQuery = yield q(noDbClient, `SELECT 1 from pg_database WHERE datname='${config.database}'`);

      if(dbexistsQuery.rows.length === 0) {
        console.log('botkit-storage-postgres> creating db ' + config.database);
        yield q(noDbClient, 'CREATE DATABASE ' + config.database);
      }

      noDbClient.end();
    } catch(err) {
      console.error("Could not create database. Error: ", err.stack);
    }

    const dbClient = new pg.Client(config);
    yield connect(dbClient);

    yield ['botkit_teams', 'botkit_users', 'botkit_channels'].map(tableName =>
      q(dbClient, `CREATE TABLE IF NOT EXISTS ${tableName} (
        id char(50) NOT NULL PRIMARY KEY,
        json TEXT NOT NULL
      )`))

    dbClient.end();

    const pool = new pg.Pool(config);

    pool.on('error', function (err, client) {
      console.error('botkit-storage-postgres> idle client error', err.message, err.stack);
    });
    return pool;
  });

  promisedPool.then(() => {
    console.log(`botkit-storage-postgres> connected to ${config.host}:${config.port}/${config.database}`);
  }, (err) => {
    console.error('botkit-storage-postgres> error running setup. Error: ' + err.stack);
  });

  const dbexec = co.wrap(function *(func) {
    const pool = yield promisedPool;
    const {client,done} = yield new Promise((resolve,reject) => pool.connect((err, client, done) =>
      err ? reject(err) : resolve({client,done})))
      .catch(err => {throw new Error(`Could not execute '${qstr}'. Error: ${err.stack || err}`)});;

    _pool = new pg.Pool(config);

    const query = (...args) => new Promise((resolve,reject) =>
      client.query(...args, (err, res) => err ? reject(err) : resolve(res)));

    const x = yield func(query, client);
    done();
    return x;
  });

  const wrap = (func) => {
    func = co.wrap(func);
    return (...args) => {
      if(args.length > 0 && typeof args[args.length - 1] === 'function') {
        const cb = args.pop();
        func(...args).then(res => cb(null, res), err => cb(err, null));
      } else {
        return func(...args);
      }
    };
  };

  const persisting = (tableName) => {
    return {
      get: wrap(function *(id) {
        const result = yield dbexec(q => q(`SELECT json from ${tableName} where id = $1`, [id]));
        if(result.rowCount === 0) {
          throw {displayName: 'NotFound'};
        }
        return JSON.parse(result.rows[0].json);
      }),
      save: wrap(function *(data) {
        yield dbexec(q => q(`INSERT INTO ${tableName} (id, json)
                             VALUES ($1, $2)
                             ON CONFLICT (id) DO UPDATE SET json = EXCLUDED.json;`, [data.id, JSON.stringify(data)]))
      }),
      all: wrap(function *() {
        const result = yield dbexec(q => q(`SELECT json from ${tableName}`))
        return result.rows.map(x => JSON.parse(x.json));
      })
    };
  }

  const storage = {
    teams: persisting('botkit_teams'),
    channels: persisting('botkit_channels'),
    users: persisting('botkit_users'),
    end: () => {
      return promisedPool.then(x => x.end())
    }
  };

  return storage;
};
