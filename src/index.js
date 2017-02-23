const pg = require('pg');
const co = require('co');

module.exports = function (config, defaults) {
  if (typeof config == "object") {
    config = {
      user: config.user || process.env.BOTKIT_STORAGE_POSTGRES_USER || 'botkit',
      database: config.database || process.env.BOTKIT_STORAGE_POSTGRES_DATABASE || 'botkit',
      password: config.password || process.env.BOTKIT_STORAGE_POSTGRES_PASSWORD || 'botkit',
      host: config.host || process.env.BOTKIT_STORAGE_POSTGRES_HOST || 'localhost',
      port: config.port || process.env.BOTKIT_STORAGE_POSTGRES_PORT || '5432',
      max: config.maxClients || process.env.BOTKIT_STORAGE_POSTGRES_MAX_CLIENTS || '10',
      idleTimeoutMillis: config.idleTimeoutMillis || process.env.BOTKIT_STORAGE_POSTGRES_IDLE_TIMEOUT_MILLIS || '30000',
    };
  } else if (typeof config == 'string') {
    //nothing to validate, other than it being a string
  } else {
    throw new Error('Can only accept a connection string, or a configuration object');
  }

  if (defaults) {
    pg.defaults = Object.assign(pg.defaults, defaults);
  }

  const promisedPool = co(function *() {
    const q = (client, qstr) => new Promise((acc, rej) => client.query(qstr, [], (err, res) => err ? rej(err) : acc(res)))
      .catch(err => {throw new Error(`Could not execute '${qstr}'. Error: ${err.stack || err}`)});
    const connect = (client) => new Promise((acc, rej) => client.connect((err, done) => err ? rej(err) : acc()));

    const noDbClient = new pg.Client(config);
    yield connect(noDbClient);
    const dbexistsQuery = yield q(noDbClient, `SELECT 1 from pg_database WHERE datname='${noDbClient.database}'`);

    if(dbexistsQuery.rows.length === 0) {
      console.log(`botkit-storage-postgres> creating db ${noDbClient.database}`);
      yield q(noDbClient, `CREATE DATABASE ${noDbClient.database}`);
    }

    noDbClient.end();

    const dbClient = new pg.Client(config);
    yield connect(dbClient);

    yield ['botkit_teams', 'botkit_users', 'botkit_channels']
      .map(tableName => `CREATE TABLE IF NOT EXISTS ${tableName} (id char(50) NOT NULL PRIMARY KEY, json TEXT NOT NULL)`)
      .map(createQuery => q(dbClient, createQuery));

    dbClient.end();

    function FakeClient() {
      return new pg.Client(config);
    }

    const pool = yield Promise.resolve(new pg.Pool({ Client: FakeClient }));

    pool.on('error', function (err, client) {
      console.error('botkit-storage-postgres> idle client error', err.message, err.stack);
    });
    return pool;
  });

  promisedPool.then(() => {
    console.log(`botkit-storage-postgres> connected to database`);
  }, (err) => {
    console.error(`botkit-storage-postgres> error running setup. Error: '${err.stack}`);
  });

  const dbexec = co.wrap(function* (...args) {
    const pool = yield promisedPool;
    const query = yield pool.query(...args);
    return query;
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
        const result = yield dbexec(`SELECT json from ${tableName} where id = $1`, [id]);
        if(result.rowCount === 0) {
          throw {displayName: 'NotFound'};
        }
        return JSON.parse(result.rows[0].json);
      }),
      save: wrap(function *(data) {
        yield dbexec(`INSERT INTO ${tableName} (id, json)
                            VALUES ($1, $2)
                            ON CONFLICT (id) DO UPDATE SET json = EXCLUDED.json;`, [data.id, JSON.stringify(data)]);
      }),
      all: wrap(function *() {
        const result = yield dbexec(`SELECT json from ${tableName}`);
        return result.rows.map(x => JSON.parse(x.json));
      })
    };
  };

  const storage = {
    teams: persisting('botkit_teams'),
    channels: persisting('botkit_channels'),
    users: persisting('botkit_users'),
    end: () => promisedPool.then(x => x.end())
  };

  return storage;
};