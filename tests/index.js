const test = require('unit.js');
const pg = require('pg');
const async = require('async');
const storage = require('../src/index');

testObj0 = {id: 'TEST0', foo: 'bar0'};
testObj1 = {id: 'TEST1', foo: 'bar1'};

var testStorageMethod = function(storageMethod, cb) {
    storageMethod.save(testObj0, function(err) {
        if(err) cb(err);
        test.assert(!err, 'save0 got error');
        storageMethod.save(testObj1, function(err) {
            if(err) cb(err);
            test.assert(!err, 'save1 got error');
            async.parallel([
              (t1cb) => storageMethod.get(testObj0.id, function(err, data) {
                  try {
                    test.assert(!err, 'get got error');
                    test.assert(data.foo === testObj0.foo, 'get compare failed');
                  }
                  catch(testError) {
                    t1cb(testError);
                  }
                  t1cb();
              }),
              (t2cb) => storageMethod.get('shouldnt-be-here', function(err, data) {
                  try {
                    test.assert(err.displayName === 'NotFound', 'shouldnt-be-here didnt return NotFound error. ' + err.stack);
                    test.assert(!data, "shouldnt-be-here was found");
                  }
                  catch(testError) {
                    t2cb(testError);
                  }
                  t2cb();
              }),
              (t3cb) => storageMethod.all(function(err, data) {
                  try {
                    test.assert(!err, "all got error");
                    test.assert(
                        data[0].foo === testObj0.foo && data[1].foo === testObj1.foo ||
                        data[0].foo === testObj1.foo && data[1].foo === testObj0.foo,
                        "all compare failed"
                    );
                  }
                  catch(testError) {
                    t3cb(testError);
                  }
                  t3cb();
              })
            ], cb);
        });
    });
};

const dbConfig = {
  user: process.env.BOTKIT_STORAGE_POSTGRES_USER || 'botkit',
  database: process.env.BOTKIT_STORAGE_POSTGRES_DATABASE || 'botkit_test',
  password: process.env.BOTKIT_STORAGE_POSTGRES_PASSWORD || 'botkit',
  host: process.env.BOTKIT_STORAGE_POSTGRES_HOST || 'localhost',
  port: process.env.BOTKIT_STORAGE_POSTGRES_PORT || '5432'
};

const dbConnectionString = process.env.BOTKIT_STORAGE_POSTGRES_CONNECTIONSTRING || 'postgresql://botkit:botkit@localhost:5432/botkit_test';

var doTest = (connectionMethod, connectionMethodName) => {
  return cb => {
    const pgClient = new pg.Client(connectionMethod);
    pgClient.connect();
    pgClient.query(`
        drop schema public cascade;
        create schema public;`, (err, res) => {
        if(err) throw new Error(err.stack);
        pgClient.end();
        var pg_storage = storage(connectionMethod);

        async.parallel([
          (cb) => testStorageMethod(pg_storage.users, cb),
          (cb) => testStorageMethod(pg_storage.channels, cb),
          (cb) => testStorageMethod(pg_storage.teams, cb),
        ], (err, res) => {
          if (err) {
            console.error(`${connectionMethodName} test failed!`, err);
            cb(err, null);
          } else {
            console.log(`${connectionMethodName} test succeeded!`);
            cb(null, true);
          }
          pg_storage.end();
        });
    });
  };
};

async.series([
  doTest(dbConfig, "Configuration object"),
  doTest(dbConnectionString, "Connection string")
]);
