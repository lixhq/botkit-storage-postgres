# botkit-storage-postgres

Postgres storage module for Botkit

## Usage

Install with npm

```
npm install botkit-storage-postgres --save
```

and require it and use it:

```
var botkitStoragePostgres = require('botkit-storage-mysql');
var Botkit = require('botkit');

var controller = Botkit.slackbot({
  storage: botkitStoragePostgres({
    host: 'localhost',
    user: 'botkit',
    password: 'botkit',
    database: 'botkit'
  })
});
```