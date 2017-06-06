# NOT MAINTAINED

This project is not maintained and is not used by creators anymore. If anybody want to maintain the project, fork it or write us.

# botkit-storage-postgres

Postgres storage module for Botkit

## Usage

Install with npm

```
npm install botkit-storage-postgres --save
```

and require it and use it:

```
var botkitStoragePostgres = require('botkit-storage-postgres');
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
### Requirements
* Node 6.7 or later
* Postgres 9.5 or later
