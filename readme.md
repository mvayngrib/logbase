# logbase

Append-only log and log-based database

_this module is used by [Tradle](https://github.com/tradle/about/wiki)_

[![NPM](https://nodei.co/npm/logbase.png)](https://nodei.co/npm/logbase/)

# Usage

```js

var lb = require('logbase')
var log = new lb.Log('path/to/db', {
  db: leveldown
})

var red = new lb.Entry()
  .set({
    name: 'roxie',
    color: 'red'
  })
  
// change color
var blue = new lb.Entry()
  .set({
    name: 'roxie',
    color: 'blue'
  })

log.append(red)
log.append(blue)

// stores latest colors
var db = new lb.Base({
  log: log,
  db: levelup('path/to/another/db', { 
    db: leveldown,
    valueEncoding: 'utf8'
  })
})

db._process = function (entry, cb) {
  this._db.put(entry.get('name'), entry.get('color'), cb)
}

```
