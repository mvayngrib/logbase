
var Hooks = require('level-hooks')
var pl = require('pull-level')
var pull = require('pull-stream')
var mutexify = require('mutexify')
var LAST_CHANGE_KEY = '~counter'

module.exports = function augment (db, log, processEntry) {
  var ready
  var live
  var position
  var lock = mutexify()

  Hooks(db)

  db.hooks.pre({ start: '', end: '~' }, function (change, add) {
    if (change.key === LAST_CHANGE_KEY) {
      throw new Error(LAST_CHANGE_KEY + ' is a reserved key')
    }

    add({
      type: 'put',
      key: LAST_CHANGE_KEY,
      value: ++position
    })
  })

  db.hooks.post(function (change) {
    if (change.key === LAST_CHANGE_KEY) {
      db.emit('change', change.value)
    }
  })

  db.get(LAST_CHANGE_KEY, function (err, id) {
    if (err) {
      if (!err.notFound) throw err
    }

    position = id || 0
    ready = true
    db.emit('ready')
    read()
  })

  db.isLive = function () {
    return live
  }

  db.isReady = function () {
    return ready
  }

  return db

  function read () {
    var readPos = position
    var logPos
    log.on('appending', function () {
      logPos++
    })

    log.last(function (err, _logPos) {
      if (err) return db.emit('error', err)

      logPos = _logPos
      checkLive()

      pull(
        pl.read(log, {
          tail: true,
          live: true,
          since: position
        }),
        pull.asyncMap(function (op, cb) {
          lock(function (release) {
            processEntry(op, function (err) {
              readPos++
              checkLive()
              release(cb, err, op)
            })
          })
        }),
        pull.drain()
      )
    })

    function checkLive () {
      // may happen more than once
      if (readPos === logPos) {
        live = true
        db.emit('live')
      }
    }
  }
}
