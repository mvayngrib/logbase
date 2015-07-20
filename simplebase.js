
var Hooks = require('level-hooks')
var pl = require('pull-level')
var pull = require('pull-stream')
var mutexify = require('mutexify')
var LAST_CHANGE_KEY = '~counter'

module.exports = function augment (db, log, processEntry) {
  var ready
  var live
  var closing
  var position
  var readPos = position
  var logPos
  var lock = mutexify()

  Hooks(db)

  db.hooks.pre({ start: '', end: '~' }, function (change, add, batch) {
    if (change.key === LAST_CHANGE_KEY) {
      throw new Error(LAST_CHANGE_KEY + ' is a reserved key')
    }

    if (batch && batch[batch.length - 1] !== change) return

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

  db.onLive = function (cb) {
    if (db.isLive()) return cb()
    else db.once('live', cb)
  }

  db.once('closing', function () {
    closing = true
  })

  return db

  function read () {
    log.on('appending', function () {
      live = false
      logPos++
    })

    log.last(function (err, _logPos) {
      if (err) return db.emit('error', err)

      logPos = _logPos
      checkLive()
      doRead()
    })
  }

  function checkLive () {
    // may happen more than once
    if (readPos === logPos) {
      live = true
      db.emit('live')
    }
  }

  function doRead () {
    pull(
      pl.read(log, {
        tail: true,
        live: true,
        since: position
      }),
      pull.asyncMap(function (entry, cb) {
        // if (closing) return cb()

        lock(function (release) {
          // if (closing) return release(cb)

          var timeout = setTimeout(function () {
            if (!closing) {
              throw new Error('timed out processing:' + JSON.stringify(entry.toJSON(), null, 2))
            }
          }, 2000)

          processEntry(entry, function (err) {
            clearTimeout(timeout)
            readPos++
            checkLive()
            // db.emit('tick')
            release(cb, err, entry)
          })
        })
      }),
      pull.drain()
    )
  }
}
