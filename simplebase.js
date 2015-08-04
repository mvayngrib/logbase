
// var Hooks = require('level-hooks')
var pl = require('pull-level')
var pull = require('pull-stream')
var mutexify = require('mutexify')
var sublevel = require('level-sublevel')
var LAST_CHANGE_KEY = 'count'
var COUNTER_SUBLEVEL = '~counter'

module.exports = function augment (db, log, processEntry) {
  var ready
  var live
  var closing
  var myPosition
  var lastSaved
  var logPos
  var lock = mutexify()

  var sub = sublevel(db)
  sub.setMaxListeners(0)
  var counter = sub.sublevel(COUNTER_SUBLEVEL)

  sub.pre(prehook)

  var nextSub = sub.sublevel
  sub.sublevel = function () {
    var sublev = nextSub.apply(this, arguments)
    sublev.pre(prehook)
    return sublev
  }

  counter.post(function (change) {
    sub.emit('change', change.value)
  })

  counter.get(LAST_CHANGE_KEY, function (err, id) {
    if (err) {
      if (!err.notFound) throw err
    }

    lastSaved = myPosition = id || 0
    ready = true
    sub.emit('ready')
    read()
  })

  sub.isLive = function () {
    return live
  }

  sub.isReady = function () {
    return ready
  }

  sub.onLive = function (cb) {
    if (sub.isLive()) return cb()
    else sub.once('live', cb)
  }

  db.once('closing', function () {
    closing = true
  })

  sub.close = db.close.bind(db)

  return sub

  function prehook (change, add, batch) {
    if (change.key === LAST_CHANGE_KEY) {
      throw new Error(LAST_CHANGE_KEY + ' is a reserved key')
    }

    if (myPosition === lastSaved || batch[batch.length - 1] !== change) {
      return
    }

    lastSaved = myPosition

    add({
      type: 'put',
      key: LAST_CHANGE_KEY,
      value: myPosition,
      prefix: counter
    })
  }

  function read () {
    log.on('appending', function () {
      live = false
      logPos++
    })

    log.last(function (err, _logPos) {
      if (err) return sub.emit('error', err)

      logPos = _logPos
      checkLive()
      doRead()
    })
  }

  function checkLive () {
    // may happen more than once
    if (myPosition === logPos) {
      live = true
      sub.emit('live')
    }
  }

  function doRead () {
    pull(
      pl.read(log, {
        tail: true,
        live: true,
        since: myPosition
      }),
      pull.asyncMap(function (entry, cb) {
        // if (closing) return cb()

        myPosition++
        lock(function (release) {
          // if (closing) return release(cb)

          var timeout = setTimeout(function () {
            if (!closing) {
              throw new Error('timed out processing:' + JSON.stringify(entry.toJSON(), null, 2))
            }
          }, 2000)

          processEntry(entry, function (err) {
            clearTimeout(timeout)
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
