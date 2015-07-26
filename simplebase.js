
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
  var logPos
  var lock = mutexify()

  var sub = sublevel(db)
  var counter = sub.sublevel(COUNTER_SUBLEVEL)
  var main = sub.sublevel('main')

  counter.post(function (change) {
    main.emit('change', change.value)
  })

  counter.get(LAST_CHANGE_KEY, function (err, id) {
    if (err) {
      if (!err.notFound) throw err
    }

    myPosition = id || 0
    ready = true
    main.emit('ready')
    read()
  })

  main.isLive = function () {
    return live
  }

  main.isReady = function () {
    return ready
  }

  main.onLive = function (cb) {
    if (main.isLive()) return cb()
    else main.once('live', cb)
  }

  db.once('closing', function () {
    closing = true
  })

  return main

  function read () {
    log.on('appending', function () {
      live = false
      logPos++
    })

    log.last(function (err, _logPos) {
      if (err) return main.emit('error', err)

      logPos = _logPos
      checkLive()
      doRead()
    })
  }

  function checkLive () {
    // may happen more than once
    if (myPosition === logPos) {
      live = true
      main.emit('live')
    }
  }

  function doRead () {
    pull(
      pl.read(log, {
        tail: true,
        live: true,
        since: myPosition
      }),
      pull.asyncMap(processor),
      pull.drain()
    )
  }

  function processor (entry, cb) {
    // if (closing) return cb()

    myPosition++
    lock(function (release) {
      // if (closing) return release(cb)

      var timeout = setTimeout(function () {
        if (!closing) {
          throw new Error('timed out processing:' + JSON.stringify(entry.toJSON(), null, 2))
        }
      }, 2000)

      var batch = []

      processEntry(entry, add, onProcessed)

      function add (op) {
        if (typeof op.prefix === 'undefined') {
          op.prefix = main
        }

        batch.push(op)
      }

      function onProcessed (err) {
        clearTimeout(timeout)
        if (err || !batch.length) return finish(err)

        batch.push({
          type: 'put',
          key: LAST_CHANGE_KEY,
          value: myPosition,
          prefix: counter
        })

        sub.batch(batch, finish)
      }

      function finish (err) {
        checkLive()
        // db.emit('tick')
        release(cb, err, entry)
      }
    })
  }
}
