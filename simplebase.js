
// var Hooks = require('level-hooks')
var debug = require('debug')('logbase')
var typeforce = require('typeforce')
var pl = require('pull-level')
var pull = require('pull-stream')
var mutexify = require('mutexify')
var sublevel = require('level-sublevel')
var LAST_CHANGE_KEY = 'count'
var COUNTER_SUBLEVEL = '~counter'
var DEFAULT_TIMEOUT = 2000

/**
 * augment a levelup to be a log consumer db
 * @param  {Object} opts
 * @param  {LevelUp} opts.db
 * @param  {Log} opts.log
 * @param  {Function} opts.process entry processor function
 * @param  {Boolean} opts.autostart (optional, default: true) start when ready
 * @return {sublevel}
 */
module.exports = function augment (opts) {
  typeforce({
    db: 'Object',
    log: 'Log',
    process: 'Function'
  }, opts)

  var autostart = opts.autostart !== false
  var db = opts.db
  var log = opts.log
  var processEntry = opts.process
  var entryTimeout = opts.timeout === false ? false : opts.timeout || DEFAULT_TIMEOUT
  var running
  var ready
  var live
  var closing
  var myPosition
  var nextPosition
  var lastSaved
  var logPos = 0
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
    if (autostart) sub.start()
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
  sub.start = function () {
    if (ready) {
      if (!running) read()
    } else {
      autostart = true
    }
  }

  return toReadOnly(sub)

  function prehook (change, add, batch) {
    if (change.key === LAST_CHANGE_KEY) {
      throw new Error(LAST_CHANGE_KEY + ' is a reserved key')
    }

    if (nextPosition === lastSaved || batch[batch.length - 1] !== change) {
      return
    }

    lastSaved = nextPosition

    add({
      type: 'put',
      key: LAST_CHANGE_KEY,
      value: nextPosition,
      prefix: counter
    })
  }

  function read () {
    // console.log('started!', db.db.location)
    running = true
    var appending = 0
    var appended = 0
    log.on('appending', function () {
      appending++
      live = false
      logPos++
    })

    log.on('appended', function () {
      appended++
      if (appended !== appending) {
        live = false
        logPos += (appended - appending)
        appending = appended
      }
    })

    log.last(function (err, _logPos) {
      if (err) return sub.emit('error', err)

      logPos += _logPos
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

        nextPosition = myPosition + 1
        lock(processorFor(entry, cb))
      }),
      pull.drain()
    )
  }

  function processorFor (entry, cb) {
    var timedOut
    var timeout
    return function (release) {
      // if (closing) return release(cb)
      if (entryTimeout !== false) {
        timeout = setTimeout(onTimedOut, entryTimeout)
      }

      processEntry.call(sub, entry, function (err) {
        if (timedOut) debug('timed out but eventually finished: ' + stringify(entry))
        if (timeout) clearTimeout(timeout)

        myPosition = nextPosition
        checkLive()
        // db.emit('tick')
        if (!timedOut) release(cb, err, entry)
      })

      function onTimedOut () {
        if (closing) return

        timedOut = true
        var msg = 'timed out processing:' + stringify(entry)
        var err = new Error(msg)
        debug(msg, db.db.location)
        sub.emit('error', err)
        release(cb, null, entry)
      }
    }
  }
}

function stringify (entry) {
  return JSON.stringify(entry.toJSON())
}

function toReadOnly (db) {
  var readOnly = {}
  for (var p in db) {
    if (p === 'put' || p === 'batch') {
      readOnly[p] = readOnlyErrThrower
      continue
    }

    var val = db[p]
    if (typeof val === 'function') {
      readOnly[p] = val.bind(db)
    } else {
      readOnly[p] = val
    }
  }

  return readOnly
}

function readOnlyErrThrower () {
  throw new Error('this database is read-only')
}
