
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
var noop = function () {}

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
  var processing
  var processEntry = opts.process
  var entryTimeout = opts.timeout === false ? false : opts.timeout || DEFAULT_TIMEOUT
  var checkLiveTimeout
  var running
  var ready
  var live
  var closing
  var myPosition
  var lastSaved
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
    // console.log('started!', db.db.location)
    running = true
    log.on('appending', function () {
      live = false
    })

    doRead()
  }

  function checkLive (cb) {
    cb = cb || noop

    var tmpLive = true
    log.on('appending', notLive)
    log.last(function (err, logPos) {
      log.removeListener('appending', notLive)

      if (err) {
        sub.emit('error', err)
        cb(err)
      }

      var cursorPos = processing ? myPosition - 1 : myPosition
      tmpLive = tmpLive && logPos === cursorPos

      if (!tmpLive) {
        live = false
        // do nothing
      } else {
        if (!live) {
          live = true
          sub.emit('live')
        }
      }

      cb(null, live)
    })

    function notLive () {
      tmpLive = false
    }
  }

  function postProcess () {
    clearTimeout(checkLiveTimeout)
    checkLiveTimeout = setTimeout(checkLive, 200)
  }

  function doRead () {
    pull(
      pl.read(log, {
        tail: true,
        live: true,
        since: myPosition
      }),
      pull.asyncMap(function (entry, cb) {
        lock(processorFor(entry, cb))
      }),
      pull.drain()
    )
  }

  function processorFor (entry, cb) {
    var timedOut
    var timeout
    return function (release) {
      if (entryTimeout !== false) {
        timeout = setTimeout(onTimedOut, entryTimeout)
      }

      processing = true
      myPosition++
      processEntry(entry, function (err) {
        processing = false
        if (timeout) clearTimeout(timeout)

        if (timedOut) {
          debug('timed out but eventually finished: ' + stringify(entry))
        } else {
          release(cb, err, entry)
        }

        postProcess()
      })

      function onTimedOut () {
        if (closing) return

        timedOut = true
        var msg = 'timed out processing:' + stringify(entry)
        var err = new Error(msg)
        debug(msg, db.db.location)
        sub.emit('error', err)
        if (!closing) release(cb, err, entry)
      }
    }
  }
}

function stringify (entry) {
  return JSON.stringify(entry.toJSON())
}
