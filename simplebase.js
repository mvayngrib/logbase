
var debug = require('debug')('logbase')
var pump = require('pump')
var typeforce = require('typeforce')
// var pl = require('pull-level')
// var pull = require('pull-stream')
var mutexify = require('mutexify')
// var through = require('through')
var PassThrough = require('readable-stream').PassThrough
var NULL_CHAR = '\x00'
var COUNTER_KEY = NULL_CHAR
var DEFAULT_TIMEOUT = 2000
var LONG_TIMEOUT = 20000

/**
 * augment a levelup to be a log consumer db
 * @param  {Object} opts
 * @param  {LevelUp} opts.db
 * @param  {Log} opts.log
 * @param  {Function} opts.process entry processor function
 * @param  {Boolean} opts.autostart (optional, default: true) start when ready
 * @return {LevelUp} same instance
 */
module.exports = function augment (opts) {
  typeforce({
    db: 'Object',
    log: 'Log',
    process: 'Function',
    timeout: typeforce.oneOf('Boolean', 'Number', 'Null')
  }, opts)

  var processing
  var longTimeout
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
  // var nextPosition
  var logPos = 0
  var lock = mutexify()

  db.setMaxListeners(0)
  db.get(COUNTER_KEY, function (err, id) {
    if (err) {
      if (!err.notFound) throw err
    }

    myPosition = id || 0
    ready = true
    db.emit('ready')
    if (autostart) db.start()
  })

  db.once('closing', function () {
    closing = true
    clearInterval(longTimeout)
  })

  db.isLive = function () {
    return live
  }

  db.isReady = function () {
    return ready
  }

  db.onLive = function (cb) {
    if (processing) {
      throw new Error('using onLive while processing an entry causes deadlock')
    }

    if (db.isLive()) return cb()
    else db.once('live', cb)
  }

  db.liveOnly = function (fn, ctx) {
    return function () {
      var args = arguments
      db.onLive(function () {
        return fn.apply(ctx || this, args)
      })
    }
  }

  db.start = function () {
    if (ready) {
      if (!running) read()
    } else {
      autostart = true
    }
  }

  db.rawReadStream = db.createReadStream
  db.createReadStream = function (opts) {
    opts = opts || {}
    if (opts.start) {
      if (opts.start <= NULL_CHAR) {
        throw new Error('invalid range')
      }
    } else {
      opts.start = '\x00\x00' // skip counter
    }

    var paused = new PassThrough({ objectMode: true })
    var rs
    paused.destroy = function () {
      if (rs) rs.destroy()
      else this.end()
    }

    paused.pause()
    db.onLive(function () {
      rs = db.rawReadStream.call(db, opts)
      pump(
        rs,
        paused
      )

      paused.resume()
    })

    return paused
  }

  return db

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
      if (err) return db.emit('error', err)

      logPos += _logPos
      checkLive()
      doRead()
    })
  }

  function checkLive () {
    // may happen more than once
    if (myPosition === logPos) {
      live = true
      db.emit('live')
      debug('db is live')
    }
  }

  function doRead () {
    var stream = log.createReadStream({
      live: true,
      since: myPosition
    })

    var resume = stream.resume.bind(stream)
    stream.on('data', function (entry) {
      // if (stream.isPaused()) throw new Error('oops')

      stream.pause()
      lock(processorFor(entry, resume))
    })
  }

  function processorFor (entry, cb) {
    var timedOut
    var timeout
    return function (release) {
      // if (closing) return release(cb)
      if (entryTimeout !== false) {
        timeout = setTimeout(onTimedOut, entryTimeout)
      }

      var nextPosition = entry.id()
      var upCounter = {
        type: 'put',
        key: COUNTER_KEY,
        value: nextPosition
      }

      var startTime = Date.now()
      longTimeout = setInterval(function () {
        var time = Date.now() - startTime
        debug('still processing (' + time + 'ms): ' + stringify(entry))
      }, LONG_TIMEOUT)

      processing = true
      processEntry(entry, function (batch) {
        processing = false
        clearInterval(longTimeout)
        if (timedOut) debug('timed out but eventually finished: ' + stringify(entry))
        if (timeout) clearTimeout(timeout)

        batch = batch || []
        var valid = batch.every(function (item) {
          return item.key.charAt(0) !== NULL_CHAR
        })

        if (!valid) throw new Error('no nulls allowed in keys')

        batch.push(upCounter)
        db.batch(batch, postProcess)
      })

      function postProcess (err) {
        if (closing) return
        if (err) db.emit('error')
        // continue even if error?

        myPosition = nextPosition
        checkLive()
        db.emit('change', nextPosition)
        if (!timedOut) release(cb, err, entry)
      }

      function onTimedOut () {
        if (closing) return

        timedOut = true
        var msg = 'timed out processing:' + stringify(entry)
        var err = new Error(msg)
        debug(msg, db.db.location)
        db.emit('error', err)
        release(cb, null, entry)
      }
    }
  }
}

function stringify (entry) {
  return JSON.stringify(entry.toJSON())
}

// function toReadOnly (db) {
//   var readOnly = {}
//   for (var p in db) {
//     if (p === 'put' || p === 'batch') {
//       readOnly[p] = readOnlyErrThrower
//       continue
//     }

//     var val = db[p]
//     if (typeof val === 'function') {
//       readOnly[p] = val.bind(db)
//     } else {
//       readOnly[p] = val
//     }
//   }

//   return readOnly
// }

// function readOnlyErrThrower () {
//   throw new Error('this database is read-only')
// }
