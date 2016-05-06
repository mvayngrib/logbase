
var util = require('util')
var extend = require('xtend')
var thunky = require('thunky')
var pump = require('pump')
var Writable = require('readable-stream').Writable
var levelup = require('levelup')
var changesFeed = require('changes-feed')
var typeforce = require('typeforce')
var map = require('map-stream')
var safe = require('safecb')
var rebuf = require('./rebuf')

module.exports = Log
util.inherits(Log, Writable)

var entryEncoding = {
  encode: JSON.stringify,
  decode: function (val) {
    return rebuf(JSON.parse(val))
  },
  buffer: false,
  type: 'logEntry'
}

function Log (path, options) {
  var self = this
  if (!(this instanceof Log)) return new Log(path, options)

  Writable.call(this, { objectMode: true })

  options = extend(options)
  options.valueEncoding = entryEncoding

  this._db = levelup(path, options)
  this._log = changesFeed(this._db)

  this._length = undefined
  this._init = thunky(function (cb) {
    self.last(function (err, last) {
      if (err) throw err

      self._initialized = true
      self._length = last
      self.emit('ready')
      if (cb) cb()
    })
  })

  this._init()
}

Log.prototype.length = function () {
  return this._length
}

Log.prototype.onready = function (cb) {
  if (this._initialized) {
    process.nextTick(cb)
  } else {
    // make sure it's on next tick
    // as _append ops queued for after init need to
    // be taked into account first
    this.once('ready', this.onready.bind(this, cb))
  }
}

Log.prototype._write = function (chunk, enc, next) {
  this.append(chunk, next)
}

Log.prototype.close = function (cb) {
  this._db.close(cb)
}

Log.prototype.read =
Log.prototype.readStream =
Log.prototype.createReadStream = function (options) {
  if (options.keys === false) throw new Error('"keys" are required')

  return pump(
    this._log.createReadStream(options),
    map(function (data, cb) {
      var hasValues = !options || options.values !== false
      if (hasValues) {
        data.value.id = data.change
        cb(null, data.value)
      } else {
        cb(null, data)
      }
    })
  )
}

Log.prototype.get = function (id, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }

  return this._log.get(id, opts, function (err, entry) {
    if (err) return cb(err)

    entry.id = id
    cb(null, entry)
  })
}

Log.prototype.append = function (entry, cb) {
  var self = this

  // assert(entry.type, 'log entry must have "type"')
  this._init(function () {
    self._setLength(self._length + 1)
    return self._log.append(entry, function (err) {
      if (err) self._setLength(self._length - 1)

      if (cb) cb(err)
    })
  })
}

Log.prototype._setLength = function (length) {
  if (this._length !== length) {
    this._length = length
    this.emit('length', length)
  }
}

Log.prototype.last = function (cb) {
  cb = safe(cb)
  return this.read({ limit: 1, reverse: true })
    .once('error', cb)
    .once('data', function (entry) {
      cb(null, entry.id)
    })
    .once('end', function () {
      cb(null, 0)
    })
}

// function assert (statement, msg) {
//   if (!statement) throw new Error(msg)
// }
