
var util = require('util')
var extend = require('xtend')
var pump = require('pump')
var Writable = require('readable-stream').Writable
var levelup = require('levelup')
var changesFeed = require('changes-feed')
var typeforce = require('typeforce')
var map = require('map-stream')
var safe = require('safecb')
var rebuf = require('./rebuf')
var Entry = require('./entry')

module.exports = Log
util.inherits(Log, Writable)

var entryEncoding = {
  encode: function (entry) {
    return JSON.stringify(entry.toJSON())
  },
  decode: function (entry) {
    return new Entry(rebuf(JSON.parse(entry)))
  },
  buffer: false,
  type: 'logEntry'
}

function Log (path, options) {
  if (!(this instanceof Log)) return new Log(path, options)

  Writable.call(this, { objectMode: true })

  options = extend(options)
  options.valueEncoding = entryEncoding

  this._db = levelup(path, options)
  this._log = changesFeed(this._db)
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
        cb(null, data.value.id(data.change))
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

    entry.id(id)
    cb(null, entry)
  })
}

Log.prototype.append = function (entry, cb) {
  var self = this
  typeforce('Entry', entry)
  this.emit('appending', entry)
  return this._log.append(entry, function () {
    self.emit('appended', entry)
    if (cb) cb()
  })
}

Log.prototype.last = function (cb) {
  cb = safe(cb)
  return this.read({ limit: 1, reverse: true })
    .once('error', cb)
    .once('data', function (entry) {
      cb(null, entry.id())
    })
    .once('end', function () {
      cb(null, 0)
    })
}
