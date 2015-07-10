
var util = require('util')
var combine = require('stream-combiner2')
var Writable = require('readable-stream').Writable
var levelup = require('levelup')
var changesFeed = require('changes-feed')
var typeforce = require('typeforce')
var map = require('map-stream')
var safe = require('safecb')
var rebuf = require('./rebuf')
var Entry = require('./entry')
var noop = function () {}

module.exports = Log
util.inherits(Log, Writable)

function Log (path, options) {
  if (!(this instanceof Log)) return new Log(path, options)

  this._db = levelup(path, options)
  this._log = changesFeed(this._db)
}

Log.prototype._write = function (chunk, enc, next) {
  this._log.append(chunk)
  next()
}

Log.prototype.close = function (cb) {
  this._db.close(cb)
}

Log.prototype.createReadStream = function (options) {
  if (options.keys === false) throw new Error('"keys" are required')

  return combine.obj(
    this._log.createReadStream(options),
    map(function (data, cb) {
      var hasValues = !options || options.values !== false
      if (hasValues) {
        var entry = Entry
          .fromJSON(rebuf(data.value))
          .id(data.change)

        cb(null, entry)
      } else {
        cb(null, data)
      }
    })
  )
}

Log.prototype.append = function (entry, cb) {
  typeforce('Entry', entry)
  entry.validate()
  return this._log.append(entry.toJSON(), cb || noop)
}

Log.prototype.last = function (cb) {
  cb = safe(cb)
  return this._log.createReadStream({ limit: 1, reverse: true })
    .once('error', cb)
    .once('data', function (entry) {
      cb(null, entry.id())
    })
    .once('close', function () {
      cb(null, 0)
    })
}
