
var assert = require('assert')
var util = require('util')
var extend = require('extend')
var combine = require('stream-combiner2')
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

function Log (path, options) {
  if (!(this instanceof Log)) return new Log(path, options)

  options = extend({
    valueEncoding: 'json'
  }, options)

  assert.equal(options.valueEncoding, 'json')
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

  var pipeline = [
    this._log.createReadStream(options),
    map(function (data, cb) {
      var hasValues = !options || options.values !== false
      if (hasValues) {
        cb(null, new Entry(rebuf(data.value)))
      } else {
        cb(null, data)
      }
    })
  ]

  return combine.obj.apply(combine, pipeline)
}

Log.prototype.get = function (id, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }

  return this._log.get(id, opts, function (err, val) {
    if (err) return cb(err)

    var entry = new Entry(rebuf(val))
     .id(id)

    cb(null, entry)
  })
}

Log.prototype.append = function (entry, cb) {
  typeforce('Entry', entry)
  return this._log.append(entry.toJSON(), safe(cb))
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
