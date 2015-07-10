
var util = require('util')
var EventEmitter = require('events').EventEmitter
var typeforce = require('typeforce')
var safe = require('safecb')
var sublevel = require('level-sublevel')
var LAST_CHANGE_ID_KEY = 'last'

module.exports = LogBase
util.inherits(LogBase, EventEmitter)

function LogBase (options) {
  var self = this
  if (!(this instanceof LogBase)) return new LogBase(options)

  typeforce({
    log: 'Log',
    db: 'Object'
  }, options)

  this._log = options.log
  this._rawDB = options.db
  this._sub = this._rawDB.sublevel ? this._rawDB : sublevel(this._rawDB)
  this._meta = this._sub.sublevel('meta')
  this._db = this._sub.sublevel('main')

  this._meta.get(LAST_CHANGE_ID_KEY, function (err, id) {
    if (err) {
      if (!err.notFound) throw err
    }

    self._position = id || 0
    self._startReading()
    self._ready = true
    self.emit('ready')
  })

  this._db.pre({ start: '', end: '~' }, function (change, add) {
    add({
      type: 'put',
      key: LAST_CHANGE_ID_KEY,
      value: ++self._position,
      prefix: self._meta
    })
  })

  this._db.post(function (change) {
    self.emit('ate', change)
  })

  this._meta.post(function (change) {
    self.emit('change', change.value)
  })
}

LogBase.prototype._startReading = function () {
  var self = this

  this._log.createReadStream({
      live: true,
      since: this._position
    })
    .on('data', function (data) {
      if (self.willProcess(data)) self.process(data)
    })
    .on('error', this.emit.bind(this, 'error'))
}

LogBase.prototype.last = function () {
  return this._position
}

LogBase.prototype.get = function () {
  return this._db.get.apply(this._db, arguments)
}

LogBase.prototype.put = function () {
  return this._db.put.apply(this._db, arguments)
}

LogBase.prototype.del = function () {
  return this._db.del.apply(this._db, arguments)
}

LogBase.prototype.batch = function () {
  return this._db.batch.apply(this._db, arguments)
}

LogBase.prototype._catchUp = function (cb) {
  var self = this
  cb = safe(cb)

  this._log.createReadStream({
      since: this._position
    })
    .on('data', function (data) {
      if (self.willProcess(data)) self.process(data)
    })
    .on('end', cb)
    .on('close', cb)
}

LogBase.prototype.willProcess = function (entry) {
  if (this._destroyed || entry.id() <= this._position) return false
  if (this._willProcess) return this._willProcess(entry)

  return true
}

LogBase.prototype.process = function (entry, cb) {
  cb = safe(cb)
  if (!this.willProcess(entry)) {
    return cb(new Error('refusing to process entry'))
  }

  return this._process(entry, cb)
}

LogBase.prototype.destroy = function (cb) {
  this._destroyed = true
  this._rawDB.close(cb)
}
