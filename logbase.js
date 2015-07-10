
var util = require('util')
var EventEmitter = require('events').EventEmitter
var Writable = require('readable-stream').Writable
var typeforce = require('typeforce')
var safe = require('safecb')
var sublevel = require('level-sublevel')
var addStream = require('add-stream')
var filter = require('./filterStream')
// var map = require('map-stream')
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

  EventEmitter.call(this)

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

  this._ws = new Writable({ objectMode: true })
  this._ws._write = function (entry, enc, next) {
    if (!self.willProcess(entry)) {
      next()
    } else {
      self.process(entry, next)
    }
  }
}

LogBase.prototype._startReading = function () {
  var self = this
  this._log.last(function (err, logPosition) {
    if (err) throw err

    var catchUp = self._log.createReadStream({
        live: false,
        since: self._position
      })
      .pipe(filter(function (entry) {
        return entry.id() <= logPosition
      }))
      .on('end', self.emit.bind(self, 'live'))

    var live = self._log.createReadStream({
        live: true,
        since: logPosition
      })
      // .pipe(map(function (data, cb) {
      //   cb(null, data)
      // }))

    catchUp
      .pipe(addStream.obj(live))
      .pipe(self._ws)
      .on('error', self.emit.bind(self, 'error'))
  })
}

LogBase.prototype.last = function () {
  return this._position
}

LogBase.prototype.willProcess = function (entry) {
  if (this._destroyed || entry.id() <= this._position) return false
  if (this._willProcess) return this._willProcess(entry)

  return true
}

LogBase.prototype.process = function (entry, cb) {
  return this._process(entry, safe(cb))
}

LogBase.prototype.destroy = function (cb) {
  this._destroyed = true
  this._rawDB.close(cb)
}

;['put', 'get', 'del', 'batch'].forEach(function (method) {
  LogBase.prototype[method] = function () {
    var cb = arguments[arguments.length - 1]
    if (this._destroyed) {
      if (typeof cb === 'function') cb(new Error('destroyed'))
      else this.emit('error', new Error('destroyed'))

      return
    }

    return this._db[method].apply(this._db, arguments)
  }
})
