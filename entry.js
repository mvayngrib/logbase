
var typeforce = require('typeforce')
var extend = require('extend')
var rest = require('./rest')
// var RESERVED_PROPS = [
//   'type',
//   'id',
//   'timestamp',
//   'prev'
// ]

module.exports = Entry

function Entry (props) {
  if (!(this instanceof Entry)) return new Entry(props)

  this._props = extend(true, {
    timestamp: Date.now()
    // id: null
  }, props)
}

Entry.prototype.prev = function (id) {
  if (arguments.length === 0) return this._props.prev

  var prevId
  if (typeof id === 'number') {
    prevId = id
  } else if (id instanceof Entry) {
    prevId = id.prev().slice()
    prevId.push(id.id())
  } else if (id._l) {
    prevId = id._l.id
  }

  if (typeof prevId !== 'number' && !Array.isArray(prevId)) {
    throw new Error('invalid "prev"')
  }

  typeforce('Number', prevId)
  this._props.prev = this._props.prev.concat(prevId)
  return this
}

Entry.prototype.timestamp = function () {
  return this._props.timestamp
}

Entry.prototype.get = function (name) {
  return this._props[name]
}

Entry.prototype.set = function (name, value) {
  if (typeof name === 'object') {
    extend(true, this._props, name)
  } else {
    this._props[name] = value
  }

  return this
}

Entry.prototype.copy = function (props) {
  if (arguments.length === 1) this.set(props)
  else {
    rest(arguments).forEach(function (prop) {
      this._props[prop] = getProp(props, prop)
    }, this)
  }

  return this
}

Entry.prototype.toJSON = function () {
  return extend(true, {}, this._props)
}

Entry.prototype.clone = function () {
  return new Entry(this.toJSON())
}

function getProp (obj, name) {
  return obj instanceof Entry ? obj.get(name) : obj[name]
}

;['type', 'id'].forEach(function (prop) {
  Entry.prototype[prop] = function (val) {
    if (typeof val === 'undefined') {
      return this._props[prop]
    }

    if (val == null) delete this._props[prop]
    else this._props[prop] = val

    return this
  }
})

