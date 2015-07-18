
var extend = require('xtend')
// var RESERVED_PROPS = [
//   'type',
//   'id',
//   'timestamp',
//   'prev'
// ]

module.exports = Entry

function Entry (props) {
  if (!(this instanceof Entry)) return new Entry(props)

  this._props = extend({
    timestamp: Date.now(),
    prev: []
  }, props)
}

Entry.prototype.prev = function (id) {
  if (arguments.length === 0) {
    return this._props.prev ? this._props.prev.slice() : []
  }

  var history
  if (typeof id === 'number') {
    history = [id]
  } else if (id instanceof Entry) {
    history = id.prev() || []
    history.push(id.id())
  } else if (Array.isArray(id)) {
    history = id
  } else {
    throw new Error('invalid "prev"')
  }

  this._props.prev = history
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
    extend(this._props, name)
  } else {
    this._props[name] = value
  }

  return this
}

Entry.prototype.unset = function (name) {
  delete this._props[name]
  return this
}

Entry.fromJSON = function (json) {
  return new Entry(json) // simple for now
}

Entry.prototype.toJSON = function () {
  return extend({}, this._props)
}

Entry.prototype.clone = function () {
  return new Entry(this.toJSON())
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

