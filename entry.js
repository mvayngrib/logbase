
var typeforce = require('typeforce')
var extend = require('extend')
var rest = require('./rest')
var ArrayProto = Array.prototype

module.exports = Entry

function Entry (id) {
  if (!(this instanceof Entry)) return new Entry(id)

  this._props = {}
  this._metadata = {
    timestamp: Date.now(),
    tags: [],
    prev: []
    // id: null,
    // prev: null
  }

  if (typeof id !== 'undefined') this._metadata.id = id
}

Entry.prototype.meta =
Entry.prototype.metadata = function (name, value) {
  if (arguments.length === 0) {
    return extend(true, {}, this._metadata)
  }

  if (arguments.length === 1) {
    if (typeof name === 'string') {
      return this._metadata[name]
    } else {
      extend(true, this._metadata, name)
    }
  } else {
    this._metadata[name] = this._metadata[value]
  }

  return this
}

Entry.prototype.hasTag = function (tag) {
  validateTag(tag)
  return this._metadata.tags.indexOf(tag) !== -1
}

Entry.prototype.tag = function (tags) {
  var myTags = this._metadata.tags
  tags = ArrayProto.concat.apply([], arguments)
  tags.forEach(validateTag)
  tags.forEach(function (tag) {
    if (myTags.indexOf(tag) === -1) {
      myTags.push(tag)
    }
  })

  return this
}

Entry.prototype.copyTags = function (entry /*, tags */) {
  if (arguments.length === 1) this.tag(entry.tags())

  rest(arguments).forEach(function (tag) {
    if (entry.hasTag(tag)) this.tag(tag)
  }, this)

  return this
}

Entry.prototype.id = function (id) {
  if (typeof id === 'number') {
    this._metadata.id = id
    return this
  } else {
    return this._metadata.id
  }
}

Entry.prototype.tags = function () {
  return this._metadata.tags.slice()
}

Entry.prototype.prev = function (id) {
  if (arguments.length === 0) return this._metadata.prev

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
  this._metadata.prev = this._metadata.prev.concat(prevId)
  return this
}

Entry.prototype.timestamp = function () {
  return this._metadata.timestamp
}

Entry.prototype.get = function (name) {
  return this._props[name]
}

Entry.prototype.data =
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

Entry.prototype.toJSON = function (skipMetadata) {
  this.validate()

  return {
    meta: extend(true, {}, this._metadata),
    data: extend(true, {}, this._props)
  }
}

Entry.prototype.validate = function () {
  return !!this._metadata.tags.length
}

Entry.fromJSON = function (json) {
  typeforce({
    meta: '?Object',
    data: 'Object'
  }, json)

  var entry = new Entry()
    .data(json.data)

  if (json.meta) {
    entry.meta(json.meta)
  }

  return entry
}

function getProp (obj, name) {
  return obj instanceof Entry ? obj.get(name) : obj[name]
}

function validateTag (tag) {
  typeforce('String', tag)
}
