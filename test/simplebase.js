
var test = require('tape')
var levelup = require('levelup')
var leveldown = require('memdown')
var SimpleBase = require('../simplebase')
var Log = require('../log')
var LogEntry = require('../entry')
var counter = 0

function nextName () {
  return (counter++) + '.db'
}

test('add while reading', function (t) {
  var numEntries = 15
  t.plan(15)

  var numQueued = 0
  var batchSize = 3
  var log = new Log(nextName(), {
    db: leveldown,
    valueEncoding: 'json'
  })

  log.setMaxListeners(0)
  var ldb = levelup(nextName(), {
    db: leveldown,
    valueEncoding: 'json'
  })

  var base = SimpleBase({
    timeout: false,
    db: ldb,
    log: log,
    process: function (entry, cb) {
      if (numQueued < numEntries) {
        numQueued += 3
        addEntries(log, batchSize)
      }

      setTimeout(function () {
        cb()
        t.pass()
      }, 200)
    }
  })

  base.on('error', t.error)

  numQueued += 3
  addEntries(log, batchSize)
})

test('start/stop', function (t) {
  t.plan(1)
  t.timeoutAfter(1000)

  var log = new Log(nextName(), {
    db: leveldown,
    valueEncoding: 'json'
  })

  log.setMaxListeners(0)
  var ldb = levelup(nextName(), {
    db: leveldown,
    valueEncoding: 'json'
  })

  var base = SimpleBase({
    db: ldb,
    log: log,
    process: function (entry, cb) {
      base.close(function () {
        t.pass()
      })
    }
  })

  addEntries(log, 1)
})

test('no timeout', function (t) {
  t.plan(1)

  var log = new Log(nextName(), {
    db: leveldown,
    valueEncoding: 'json'
  })

  log.setMaxListeners(0)
  var ldb = levelup(nextName(), {
    db: leveldown,
    valueEncoding: 'json'
  })

  var base = SimpleBase({
    timeout: false,
    db: ldb,
    log: log,
    process: function (entry, cb) {
      setTimeout(function () {
        cb()
        base.close(function () {
          t.pass()
        })
      }, 3000)
    }
  })

  base.on('error', t.error)

  addEntries(log, 1)
})

test('timeout', function (t) {
  var log = new Log(nextName(), {
    db: leveldown,
    valueEncoding: 'json'
  })

  log.setMaxListeners(0)
  var ldb = levelup(nextName(), {
    db: leveldown,
    valueEncoding: 'json'
  })

  var base = SimpleBase({
    timeout: 100,
    db: ldb,
    log: log,
    process: function (entry, cb) {
      setTimeout(function () {
        cb()
      }, 200)
    }
  })

  base.on('error', function (err) {
    t.ok(/timed out/.test(err.message))
    base.close(t.end)
  })

  addEntries(log, 1)
})

test('basic', function (t) {
  t.timeoutAfter(5000)

  var paths = {
    db: 'simpledb.db',
    log: 'simpledblog.db'
  }

  cleanup()

  var log = new Log(paths.log, {
    db: leveldown,
    valueEncoding: 'json'
  })

  log.setMaxListeners(0)

  var numRead = 0
  var numDead = 10
  var numLive = 10
  var numEntries = numDead + numLive
  log.createReadStream({ live: true })
    .on('data', function () {
      if (++numRead === numDead) {
        restart()
      }
    })

  var expectedIds = []
  for (var i = 1; i <= numEntries; i++) {
    expectedIds.push(i)
  }

  // pre-add
  addEntries(log, numDead)

  var ldb
  var base
  var processedId = 1

  function restart (cb) {
    if (base) {
      return base.close(function (err) {
        if (err) throw err

        base = null
        restart(cb)
      })
    }

    ldb = levelup(paths.db, {
      db: leveldown,
      valueEncoding: 'json'
    })

    base = SimpleBase({
      db: ldb,
      log: log,
      process: processEntry
    })

    base.on('change', function (id) {
      t.equal(id, processedId++)
    })

    base.once('live', function () {
      addEntries(log, numLive, numRead)
      base.once('live', function () {
        cleanup()
        t.end()
      })
    })

    if (cb) cb()
  }

  var passed = 0
  var live

  function processEntry (entry, cb) {
    live = processedId > numDead
    if (!live && passed++ === 3) {
      // die a few times
      passed = 0
      return restart()
    }

    base.get('ids', function (err, ids) {
      if (err) {
        if (err.notFound) ids = []
        else throw err
      }

      ids.push(entry.id())
      base.batch([{ type: 'put', key: 'ids', value: ids }], cb)
    })
  }

  function cleanup () {
    if (base) base.close()
    if (log) log.close()
  }
})

function addEntries (log, num, offset) {
  offset = offset || 0
  for (var i = 1; i <= num; i++) {
    var entry = new LogEntry()
      .set('count', i + offset)

    log.append(entry)
  }
}
