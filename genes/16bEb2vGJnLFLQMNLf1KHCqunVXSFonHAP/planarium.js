const bcode = require('bcode');
const fs = require('fs');
const mkdir = require('make-dir');
const lmdb = require('node-lmdb');

var en = new lmdb.Env();
let fspath;
let dbpath;
let db;

async function serveFile(path, req, res) {
  let txn = en.beginTxn()
  let contentType = txn.getString(db, req.params.id);
  txn.commit()
  console.log(contentType);
  if (contentType) {
    res.setHeader('Content-Type', contentType);
  }
  fs.stat(filename, function(err, stat) {
    if(err) {
      return res.status(404).send('Not Found');
    }

    if (stat && stat.size) {
      res.setHeader('Content-Length', stat.size)
    }
    // 4. Send file
    let filestream = fs.createReadStream(filename)
    filestream.on("error", function(e) {
      res.status(500).send(e.message);
    });
    filestream.pipe(res);
  })
}

module.exports = {
  query: {
    web: {
      v: 3,
      q: { find: {}, limit: 10 }
    },
    api: {
      timeout: 50000,
      sort: {
        "blk.i": -1
      },
      concurrency: { aggregate: 7 },
      oncreate: async function (m) {
        fspath = m.fs.path
        dbpath = m.fs.path + "/lmdb"
        await mkdir(dbpath)
        en.open({ path: dbpath, mapSize: 2*1024*1024*1024, maxDbs: 3 });
        db = en.openDbi({ name: "mimetype", create: true })
      },
      routes: {
        "c/:hash": async function (req, res) {
          if (!/^[0-9A-Fa-f]{64}$/g.test(req.params.id)) {
            return res.status(400).send('Invalid id');
          };
          serveFile(`${fspath}/c/${req.params.hash}`)
        },
        "b/:txId": async function (req, res) {
          if (!/^[0-9A-Fa-f]{64}$/g.test(req.params.id)) {
            return res.status(400).send('Invalid id');
          };
          serveFile(`${fspath}/b/${req.params.txId}`)
        },
        "bcat/:txId": async function (req, res) {
          if (!/^[0-9A-Fa-f]{64}$/g.test(req.params.id)) {
            return res.status(400).send('Invalid id');
          };
          serveFile(`${fspath}/bcat/${req.params.txId}`)
        },
        ":owner/:path": async function (req, res) {
          serveFile(`${fspath}/${owner}/${req.params.path}`)
        }
      },
      log: true
    }
  },
  socket: {
    web: {
      v: 3,
      q: { find: {} }
    },
    api: {},
    topics: ["m"]
  },
  transform: {
    request: bcode.encode,
    response: bcode.decode
  },
  url: "mongodb://localhost:27020",
  port: 3002,
}
