const bcode = require('bcode');
const fs = require('fs');
const mkdir = require('make-dir');
const lmdb = require('node-lmdb');

var en = new lmdb.Env();
let fspath;
let dbpath;
let db;

async function serveFile(path, res) {
  let txn = en.beginTxn()
  let contentType = txn.getString(db, path);
  txn.commit()
  console.log(contentType);
  if (contentType) {
    res.setHeader('Content-Type', contentType);
  }
  const filename = `${fspath}/${path}`;
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
        "/c/:hash": async function (req, res) {
          if (!/^[0-9A-Fa-f]{64}$/g.test(req.params.hash)) {
            return res.status(400).send('Invalid hash');
          };
          serveFile(`c/${req.params.hash}`, res);
        },
        "/b/:txId": async function (req, res) {
          if (!/^[0-9A-Fa-f]{64}$/g.test(req.params.txId)) {
            return res.status(400).send('Invalid txId');
          };
          serveFile(`b/${req.params.txId}`, res);
        },
        "/bcat/:txId": async function (req, res) {
          if (!/^[0-9A-Fa-f]{64}$/g.test(req.params.txId)) {
            return res.status(400).send('Invalid txId');
          };
          serveFile(`bcat/${req.params.txId}`, res);
        },
        "/:owner/:path": async function (req, res) {
          serveFile(`bitcom/${owner}/${req.params.path}`, res);
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
