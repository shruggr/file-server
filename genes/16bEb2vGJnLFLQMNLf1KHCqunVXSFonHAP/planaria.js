const mkdir = require('make-dir');
const fs = require('fs');
const crypto = require('crypto');
const lmdb = require('node-lmdb');
let en = new lmdb.Env();
let db;

async function readFile(path) {
  return new Promise((resolve, reject) => {
    fs.readFile(path, (err, data) => {
      if (err) return reject(err);
      resolve(data);
    });
  });
}

async function writeFile(path, buf) {
  return new Promise((resolve, reject) => {
    fs.writeFile(path, buf, (err) => {
      if (err && err.code != 'EEXIST') return reject(err);
      resolve();
    });
  });
}

async function linkFile(existingPath, newPath) {
  return new Promise((resolve, reject) => {
    fs.link(existingPath, newPath, (err) => {
      if (err && err.code != 'EEXIST') return reject(err);
      resolve();
    });
  });
}

async function saveMetadata(id, fileData) {
  let txn = en.beginTxn();
  txn.putString(db, id, fileData)
  txn.commit();
}

async function exists(path) {
  return new Promise((resolve) => {
    fs.access(path, fs.constants.F_OK, (err) => {
      if (err) return resolve(false);
      resolve(true);
    });
  })
}

async function processTransaction(m, txn) {
  const opRet = txn.out.find((out) => out.b0.op == 106);
  if (!opRet) return;
  let buffer;
  let fileData;
  let bTxId;
  try {
    switch (opRet.s1) {
      case '19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut':
        console.log(`Processing B: ${txn.tx.h}`);
        bTxId = txn.tx.h;
        buffer = Buffer.from(opRet.lb2 || opRet.b2 || '', 'base64');
        fileData = {
          info: 'b',
          contentType: opRet.s3,
          encoding: opRet.s4,
          filename: opRet.s5
        };
        break;
      case '15DHFxWZJT58f9nhyGnsRBqrgwK4W6h4Up':
        console.log(`Processing BCAT: ${txn.tx.h}`);
        bTxId = txn.tx.h;
        buffer = Buffer.alloc(0);
        fileData = {
          info: opRet.s2,
          contentType: opRet.s3,
          encoding: opRet.s4,
          filename: opRet.s5
        }

        let i = 7;
        let chunkId;
        while (chunkId = opRet[`lh${i}`] && /^[0-9A-Fa-f]{64}$/g.test(chunkId)) {
          const chunkPath = `${m.fs.path}/files/${chunkId}`;
          if (! await exists(chunkPath)) return;
          buffer = buffer.concat(await readFile(chunkPath));
          i++;
        }
        break;
      case '1ChDHzdd1H4wSjgGMHyndZm6qxEDGjqpJL':
        console.log(`Processing BCAT chunk: ${txn.tx.h}`);
        buffer = Buffer.from(opRet.lb2 || opRet.b2 || '', 'base64');
        break;
      default:
        return;
    }
  }
  catch (e) {
    return;
  }
  const hash = crypto.createHash('sha256').update(buffer).digest('hex');
  const filePath = `${m.fs.path}/files/${hash}`;
  console.log("Saving C: ", hash)
  await writeFile(filePath, buffer);

  if (bTxId) {
    console.log("Saving B/BCAT: ", bTxId)
    const bPath = `${m.fs.path}/files/${bTxId}`;
    await linkFile(filePath, bPath);
  }

  if (fileData) {
    await saveMetadata(hash, fileData);
    if (bTxId) await saveMetadata(bTxId, fileData);
  }
}

// initialize LMDB
var initLMDB = function(m) {
  en.open({
    path: m.fs.path + "/lmdb",
    mapSize: 2*1024*1024*1024,
    maxDbs: 3
  });
  db = en.openDbi({ name: "filedata", create: true })
}

module.exports = {
  planaria: '0.0.1',
  from: 566470,
  name: 'file-server',
  version: '0.0.1',
  description: 'file server for b, c, bcat, and ccat files',
  address: '16bEb2vGJnLFLQMNLf1KHCqunVXSFonHAP',
  index: {
    m: {
      keys: ['id'],
      unique: ['id']
    }
  },
  oncreate: async function (m) {
    await mkdir(m.fs.path + "/files");
    await mkdir(m.fs.path + "/lmdb");
    initLMDB(m);
  },
  onmempool: async function (m) {
    return processTransaction(m, m.input)
  },
  onblock: async function (m) {
    console.log("## onblock", "block height = ", m.input.block.info.height, "block hash =", m.input.block.info.hash, "txs =", m.input.block.info.tx.length);
    for (let input of m.input.block.items) {
      await processTransaction(m, input);
    }
    for (let input of m.input.mempool.items) {
      await processTransaction(m, input);
    }
  },
  onrestart: async function (m) { }
}