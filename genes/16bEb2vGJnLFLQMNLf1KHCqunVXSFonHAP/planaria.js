const mkdir = require('make-dir');
const fs = require('fs');
const crypto = require('crypto');
const lmdb = require('node-lmdb');
let en = new lmdb.Env();
let db;
let fspath;

async function readFile(path) {
  return new Promise((resolve, reject) => {
    fs.readFile(path, (err, data) => {
      if (err) return reject(err);
      resolve(data);
    });
  });
}

async function writeFile(path, buf) {
  console.log(`Writing ${path}`);
  return new Promise((resolve, reject) => {
    fs.writeFile(path, buf, (err) => {
      if (err && err.code != 'EEXIST') return reject(err);
      resolve();
    });
  });
}

async function linkFile(existingPath, newPath) {
  console.log(`Linking ${existingPath} ${newPath}`);
  return new Promise((resolve, reject) => {
    fs.link(existingPath, newPath, (err) => {
      if (err && err.code != 'EEXIST') return reject(err);
      resolve();
    });
  });
}

async function saveMetadata(id, fileData) {
  console.log(`Saving Metadata ${id}`);
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

async function save(buffer) {
  const hash = crypto.createHash('sha256').update(buffer).digest('hex');
  const filePath = `${fspath}/files/${hash}`;
  console.log("Saving C: ", filePath);
  await writeFile(filePath, buffer);
  return hash;
}

async function saveB(txId, opRet) {
  console.log(`Processing B: ${txId}`);
  buffer = Buffer.from(opRet.lb2 || opRet.b2 || '', 'base64');
  const fileData = {
    info: 'b',
    contentType: opRet.s3,
    encoding: opRet.s4,
    filename: opRet.s5
  };
  const hash = await save(buffer);
  await saveMetadata(hash, fileData);

  const filepath = `${fspath}/files/${hash}`;
  console.log("Saving B: ", txId)
  const bPath = `${fspath}/files/${txId}`;
  await linkFile(filepath, bPath);
  await saveMetadata(txId, fileData);
}

async function saveChunk(txId, opRet) {
  const filepath = `${fspath}/chunks/${bcat.txId}`;
  // if(await exists(filepath)) return;

  console.log(`Saving Chunk: ${txId}`);
  const buffer = Buffer.from(opRet.lb2 || opRet.b2 || '', 'base64');
  await writeFile(filepath, buffer);
}

async function saveBCat(bcat) {
  if(await exists(`${fspath}/file/${bcat.txId}`)) return;
  for(let chunkId of bcat.chunks) {
    if (! await exists(`${fspath}/chunks/${chunkId}`)) return;
  }

  let buffer = Buffer.alloc(0);
  for(let chunkId of bcat.chunks) {
    buffer = buffer.concat(await readFile(`${fspath}/chunks/${chunkId}`));
  }

  const hash = await save(buffer);
  await saveMetadata(hash, bcat.fileData);

  const filepath = `${fspath}/files/${hash}`;
  console.log("Saving BCAT: ", txId)
  const bPath = `${fspath}/files/${txId}`;
  await linkFile(filepath, bPath);
  await saveMetadata(txId, bcat.fileData);
}

async function processTransaction(m, txn) {
  const opRet = txn.out.find((out) => out.b0.op == 106);
  if (!opRet) return;
  let bcat;
  try {
    switch (opRet.s1) {
      case '19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut':
        return saveB(txn.tx.h, opRet);
        break;
      case '15DHFxWZJT58f9nhyGnsRBqrgwK4W6h4Up':
        console.log(`Processing BCAT: ${txn.tx.h}`);
        bcat = {
          txId: txn.tx.h,
          chunks: [],
          fileData: {
            info: opRet.s2,
            contentType: opRet.s3,
            encoding: opRet.s4,
            filename: opRet.s5
          }
        };

        let i = 7;
        let chunkId;
        while (chunkId = opRet[`h${i}`]) {
          // if(!/^[0-9A-Fa-f]{64}$/g.test(chunkId)) return;
          bcat.chunks.push(chunkId);
          i++;
        }

        console.log(bcat);
        await m.state.create({
          name: 'bcat',
          data: bcat
        }).catch(function (e) {
          if (e.code != 11000) {
            process.exit();
          }
        });
        console.log('Saving BCAT index: ' + txn.tx.h);
        return saveBCat(bcat);
        break;
      case '1ChDHzdd1H4wSjgGMHyndZm6qxEDGjqpJL':
        console.log(`Processing Chunk: ${txn.tx.h}`);
        await saveChunk(txn.tx.h, opRet);
        [bcat] = await m.state.read({
          name: 'bcat',
          filter: {find: {chunks: txn.tx.h}}
        });
        if(bcat) {
          return saveBCat(bcat);
        }
        break;
      default:
        return;
    }
  }
  catch (e) {
    return;
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
  from: 570000,
  name: 'file-server',
  version: '0.0.1',
  description: 'file server for b, c, bcat, and ccat files',
  address: '16bEb2vGJnLFLQMNLf1KHCqunVXSFonHAP',
  index: {
    bcat: {
      keys: ['txId', 'chunks'],
      unique: ['txId']
    }
  },
  oncreate: async function (m) {
    await mkdir(m.fs.path + "/chunks");
    await mkdir(m.fs.path + "/files");
    await mkdir(m.fs.path + "/lmdb");
    initLMDB(m);
  },
  onmempool: async function (m) {
    fspath = m.fs.path;
    return processTransaction(m, m.input)
  },
  onblock: async function (m) {
    fspath = m.fs.path;
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