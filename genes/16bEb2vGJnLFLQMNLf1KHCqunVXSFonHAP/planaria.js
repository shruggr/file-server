const mkdir = require('make-dir');
const fs = require('fs');
const crypto = require('crypto');
const fileType = require('file-type');
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

async function saveMetadata(id, contentType) {
  console.log(`Saving Metadata ${id}`);
  let txn = en.beginTxn();
  txn.putString(db, id, contentType);
  txn.commit();
}

async function detectFileType(buffer) {
  try {
    let detection = fileType(buffer)
    if (detection) return detection.mime;
  } catch (e) {}
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
  if(await exists(`${fspath}/file/${txId}`)) return;

  const data = opRet.lb2 || opRet.b2 || '';
  if(typeof data !== 'string') return;
  buffer = Buffer.from(data, 'base64');

  let contentType = await detectFileType(buffer);
  const fileData = {
    info: 'b',
    contentType: contentType || opRet.s3,
    encoding: opRet.s4,
    filename: opRet.s5
  };
  const hash = await save(buffer);
  await saveMetadata(hash, fileData.contentType);

  const filepath = `${fspath}/files/${hash}`;
  console.log("Saving B: ", txId)
  const bPath = `${fspath}/files/${txId}`;
  await linkFile(filepath, bPath);
  await saveMetadata(txId, fileData.contentType);
}

async function saveChunk(txId, opRet) {
  const filepath = `${fspath}/chunks/${txId}`;
  if(await exists(filepath)) return;

  console.log(`Saving Chunk: ${txId}`);
  const data = opRet.lb2 || opRet.b2 || '';
  if(typeof data !== 'string') return;
  buffer = Buffer.from(data, 'base64');

  await writeFile(filepath, buffer);
}

async function saveBCat(bcat) {
  if(await exists(`${fspath}/file/${bcat.txId}`)) return;
  for(let chunkId of bcat.chunks) {
    if (! await exists(`${fspath}/chunks/${chunkId}`)) return;
  }

  let buffer = Buffer.alloc(0);
  for(let chunkId of bcat.chunks) {
    let chunk = await readFile(`${fspath}/chunks/${chunkId}`);
    buffer = Buffer.concat([buffer, chunk], buffer.length + chunk.length);
  }
  let contentType = await detectFileType(buffer);
  if(contentType) {
    bcat.fileData.contentType = contentType;
  }
  const hash = await save(buffer);
  await saveMetadata(hash, bcat.fileData.contentType);

  const filepath = `${fspath}/files/${hash}`;
  console.log("Saving BCAT: ", bcat.txId)
  const bPath = `${fspath}/files/${bcat.txId}`;
  await linkFile(filepath, bPath);
  await saveMetadata(bcat.txId, bcat.fileData.contentType);
}

async function processTransaction(m, txn) {
  const opRet = txn.out.find((out) => out.b0.op == 106);
  if (!opRet) return;
  let bcat;
  try {
    switch (opRet.s1) {
      case '19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut':
        console.log(`Processing B: ${txn.tx.h}`);
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
  db = en.openDbi({ name: "mimetype", create: true })
}

module.exports = {
  planaria: '0.0.1',
  from: 570000,
  name: 'file-server',
  version: '0.0.1',
  description: 'file server for b, c, and bcat files',
  address: '16bEb2vGJnLFLQMNLf1KHCqunVXSFonHAP',
  index: {
    bcat: {
      keys: ['txId', 'chunks'],
      unique: ['txId']
    }
  },
  oncreate: async function (m) {
    fspath = m.fs.path;
    await mkdir(m.fs.path + "/chunks");
    await mkdir(m.fs.path + "/files");
    await mkdir(m.fs.path + "/lmdb");
    initLMDB(m);
  },
  onrestart: async function(m) {
    fspath = m.fs.path;
    initLMDB(m);
  },
  onmempool: async function (m) {
    fspath = m.fs.path;
    try {
      return processTransaction(m, m.input)
    }
    catch(e) {
      console.log(e);
      process.exit()
    }
  },
  onblock: async function (m) {
    fspath = m.fs.path;
    console.log(`FSPATH: ${fspath}`);
    console.log("## onblock", "block height = ", m.input.block.info.height, "block hash =", m.input.block.info.hash, "txs =", m.input.block.info.tx.length);
    try {
      for (let input of m.input.block.items) {
        await processTransaction(m, input);
      }
      for (let input of m.input.mempool.items) {
        await processTransaction(m, input);
      }
    }
    catch(e) {
      console.log(e);
      process.exit()
    }
  },
  onrestart: async function (m) { }
}