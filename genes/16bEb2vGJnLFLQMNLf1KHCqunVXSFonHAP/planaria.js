const crypto = require('crypto');
const fileType = require('file-type');
const fs = require('fs-extra');
const lmdb = require('node-lmdb');

let en = new lmdb.Env();
let db;
let fspath;

async function saveMetadata(id, contentType) {
  console.log(`Saving Metadata ${id}`);
  let txn = en.beginTxn();
  txn.putString(db, id, contentType);
  txn.commit();
}

async function detectBufferType(buffer) {
  try {
    let detection = fileType(buffer)
    if (detection) return detection.mime;
  } catch (e) { }
}

async function detectFileType(path) {
  try {
    const stream = await fileType.stream(fs.createReadStream(path));
    return stream.fileType && stream.fileType.mime;
  } catch (e) { }
}

async function streamFile(destPath, sourcePath, options) {
  return new Promise((resolve, reject) => {
    const write = fs.createWriteStream(destPath, options)
      .on('error', reject)
      .on('close', resolve);
    fs.createReadStream(sourcePath)
      .on('error', reject)
      .pipe(write);
  })
}

async function hashFile(filename) {
  const sum = crypto.createHash('sha256');
  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(filename);
    fileStream.on('error', reject);
    fileStream.on('data', (chunk) => {
      try {
        sum.update(chunk)
      } catch (err) {
        reject(err)
      }
    });
    fileStream.on('end', () => {
      resolve(sum.digest('hex'))
    })
  });
}

// async function saveC(buffer) {
//   const hash = crypto.createHash('sha256').update(buffer).digest('hex');
//   const filePath = `${fspath}/c/${hash}`;
//   if(await fs.pathExists(filePath)) return;

//   console.log("Saving C: ", filePath);
//   await fs.writeFile(filePath, buffer)
//   return hash;
// }

async function createCLink(source) {
  let hash = await hashFile(source);
  const filePath = `${fspath}/c/${hash}`;
  if(!await fs.pathExists(filePath)) {
    await fs.rename(source, filePath);
  }
  else {
    await fs.remove(source);
  };
  await fs.link(filePath, source);

  return hash;
}

async function saveB(txId, opRet) {
  if (await fs.pathExists(`${fspath}/b/${txId}`)) return;

  const data = opRet.lb2 || opRet.b2 || '';
  if(typeof data !== 'string') return;
  buffer = Buffer.from(data, 'base64');

  const fileData = {
    info: 'b',
    contentType: opRet.s3 || await detectBufferType(buffer),
    encoding: opRet.s4,
    filename: opRet.s5
  };

  console.log("Saving B: ", txId)
  const bPath = `${fspath}/b/${txId}`;
  await fs.writeFile(bPath, buffer);
  const hash = await createCLink(bPath);
  await saveMetadata(`b/${txId}`, fileData.contentType);
  await saveMetadata(`c/${hash}`, fileData.contentType);
}

async function saveChunk(txId, opRet) {
  const filepath = `${fspath}/chunks/${txId}`;
  if (await fs.pathExists(filepath)) return;

  console.log(`Saving Chunk: ${txId}`);
  const data = opRet.lb2 || opRet.b2 || '';
  if (typeof data !== 'string') return;
  buffer = Buffer.from(data, 'base64');

  await fs.writeFile(filepath, buffer);
}

async function saveBCat(bcat) {
  const destPath = `${fspath}/bcat/${bcat.txId}`;
  if (!bcat.chunks.length || await fs.pathExists(destPath)) return;
  for (let chunkId of bcat.chunks) {
    if (! await fs.pathExists(`${fspath}/chunks/${chunkId}`)) return;
  }

  console.log("Saving BCAT: ", bcat.txId)
  for (let chunkId of bcat.chunks) {
    await streamFile(destPath, `${fspath}/chunks/${chunkId}`, {flags: 'a'})
  }

  const contentType = bcat.fileData.contentType || await detectFileType(destPath)
  const hash = await createCLink(destPath);
  await saveMetadata(`bcat/${bcat.txId}`, contentType);
  await saveMetadata(`c/${hash}`, contentType);
}

async function saveBitcom(txId, owner, opRet) {
  const path = opRet.s5;
  if (!path || path.indexOf('..') > -1) return;

  const fullpath = `${fspath}/bitcom/${owner}/${path}`;
  await fs.ensureDir(fullpath.substring(0, fullpath.lastIndexOf('/')));

  switch (opRet.s2) {
    case 'echo':
      const data = opRet.ls3 || opRet.s3 || '';
      if (typeof data !== 'string') return;
      console.log("Saving Bitcom echo: ", txId)
      if (opRet.s4 == '>' || opRet.s4 == 'to') {
        await fs.writeFile(fullpath, Buffer.from(data));
      }
      else if (opRet.s4 === '>>') {
        await fs.appendFile(fullpath, Buffer.from(data));
      }

      break;
    case 'cat':
      // TODO: copy content type from source file.
      let sourceFile;
      const ref = new URL(opRet.s3);
      if (ref.protocol == 'b') {
        sourceFile = `${fspath}/b/${ref.hostname}`;
      }
      else if (ref.protocol == 'c') {
        sourceFile = `${fspath}/c/${ref.hostname}`;
      }
      else if (ref.protocol == 'bit' && ref.host == '19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut') {
        sourceFile = `${fspath}/b/${ref.pathname}`;
      }

      let flags;
      if (opRet.s4 == '>' || opRet.s4 == 'to') {
        flags = 'w';
      }
      else if (opRet.s4 === '>>') {
        flags = 'a';
      }
      else {
        return;
      }
      console.log("Saving Bitcom cat: ", txId)
      await streamFile(fullpath, sourceFile, { flags })

      break;
  }
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
          filter: { find: { chunks: txn.tx.h } }
        });
        if (bcat) {
          return saveBCat(bcat);
        }
        break;

      case '$':
        console.log(`Processing Bitcom: ${txn.tx.h}`);
        saveBitcom(txn.tx.h, txn.in[0].e.a, opRet);
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
var initLMDB = function (m) {
  en.open({
    path: m.fs.path + "/lmdb",
    mapSize: 2 * 1024 * 1024 * 1024,
    maxDbs: 3
  });
  db = en.openDbi({ name: "mimetype", create: true })
}

module.exports = {
  from: 570000,
  name: 'file-server',
  version: '0.0.3',
  description: 'file server for b, bcat, and bitcom files and associated C hashes',
  address: '16bEb2vGJnLFLQMNLf1KHCqunVXSFonHAP',
  index: {
    bcat: {
      keys: ['txId', 'chunks'],
      unique: ['txId']
    }
  },
  oncreate: async function (m) {
    fspath = m.fs.path;
    await fs.ensureDir(m.fs.path + "/chunks");
    await fs.ensureDir(m.fs.path + "/c");
    await fs.ensureDir(m.fs.path + "/b");
    await fs.ensureDir(m.fs.path + "/bcat");
    await fs.ensureDir(m.fs.path + "/bitcom");
    await fs.ensureDir(m.fs.path + "/lmdb");
    initLMDB(m);
  },
  onmempool: async function (m) {
    try {
      return processTransaction(m, m.input)
    }
    catch (e) {
      console.log(e);
      process.exit()
    }
  },
  onblock: async function (m) {
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
    catch (e) {
      console.log(e);
      process.exit()
    }
  },
  onrestart: async function (m) {
    fspath = m.fs.path;
    initLMDB(m);
  },
}