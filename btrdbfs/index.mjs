import { Database } from "@yuuza/btrdb";
import Fuse from "fuse-native";
import { getgid, getuid } from "process";

const FS_SIZE = 2 * 1024 * 1024 * 1024;

const EXTENT_SIZE = 4 * 4096;

const KIND_DIR = 1;
const KIND_FILE = 2;

const [gid, uid] = [getgid(), getuid()];

const db = new Database();
await db.openFile("testdata/fs.db");

/**
 * @typedef {Object} Inode
 * @property {number} id
 * @property {number} paid - parent id
 * @property {number} kind - KIND_*
 * @property {string} name
 * @property {number} size
 * @property {number} ct - ctime
 * @property {number} at - atime
 * @property {number} mt - mtime
 * @property {number} mode
 * @property {number} uid
 * @property {number} gid
 */

const inodes = await db.createSet("inodes", "doc");
inodes.useIndexes({
  "paid": (x) => x.paid,
  "paid_name": (x) => x.paid + "_" + x.name,
});

/**
 * @typedef {Object} Extent
 * @property {number} id
 * @property {number} ino - inode id
 * @property {number} pos
 * @property {Uint8Array} data
 */

const extents = await db.createSet("extents", "doc");
extents.useIndexes({
  "ino_pos": (x) => x.ino + "_" + x.pos,
});

// console.info(await inodes._dump());
// console.info(await inodes.getAll());
// console.info((await extents._dump()).indexes.ino_pos);
// throw '';

if (!(await inodes.get(1))) {
  // create the "root" folder
  const now = Date.now();
  await inodes.insert({
    id: null,
    paid: 0,
    kind: KIND_DIR,
    name: "",
    size: 0,
    ct: now,
    at: now,
    mt: now,
    mode: 16877,
    uid: uid,
    gid: gid,
  });
}

/** @param {Inode} node */
function statFromInode(node) {
  let mode = node.mode;
  if (node.kind === KIND_DIR) {
    mode |= 0o40000;
  }
  // console.info('stat', node.name, 'mode', (mode >>> 0).toString(2));
  return {
    mtime: node.mt,
    atime: node.at,
    ctime: node.ct,
    nlink: 1,
    size: node.size,
    mode: mode,
    uid: node.uid,
    gid: node.gid,
    ino: node.id,
    blksize: EXTENT_SIZE,
    blocks: Math.ceil(node.size / 512),
  };
}

/** @param {string} path */
function namesFromPath(path) {
  const names = path.split("/").filter((x) => !!x);
  return names;
}

/** @param {string[]} names */
function dirOfNames(names) {
  return names.slice(0, -1);
}

/** @param {string} path */
function nodeFromPath(path) {
  const names = namesFromPath(path);
  return nodeFromNames(names);
}

/** @param {string[]} names */
async function nodeFromNames(names) {
  if (names.length === 0) return tryGetCached(await inodes.get(1));
  /** @type {Inode} */
  let node = null;
  for (const name of names) {
    const [nextNode] = await inodes.findIndex(
      "paid_name",
      (node ? node.id : 1) + "_" + name,
    );
    if (!nextNode) return null;
    node = nextNode;
  }
  return tryGetCached(node);
}

function createFd(node) {
  const fd = nextFd++;
  let cached = inoMap.get(node.id);
  if (cached) {
    if (cached.inode !== node) {
      throw new Error("Expected having same inode in cache");
    }
  } else {
    cached = new Cached(node);
    inoMap.set(node.id, cached);
  }
  fdMap.set(fd, cached);
  return fd;
}

function nodeFromFd(fd) {
  const node = fdMap.get(fd).inode;
  return node;
}

function nodeFromIno(ino) {
  const cached = inoMap.get(ino);
  if (cached) return Promise.resolve(cached.inode);
  return inodes.get(ino);
}

function markDirtyNode(node) {
  let cached = inoMap.get(node.id);
  // console.info("markDirty", node.id);
  if (cached) {
    if (cached.inode !== node) {
      throw new Error("Expected having same inode in cache");
    }
    cached.dirty = true;
  } else {
    cached = new Cached(node);
    cached.dirty = true;
    inoMap.set(node.id, cached);
  }
}

function markCleanNode(node) {
  let cached = inoMap.get(node.id);
  if (cached) {
    if (cached.inode !== node) {
      throw new Error("Expected having same inode in cache");
    }
    cached.dirty = false;
  }
}

function tryGetCached(node) {
  const cached = inoMap.get(node.id);
  if (cached) return cached.inode;
  return node;
}

function unlinkNode(node) {
  node.paid = 0;
  return flushNode(node);
}

function flushNode(node) {
  markCleanNode(node);
  return inodes.upsert(node);
}

const zeros = new Uint8Array(EXTENT_SIZE);

class Cached {
  /** @param {Inode} inode */
  constructor(inode) {
    this.inode = inode;
    this.dirty = false;
  }
}

// Maps fd to inode
/** @type {Map<number, Cached>} */
const fdMap = new Map();

// Maps inode id to inode
/** @type {Map<number, Cached>} */
const inoMap = new Map();

let nextFd = 1;

const ops = {
  readdir: async function (path, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    const children = await inodes.findIndex("paid", node.id);
    // console.info({ node, children });
    return cb(null, children.map((x) => x.name));
  },
  getattr: async function (path, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    // console.info("getattr", { path, node });
    return cb(0, statFromInode(node));
  },
  fgetattr(path, fd, cb) {
    const node = nodeFromFd(fd);
    // console.info("fgetattr", { path, node });
    return cb(0, statFromInode(node));
  },
  create: async function (path, mode, cb) {
    console.info("create", { path, mode });
    const names = namesFromPath(path);
    const dirnode = await nodeFromNames(dirOfNames(names));
    if (!dirnode) return cb(Fuse.ENOENT);
    const now = Date.now();
    const inode = {
      id: null,
      paid: dirnode.id,
      kind: KIND_FILE,
      name: names[names.length - 1],
      size: 0,
      ct: now,
      at: now,
      mt: now,
      mode: mode,
      uid: uid,
      gid: gid,
    };
    await inodes.insert(inode);
    const fd = createFd(inode);
    console.info("create done", inode);
    cb(0, fd);
  },
  mkdir: async function (path, mode, cb) {
    console.info("mkdir", { path, mode });
    const names = namesFromPath(path);
    const dirnode = await nodeFromNames(dirOfNames(names));
    if (!dirnode) return cb(Fuse.ENOENT);
    const now = Date.now();
    const inode = {
      id: null,
      paid: dirnode.id,
      kind: KIND_DIR,
      name: names[names.length - 1],
      size: 0,
      ct: now,
      at: now,
      mt: now,
      mode: mode,
      uid: uid,
      gid: gid,
    };
    await inodes.insert(inode);
    console.info("mkdir done", inode);
    cb(0);
  },
  open: async function (path, flags, cb) {
    console.info("open", { path, flags });
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    const fd = createFd(node);
    return cb(0, fd);
  },
  release: async function (path, fd, cb) {
    const cached = fdMap.get(fd);
    fdMap.delete(fd);
    return cb(0);
  },
  /** @param {Buffer} buf */
  read: async function (path, fd, buf, len, pos, cb) {
    const node = nodeFromFd(fd);
    const ino = node.id;
    // console.info("read", ino, pos, len);
    let haveRead = 0;
    while (len > 0 && pos < node.size) {
      const extpos = Math.floor(pos / EXTENT_SIZE);
      const [extent] = await extents.findIndex("ino_pos", ino + "_" + extpos);
      let dataLen = 0;
      if (extent) {
        const data = extent.data;
        dataLen = data.byteLength;
        const extoffset = pos % EXTENT_SIZE;
        const tocopy = Math.min(dataLen - extoffset, len);
        // console.info("read", {ino, pos, len, extpos, extoffset, tocopy});
        buf.set(
          (extoffset || tocopy < dataLen)
            ? data.subarray(extoffset, extoffset + tocopy)
            : data,
          haveRead,
        );
        haveRead += tocopy;
        pos += tocopy;
        len -= tocopy;
      }
      if (len > 0 && dataLen < EXTENT_SIZE) {
        const copyZeros = Math.min(EXTENT_SIZE - dataLen, len);
        buf.set(zeros.subarray(0, copyZeros), haveRead);
        haveRead += copyZeros;
        pos += copyZeros;
        len -= copyZeros;
      }
    }
    return cb(haveRead);
  },
  /** @param {Buffer} buf */
  write: async function (path, fd, buf, len, pos, cb) {
    // console.info('write ' + len);
    // return cb(len);
    const node = nodeFromFd(fd);
    const ino = node.id;
    // console.info({ino, pos, len});
    let haveWritten = 0;
    while (len > 0) {
      const extpos = Math.floor(pos / EXTENT_SIZE);
      const extoffset = pos % EXTENT_SIZE;
      const tocopy = Math.min(EXTENT_SIZE - extoffset, len);
      // console.info({haveWritten, extpos, extoffset, tocopy});
      /** @type {[Extent]} */
      let [extent] = await extents.findIndex("ino_pos", ino + "_" + extpos);
      if (!extent) {
        extent = {
          id: null,
          ino: ino,
          pos: extpos,
          data: null,
        };
      }
      if (tocopy == EXTENT_SIZE) {
        extent.data = buf.slice(haveWritten, haveWritten + tocopy);
      } else {
        if (!extent.data || extent.data.byteLength < extoffset + tocopy) {
          const newData = new Uint8Array(extoffset + tocopy);
          if (extent.data) {
            newData.set(extent.data);
          }
          extent.data = newData;
        }
        extent.data.set(
          buf.slice(haveWritten, haveWritten + tocopy),
          extoffset,
        );
      }
      if (!extent.id) await extents.insert(extent);
      else await extents.upsert(extent);
      haveWritten += tocopy;
      pos += tocopy;
      len -= tocopy;
    }
    if (pos > node.size) {
      node.size = pos;
      markDirtyNode(node);
      // console.info("file extended", ino, node.size);
    }
    // console.info("write done", ino, haveWritten);
    return cb(haveWritten);
  },
  async chown(path, uid, gid, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    node.uid = uid;
    node.gid = gid;
    markDirtyNode(node);
    cb(0);
  },
  async chmod(path, mode, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    node.mode = mode;
    markDirtyNode(node);
    cb(0);
  },
  async utimens(path, atime, mtime, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    console.info({ path, atime, mtime });
    node.at = atime;
    node.mt = mtime;
    markDirtyNode(node);
    cb(0);
  },
  async truncate(path, size, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    node.size = size;
    markDirtyNode(node);
    cb(0);
  },
  async ftruncate(path, fd, size, cb) {
    const node = nodeFromFd(fd);
    node.size = size;
    markDirtyNode(node);
    cb(0);
  },
  async unlink(path, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    await unlinkNode(node);
    cb(0);
  },
  async rmdir(path, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    await unlinkNode(node);
    cb(0);
  },
  async rename(src, dest, cb) {
    let srcNode = await nodeFromPath(src);
    if (!srcNode) return cb(Fuse.ENOENT);
    srcNode = tryGetCached(srcNode);
    const destNames = namesFromPath(dest);
    const destFileName = destNames[destNames.length - 1];
    const destDirNode = await nodeFromNames(dirOfNames(destNames));
    if (!destDirNode) return cb(Fuse.ENOENT);
    const [destNode] = await inodes.findIndex(
      "paid_name",
      destDirNode.id + "_" + destFileName,
    );
    if (destNode) {
      await unlinkNode(tryGetCached(destNode));
    }
    srcNode.paid = destDirNode.id;
    srcNode.name = destFileName;
    await flushNode(srcNode);
    cb(0);
  },
  statfs(path, cb) {
    const maxFiles = Number.MAX_SAFE_INTEGER;
    const usedFiles = inodes.count;
    const usedBlocks = Math.ceil(db.storage.nextAddr * 4096 / EXTENT_SIZE);
    const totalBlocks = Math.ceil(Math.max(FS_SIZE / EXTENT_SIZE, usedBlocks));
    cb(0, {
      bsize: EXTENT_SIZE,
      frsize: EXTENT_SIZE,
      blocks: totalBlocks,
      bfree: totalBlocks - usedBlocks,
      bavail: totalBlocks - usedBlocks,
      files: maxFiles,
      ffree: maxFiles - usedFiles,
      favail: maxFiles - usedFiles,
      namemax: 1024,
      // fsid: 1234,
      // flag: 1000000,
    });
  },
};

// console.info(await inodes.getAll());

// await ops.create("/a", 1234, () => {});
// await ops.create("/b", 1234, () => {});
// await ops.create("/c", 1234, () => {});

// await db.commit();

(async function () {
  while (true) {
    await new Promise((r) => setTimeout(r, 5000));
    for (const [ino, cached] of inoMap) {
      if (cached.dirty) {
        console.info("upsert inode", cached.inode);
        await inodes.upsert(cached.inode);
        cached.dirty = false;
      }
    }
    if (await db.commit(false)) {
      // db.storage.cache.clear();
      // console.info(await inodes.getAll());
      console.info("commited.");
    } else {
      console.info("nothing to commit.");
    }
  }
})();

const fuse = new Fuse("mnt", ops, {
  // debug: true,
  mkdir: true,
  force: true,
  bigWrites: true,
  // directIO: true,
});
fuse.mount(function (err) {
  if (err) console.error(err);
  //   fs.readFile(path.join(mnt, 'test'), function (err, buf) {
  //     // buf should be 'hello world'
  //   })
});
