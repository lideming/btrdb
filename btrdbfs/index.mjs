import { Database } from "@yuuza/btrdb";
import { readlink } from "fs";
import Fuse from "fuse-native";
import { getgid, getuid } from "process";

const DEBUG = false;

const FS_SIZE = 2 * 1024 * 1024 * 1024;

const EXTENT_SIZE = 4 * 4096;

const KIND_DIR = 1;
const KIND_FILE = 2;
const KIND_SYMLINK = 3;

const [gid, uid] = [getgid(), getuid()];

const db = new Database();
await db.openFile("testdata/fs.db");

/**
 * @typedef {Object} Inode
 * @property {number} id
 * @property {number} kind - KIND_*
 * @property {number} size
 * @property {number} ct - ctime
 * @property {number} at - atime
 * @property {number} mt - mtime
 * @property {number} mode
 * @property {number} uid
 * @property {number} gid
 * @property {string} ln - symlink
 */

const inodes = await db.createSet("inodes", "doc");

/**
 * @typedef {Object} Link
 * @property {number} id
 * @property {number} ino
 * @property {number} paid - parent dir inode
 * @property {string} name
 */
const links = await db.createSet("links", "doc");
await links.useIndexes({
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
await extents.useIndexes({
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
    kind: KIND_DIR,
    size: 0,
    ct: now,
    at: now,
    mt: now,
    mode: 16877,
    uid: uid,
    gid: gid,
  });
  await links.insert({
    id: null,
    ino: 1,
    paid: 0,
    name: "",
  });
}

const rootLink = {
  id: 1,
  ino: 1,
  paid: 0,
  name: "",
};

/** @param {Inode} node */
function statFromInode(node) {
  let mode = node.mode;
  if (node.kind === KIND_FILE) {
  } else if (node.kind === KIND_DIR) {
    mode |= 0o040000;
  } else if (node.kind === KIND_SYMLINK) {
    mode |= 0o120000;
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

/** @param {string} path */
function linkFromPath(path) {
  const names = namesFromPath(path);
  return linkFromNames(names);
}

/** @param {string[]} names */
async function linkFromNames(names) {
  if (names.length === 0) return rootLink;
  /** @type {Inode} */
  let link = null;
  for (const name of names) {
    const [nextLink] = await links.findIndex(
      "paid_name",
      (link ? link.ino : 1) + "_" + name,
    );
    if (!nextLink) return null;
    link = nextLink;
  }
  return link;
}

/** @param {string[]} names */
async function nodeFromNames(names) {
  if (names.length === 0) return tryGetCached(await inodes.get(1));
  const link = await linkFromNames(names);
  if (!link) return null;
  return await nodeFromIno(link.ino);
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

async function unlink(link) {
  await links.delete(link.id);
}

function flushNode(node) {
  markCleanNode(node);
  return inodes.upsert(node);
}

async function createInode(path, kind, mode, symlink) {
  const names = namesFromPath(path);
  const dirlink = await linkFromNames(dirOfNames(names));
  if (!dirlink) return Fuse.ENOENT;
  const now = Date.now();
  const inode = {
    id: null,
    kind: kind,
    size: symlink ? symlink.length : 0,
    ct: now,
    at: now,
    mt: now,
    mode: mode,
    uid: uid,
    gid: gid,
    ln: symlink,
  };
  await inodes.insert(inode);
  const link = {
    id: null,
    ino: inode.id,
    paid: dirlink.ino,
    name: names[names.length - 1],
  };
  await links.insert(link);
  return inode;
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
    let dirino;
    if (path == "/") {
      dirino = 1;
    } else {
      const link = await linkFromPath(path);
      if (!link) return cb(Fuse.ENOENT);
      dirino = link.ino;
    }
    const childLinks = await links.findIndex("paid", dirino);
    if (DEBUG) console.info("readdir", { node, childLinks });
    return cb(null, childLinks.map((x) => x.name));
  },
  getattr: async function (path, cb) {
    const node = await nodeFromPath(path);
    if (DEBUG) console.info("getattr", { path, deno });
    if (!node) return cb(Fuse.ENOENT);
    const stat = statFromInode(node);
    return cb(0, stat);
  },
  fgetattr(path, fd, cb) {
    const node = nodeFromFd(fd);
    if (DEBUG) console.info("fgetattr", { path, node });
    return cb(0, statFromInode(node));
  },
  create: async function (path, mode, cb) {
    if (DEBUG) console.info("create", path, mode);
    const inode = await createInode(path, KIND_FILE, mode, null);
    if (inode < 0) return cb(inode);
    const fd = createFd(inode);
    // console.info("create done", inode.id, path);
    cb(0, fd);
  },
  mkdir: async function (path, mode, cb) {
    if (DEBUG) console.info("mkdir", path, mode);
    const inode = await createInode(path, KIND_DIR, mode, null);
    if (inode < 0) return cb(inode);
    // console.info("mkdir done", inode.id, path);
    cb(0);
  },
  open: async function (path, flags, cb) {
    if (DEBUG) console.info("open", path, flags);
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    const fd = createFd(node);
    return cb(0, fd);
  },
  release: async function (path, fd, cb) {
    if (DEBUG) console.info("close", path, fd);
    const cached = fdMap.get(fd);
    fdMap.delete(fd);
    return cb(0);
  },
  /** @param {Buffer} buf */
  read: async function (path, fd, buf, len, pos, cb) {
    const node = nodeFromFd(fd);
    const ino = node.id;
    if (DEBUG) console.info("read", ino, pos, len);
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
    if (DEBUG) console.info("write " + len);
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
    // console.info({ path, atime, mtime });
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
    const link = await linkFromPath(path);
    if (!link) return cb(Fuse.ENOENT);
    await unlink(link);
    cb(0);
  },
  async rmdir(path, cb) {
    const link = await linkFromPath(path);
    if (!link) return cb(Fuse.ENOENT);
    await unlink(link);
    cb(0);
  },
  async rename(src, dest, cb) {
    let srcLink = await linkFromPath(src);
    if (!srcLink) return cb(Fuse.ENOENT);
    const destNames = namesFromPath(dest);
    const destFileName = destNames[destNames.length - 1];
    const destDirLink = await linkFromNames(dirOfNames(destNames));
    if (!destDirLink) return cb(Fuse.ENOENT);
    const [destLink] = await links.findIndex(
      "paid_name",
      destDirLink.ino + "_" + destFileName,
    );
    if (destLink) {
      await unlink(destLink);
    }
    srcLink.paid = destDirLink.ino;
    srcLink.name = destFileName;
    await links.upsert(srcLink);
    cb(0);
  },
  async link(src, dest, cb) {
    let srcLink = await linkFromPath(src);
    if (!srcLink) return cb(Fuse.ENOENT);
    const destNames = namesFromPath(dest);
    const destFileName = destNames[destNames.length - 1];
    const destDirLink = await linkFromNames(dirOfNames(destNames));
    if (!destDirLink) return cb(Fuse.ENOENT);
    const [destLink] = await links.findIndex(
      "paid_name",
      destDirLink.ino + "_" + destFileName,
    );
    if (destLink) {
      await unlink(destLink);
    }
    srcLink.paid = destDirLink.ino;
    srcLink.name = destFileName;
    await links.insert({
      id: null,
      ino: srcLink.ino,
      paid: destDirLink.ino,
      name: destFileName,
    });
    cb(0);
  },
  async symlink(src, dest, cb) {
    const inode = await createInode(dest, KIND_SYMLINK, 0o0777, src);
    if (inode < 0) return cb(inode);
    if (DEBUG) console.info("symlink done", inode);
    cb(0);
  },
  async readlink(path, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    cb(0, node.ln);
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
    let updatedInodes = 0;
    for (const [ino, cached] of inoMap) {
      if (cached.dirty) {
        // console.info("upsert inode", cached.inode);
        await inodes.upsert(cached.inode);
        cached.dirty = false;
        updatedInodes++;
      }
    }
    if (updatedInodes) {
      console.info("updated inodes: " + updatedInodes);
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
