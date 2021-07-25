import { Database } from "@yuuza/btrdb";
import Fuse from "fuse-native";
import { getgid, getuid } from "process";

// TODO: save data as blob (waiting for btrfs)

const EXTENT_SIZE = 4096;

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
// console.info(await extents._dump());
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
  return {
    mtime: new Date(node.mt),
    atime: new Date(node.at),
    ctime: new Date(node.ct),
    nlink: 1,
    size: node.size,
    mode: node.kind == KIND_DIR ? 16877 : node.mode,
    uid: node.uid,
    gid: node.gid,
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
  if (names.length === 0) return await inodes.get(1);
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
  return node;
}

const zeros = new Uint8Array(EXTENT_SIZE);

class Opened {
  /** @param {Inode} inode */
  constructor(inode) {
    this.inode = inode;
    this.dirty = false;
  }
}

// Maps fd to inode
/** @type {Map<number, Opened>} */
const fdMap = new Map();

// Maps inode id to inode
/** @type {Map<number, Opened>} */
const inoMap = new Map();

let nextFd = 1;

const ops = {
  readdir: async function (path, cb) {
    const node = await nodeFromPath(path);
    const children = await inodes.findIndex("paid", node.id);
    // console.info({ node, children });
    return cb(null, children.map((x) => x.name));
  },
  getattr: async function (path, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    // console.info('getattr', {path, node});
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
    const fd = nextFd++;
    fdMap.set(fd, new Opened(inode));
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
    const fd = nextFd++;
    fdMap.set(fd, new Opened(node));
    return cb(0, fd);
  },
  release: async function (path, fd, cb) {
    const op = fdMap.get(fd);
    fdMap.delete(fd);
    if (op.dirty) {
      await inodes.upsert(op.inode);
    }
    return cb(0);
  },
  /** @param {Buffer} buf */
  read: async function (path, fd, buf, len, pos, cb) {
    const op = fdMap.get(fd);
    const ino = op.inode.id;
    const node = op.inode;
    console.info("read", { ino, pos, len });
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
    const op = fdMap.get(fd);
    const node = op.inode;
    const ino = node.id;
    let haveWritten = 0;
    while (len > 0) {
      const extpos = Math.floor(pos / EXTENT_SIZE);
      const extoffset = pos % EXTENT_SIZE;
      const tocopy = Math.min(EXTENT_SIZE - extoffset, len);
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
        extent.data = buf.slice(haveWritten, tocopy);
      } else {
        if (!extent.data || extent.data.byteLength < extoffset + tocopy) {
          const newData = new Uint8Array(extoffset + tocopy);
          if (extent.data) {
            newData.set(extent.data);
          }
          extent.data = newData;
        }
        extent.data.set(buf.slice(haveWritten, tocopy), extoffset);
      }
      if (!extent.id) await extents.insert(extent);
      else await extents.upsert(extent);
      haveWritten += tocopy;
      pos += tocopy;
      len -= tocopy;
    }
    if (pos > node.size) {
      node.size = pos;
      await inodes.upsert(node);
      // console.info("file extended", ino, node.size);
    }
    // console.info("write done", ino, haveWritten);
    return cb(haveWritten);
  },
  async chown(path, uid, gid, cb) {
    const node = await nodeFromPath(path);
    node.uid = uid;
    node.gid = gid;
    await inodes.upsert(node);
    cb(0);
  },
  async chmod(path, mode, cb) {
    const node = await nodeFromPath(path);
    node.mode = mode;
    await inodes.upsert(node);
    cb(0);
  },
  async truncate(path, size, cb) {
    const node = await nodeFromPath(path);
    node.size = size;
    await inodes.upsert(node);
    cb(0);
  },
  async ftruncate(path, fd, size, cb) {
    const ino = fdMap.get(fd);
    const node = await inodes.get(ino);
    node.size = size;
    await inodes.upsert(node);
    cb(0);
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
    if (await db.commit(true)) {
      // db.storage.cache.clear();
      // console.info(await inodes.getAll());
      console.info("commited.");
    } else {
      console.info("nothing to commit.");
    }
  }
})();

const fuse = new Fuse("mnt", ops, {
  debug: false,
  mkdir: true,
  force: true,
  bigWrites: true,
});
fuse.mount(function (err) {
  if (err) console.error(err);
  //   fs.readFile(path.join(mnt, 'test'), function (err, buf) {
  //     // buf should be 'hello world'
  //   })
});
