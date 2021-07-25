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
 * @property {string} data - base64
 */

const extents = await db.createSet("extents", "doc");
extents.useIndexes({
  "ino_pos": (x) => x.ino + "_" + x.pos,
});

if (!(await inodes.get(0))) {
  // create the "root" folder
  const now = Date.now();
  await inodes.upsert({
    id: 0,
    paid: -1,
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
    mode: node.mode,
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
  if (names.length === 0) return await inodes.get(0);
  /** @type {Inode} */
  let node = null;
  for (const name of names) {
    const [nextNode] = await inodes.findIndex(
      "paid_name",
      (node ? node.id : 0) + "_" + name,
    );
    if (!nextNode) return null;
    node = nextNode;
  }
  return node;
}

// Maps fd to inode id
/** @type {Map<number, number>} */
const fdMap = new Map();

let nextFd = 1;

const ops = {
  readdir: async function (path, cb) {
    const node = await nodeFromPath(path);
    const children = await inodes.findIndex("paid", node.id);
    console.info({ node, children });
    return cb(null, children.map((x) => x.name));
  },
  getattr: async function (path, cb) {
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    return cb(0, statFromInode(node));
  },
  create: async function (path, mode, cb) {
    console.info("create", { path, mode });
    const names = namesFromPath(path);
    const dirnode = await nodeFromNames(dirOfNames(names));
    if (!dirnode) return cb(Fuse.ENOENT);
    const now = Date.now();
    await inodes.insert({
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
    });
    cb(0);
  },
  open: async function (path, flags, cb) {
    console.info("open", { path, flags });
    const node = await nodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    const fd = nextFd++;
    fdMap.set(fd, node.id);
    return cb(0, fd);
  },
  release: function (path, fd, cb) {
    fdMap.delete(fd);
    return cb(0);
  },
  read: function (path, fd, buf, len, pos, cb) {
    const ino = fdMap.get(fd);
    // const node = await inodes.get(ino);
    let haveRead = 0;
    while (len > 0) {
      const extpos = Math.floor(pos / EXTENT_SIZE);
      const [extent] = await extents.findIndex("ino_pos", ino + "_" + extpos);
      // TODO
    }
    buf.write(str);
    return cb(str.length);
  },
};

const fuse = new Fuse("mnt", ops, { debug: true, mkdir: true, force: true });
fuse.mount(function (err) {
  if (err) console.error(err);
  //   fs.readFile(path.join(mnt, 'test'), function (err, buf) {
  //     // buf should be 'hello world'
  //   })
});
