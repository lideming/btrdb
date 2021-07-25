import { Database } from "@yuuza/btrdb";
import Fuse from "fuse-native";
const { stat } = Fuse;

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

const KIND_DIR = 1;
const KIND_FILE = 2;

const db = new Database();
await db.openFile("fs.db");
const inodes = await db.createSet("inodes", "doc");
inodes.useIndexes({
  "paid": (x) => x.paid,
  "paid_name": (x) => x.paid + "_" + x.name,
});

if (!(await inodes.get(0))) {
  const now = Date.now();
  await inodes.upsert({
    id: 0,
    paid: 0,
    kind: KIND_DIR,
    name: "",
    size: 0,
    ct: now,
    at: now,
    mt: now,
    mode: 16877,
    uid: 0,
    gid: 0,
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

async function getNodeFromPath(path) {
  /** @type {string[]} */
  const names = path.split("/").filter((x) => !!x);
  if (names.length === 0) return await inodes.get(0);
  /** @type {INode} */
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

const ops = {
  readdir: async function (path, cb) {
    const node = getNodeFromPath(path);
    const children = await inodes.findIndex("paid", node ? node.id : 0);
    return cb(null, children.map((x) => statFromInode(x)));
  },
  getattr: async function (path, cb) {
    const node = await getNodeFromPath(path);
    if (!node) return cb(Fuse.ENOENT);
    return cb(0, statFromInode(node));
  },
  open: function (path, flags, cb) {
    return cb(0, 42);
  },
  release: function (path, fd, cb) {
    return cb(0);
  },
  read: function (path, fd, buf, len, pos, cb) {
    var str = "hello world".slice(pos, pos + len);
    if (!str) return cb(0);
    buf.write(str);
    return cb(str.length);
  },
};

const fuse = new Fuse("mnt", ops, { debug: true });
fuse.mount(function (err) {
  if (err) console.error(err);
  //   fs.readFile(path.join(mnt, 'test'), function (err, buf) {
  //     // buf should be 'hello world'
  //   })
});
