//@ts-check
import { Database } from "@yuuza/btrdb";
import Fuse from "@yuuza/fuse-native";

export const KIND_DIR = 1;
export const KIND_FILE = 2;
export const KIND_SYMLINK = 3;

export const EXTENT_SIZE = 4 * 4096;

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


/**
 * @typedef {Object} Link
 * @property {number} id
 * @property {number} ino
 * @property {number} paid - parent dir inode
 * @property {string} name
 */


/**
 * @typedef {Object} Extent
 * @property {number} id
 * @property {number} ino - inode id
 * @property {number} pos
 * @property {Uint8Array} data
 */

const rootLink = {
  id: 1,
  ino: 1,
  paid: 0,
  name: "",
};

export class DB {
  constructor() {
    this.db = new Database();

    this.inodes = undefined;
    this.links = undefined;
    this.extents = undefined;

    // Maps fd to inode
    /** @type {Map<number, Cached>} */
    this.fdMap = new Map();

    // Maps inode id to inode
    /** @type {Map<number, Cached>} */
    this.inoMap = new Map();

    this.nextFd = 1;
  }

  async openFile(path) {
    await this.db.openFile(path);
    // await this.db.openFile("testdata/fs.db");

    /** @type {import("@yuuza/btrdb").IDbDocSet<Inode>} */
    // @ts-ignore
    this.inodes = await this.db.createSet("inodes", "doc");

    /** @type {import("@yuuza/btrdb").IDbDocSet<Link>} */
    // @ts-ignore
    this.links = await this.db.createSet("links", "doc");
    await this.links.useIndexes({
      "paid": (x) => x.paid,
      "paid_name": (x) => x.paid + "_" + x.name,
    });

    /** @type {import("@yuuza/btrdb").IDbDocSet<Extent>} */
    // @ts-ignore
    this.extents = await this.db.createSet("extents", "doc");
    await this.extents.useIndexes({
      "ino_pos": (x) => x.ino + "_" + x.pos,
    });
  }

  async initialize(uid, gid) {
    // console.info(await this.inodes._dump());
    // console.info(await this.inodes.getAll());
    // console.info((await extents._dump()).indexes.ino_pos);
    // throw '';

    if (!(await this.inodes.get(1))) {
      // create the "root" folder
      const now = Date.now();
      await this.inodes.insert({
        id: null,
        kind: KIND_DIR,
        size: 0,
        ct: now,
        at: now,
        mt: now,
        mode: 16877,
        uid: uid,
        gid: gid,
        ln: null,
      });
      await this.links.insert({
        id: null,
        ino: 1,
        paid: 0,
        name: "",
      });
    }
  }

  async startCommitingTask() {
    while (true) {
      await new Promise((r) => setTimeout(r, 5000));
      let updatedInodes = 0;
      for (const [ino, cached] of this.inoMap) {
        if (cached.dirty) {
          // console.info("upsert inode", cached.inode);
          await this.inodes.upsert(cached.inode);
          cached.dirty = false;
          updatedInodes++;
        }
      }
      if (updatedInodes) {
        console.info("updated inodes: " + updatedInodes);
      }
      if (await this.db.commit(false)) {
        // db.storage.cache.clear();
        // console.info(await inodes.getAll());
        console.info("commited.");
      } else {
        console.info("nothing to commit.");
      }
    }
  }

  async startStatTask() {
    // @ts-ignore
    const prevCouner = { ...this.db.storage.counter };
    while (true) {
      await new Promise((r) => setTimeout(r, 2000));
      // @ts-ignore
      const storage = this.db.storage;
      const counter = storage.perfCounter;
      console.info({
        ...counter,
        dirty: storage.nextAddr - 1 - storage.cleanAddr,
        writeback: storage.cleanAddr - storage.writtenAddr,
      });
      Object.assign(prevCouner, counter);
    }
  }

  /** @param {Inode} node */
  statFromInode(node) {
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
  namesFromPath(path) {
    const names = path.split("/").filter((x) => !!x);
    return names;
  }

  /** @param {string[]} names */
  dirOfNames(names) {
    return names.slice(0, -1);
  }

  /** @param {string} path */
  nodeFromPath(path) {
    const names = this.namesFromPath(path);
    return this.nodeFromNames(names);
  }

  /** @param {string} path */
  linkFromPath(path) {
    const names = this.namesFromPath(path);
    return this.linkFromNames(names);
  }

  /** @param {string[]} names */
  async linkFromNames(names) {
    if (names.length === 0) return rootLink;
    /** @type {Link} */
    let link = null;
    for (const name of names) {
      const [nextLink] = await this.links.findIndex(
        "paid_name",
        (link ? link.ino : 1) + "_" + name,
      );
      if (!nextLink) return null;
      link = nextLink;
    }
    return link;
  }

  /** @param {string[]} names */
  async nodeFromNames(names) {
    if (names.length === 0) return this.tryGetCached(await this.inodes.get(1));
    const link = await this.linkFromNames(names);
    if (!link) return null;
    return await this.nodeFromIno(link.ino);
  }

  createFd(node) {
    const fd = this.nextFd++;
    let cached = this.inoMap.get(node.id);
    if (cached) {
      if (cached.inode !== node) {
        throw new Error("Expected having same inode in cache");
      }
    } else {
      cached = new Cached(node);
      this.inoMap.set(node.id, cached);
    }
    this.fdMap.set(fd, cached);
    return fd;
  }

  deleteFd(fd) {
    this.fdMap.delete(fd);
  }

  nodeFromFd(fd) {
    const node = this.fdMap.get(fd).inode;
    return node;
  }

  nodeFromIno(ino) {
    const cached = this.inoMap.get(ino);
    if (cached) return Promise.resolve(cached.inode);
    return this.inodes.get(ino);
  }

  markDirtyNode(node) {
    let cached = this.inoMap.get(node.id);
    // console.info("markDirty", node.id);
    if (cached) {
      if (cached.inode !== node) {
        throw new Error("Expected having same inode in cache");
      }
      cached.dirty = true;
    } else {
      cached = new Cached(node);
      cached.dirty = true;
      this.inoMap.set(node.id, cached);
    }
  }

  markCleanNode(node) {
    let cached = this.inoMap.get(node.id);
    if (cached) {
      if (cached.inode !== node) {
        throw new Error("Expected having same inode in cache");
      }
      cached.dirty = false;
    }
  }

  tryGetCached(node) {
    const cached = this.inoMap.get(node.id);
    if (cached) return cached.inode;
    return node;
  }

  updateLink(link) {
    return this.links.upsert(link);
  }

  async unlink(link) {
    await this.links.delete(link.id);
  }

  flushNode(node) {
    this.markCleanNode(node);
    return this.inodes.upsert(node);
  }

  async createInode(path, kind, mode, uid, gid, symlink) {
    const names = this.namesFromPath(path);
    const dirlink = await this.linkFromNames(this.dirOfNames(names));
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
    await this.inodes.insert(inode);
    const link = {
      id: null,
      ino: inode.id,
      paid: dirlink.ino,
      name: names[names.length - 1],
    };
    await this.links.insert(link);
    return inode;
  }
}

class Cached {
  /** @param {Inode} inode */
  constructor(inode) {
    this.inode = inode;
    this.dirty = false;
  }
}
