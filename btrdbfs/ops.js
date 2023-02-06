//@ts-check
import Fuse from "@yuuza/fuse-native";
import { EXTENT_SIZE, KIND_DIR, KIND_FILE, KIND_SYMLINK } from "./db.js";

const DEBUG = false;

const FS_SIZE = 2 * 1024 * 1024 * 1024;

const zeros = new Uint8Array(EXTENT_SIZE);

/**
 * @param {import("./db").DB} db
 */
export function getOps(db, uid, gid) {
  return {
    readdir: async function (path, cb) {
      let dirino;
      if (path == "/") {
        dirino = 1;
      } else {
        const link = await db.linkFromPath(path);
        if (!link) return cb(Fuse.ENOENT);
        dirino = link.ino;
      }
      const childLinks = await db.links.findIndex("paid", dirino);
      if (DEBUG) console.info("readdir", { dirino, childLinks });
      return cb(null, childLinks.map((x) => x.name));
    },
    getattr: async function (path, cb) {
      const node = await db.nodeFromPath(path);
      if (DEBUG) console.info("getattr", { path, node });
      if (!node) return cb(Fuse.ENOENT);
      const stat = db.statFromInode(node);
      return cb(0, stat);
    },
    fgetattr(path, fd, cb) {
      const node = db.nodeFromFd(fd);
      if (DEBUG) console.info("fgetattr", { path, node });
      return cb(0, db.statFromInode(node));
    },
    create: async function (path, mode, cb) {
      if (DEBUG) console.info("create", path, mode);
      const inode = await db.createInode(path, KIND_FILE, mode, uid, gid, null);
      if (inode < 0) return cb(inode);
      const fd = db.createFd(inode);
      // console.info("create done", inode.id, path);
      cb(0, fd);
    },
    mkdir: async function (path, mode, cb) {
      if (DEBUG) console.info("mkdir", path, mode);
      const inode = await db.createInode(path, KIND_DIR, mode, uid, gid, null);
      if (inode < 0) return cb(inode);
      // console.info("mkdir done", inode.id, path);
      cb(0);
    },
    open: async function (path, flags, cb) {
      if (DEBUG) console.info("open", path, flags);
      const node = await db.nodeFromPath(path);
      if (!node) return cb(Fuse.ENOENT);
      const fd = db.createFd(node);
      return cb(0, fd);
    },
    release: async function (path, fd, cb) {
      if (DEBUG) console.info("close", path, fd);
      db.deleteFd(fd);
      return cb(0);
    },
    /** @param {Buffer} buf */
    read: async function (path, fd, buf, len, pos, cb) {
      const node = db.nodeFromFd(fd);
      const ino = node.id;
      if (DEBUG) console.info("read", ino, pos, len);
      let haveRead = 0;
      while (len > 0 && pos < node.size) {
        const extpos = Math.floor(pos / EXTENT_SIZE);
        const [extent] = await db.extents.findIndex(
          "ino_pos",
          ino + "_" + extpos,
        );
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
      const node = db.nodeFromFd(fd);
      const ino = node.id;
      // console.info({ino, pos, len});
      let haveWritten = 0;
      while (len > 0) {
        const extpos = Math.floor(pos / EXTENT_SIZE);
        const extoffset = pos % EXTENT_SIZE;
        const tocopy = Math.min(EXTENT_SIZE - extoffset, len);
        // console.info({haveWritten, extpos, extoffset, tocopy});
        /** @type {[import("./db.js").Extent]} */
        let [extent] = await db.extents.findIndex(
          "ino_pos",
          ino + "_" + extpos,
        );
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
        if (!extent.id) await db.extents.insert(extent);
        else await db.extents.upsert(extent);
        haveWritten += tocopy;
        pos += tocopy;
        len -= tocopy;
      }
      if (pos > node.size) {
        node.size = pos;
        db.markDirtyNode(node);
        // console.info("file extended", ino, node.size);
      }
      // console.info("write done", ino, haveWritten);
      return cb(haveWritten);
    },
    async chown(path, uid, gid, cb) {
      const node = await db.nodeFromPath(path);
      if (!node) return cb(Fuse.ENOENT);
      node.uid = uid;
      node.gid = gid;
      db.markDirtyNode(node);
      cb(0);
    },
    async chmod(path, mode, cb) {
      const node = await db.nodeFromPath(path);
      if (!node) return cb(Fuse.ENOENT);
      node.mode = mode;
      db.markDirtyNode(node);
      cb(0);
    },
    async utimens(path, atime, mtime, cb) {
      const node = await db.nodeFromPath(path);
      if (!node) return cb(Fuse.ENOENT);
      // console.info({ path, atime, mtime });
      node.at = atime;
      node.mt = mtime;
      db.markDirtyNode(node);
      cb(0);
    },
    async truncate(path, size, cb) {
      const node = await db.nodeFromPath(path);
      if (!node) return cb(Fuse.ENOENT);
      node.size = size;
      db.markDirtyNode(node);
      cb(0);
    },
    async ftruncate(path, fd, size, cb) {
      const node = db.nodeFromFd(fd);
      node.size = size;
      db.markDirtyNode(node);
      cb(0);
    },
    async unlink(path, cb) {
      const link = await db.linkFromPath(path);
      if (!link) return cb(Fuse.ENOENT);
      await db.unlink(link);
      const { ino } = link;
      const links = await db.linksFromIno(ino);
      if (!links.length) {
        const exts = await db.extents.findIndex("ino", ino);
        for (const it of exts) {
          await db.extents.delete(it.id);
        }
        console.info("deleted", exts.length, "extends", path);
      }
      cb(0);
    },
    async rmdir(path, cb) {
      const link = await db.linkFromPath(path);
      if (!link) return cb(Fuse.ENOENT);
      await db.unlink(link);
      cb(0);
    },
    async rename(src, dest, cb) {
      let srcLink = await db.linkFromPath(src);
      if (!srcLink) return cb(Fuse.ENOENT);
      const destNames = db.namesFromPath(dest);
      const destFileName = destNames[destNames.length - 1];
      const destDirLink = await db.linkFromNames(db.dirOfNames(destNames));
      if (!destDirLink) return cb(Fuse.ENOENT);
      const [destLink] = await db.links.findIndex(
        "paid_name",
        destDirLink.ino + "_" + destFileName,
      );
      if (destLink) {
        await db.unlink(destLink);
      }
      srcLink.paid = destDirLink.ino;
      srcLink.name = destFileName;
      await db.updateLink(srcLink);
      cb(0);
    },
    async link(src, dest, cb) {
      let srcLink = await db.linkFromPath(src);
      if (!srcLink) return cb(Fuse.ENOENT);
      const destNames = db.namesFromPath(dest);
      const destFileName = destNames[destNames.length - 1];
      const destDirLink = await db.linkFromNames(db.dirOfNames(destNames));
      if (!destDirLink) return cb(Fuse.ENOENT);
      const [destLink] = await db.links.findIndex(
        "paid_name",
        destDirLink.ino + "_" + destFileName,
      );
      if (destLink) {
        await db.unlink(destLink);
      }
      srcLink.paid = destDirLink.ino;
      srcLink.name = destFileName;
      await db.links.insert({
        id: null,
        ino: srcLink.ino,
        paid: destDirLink.ino,
        name: destFileName,
      });
      cb(0);
    },
    async symlink(src, dest, cb) {
      const inode = await db.createInode(
        dest,
        KIND_SYMLINK,
        0o0777,
        uid,
        gid,
        src,
      );
      if (inode < 0) return cb(inode);
      if (DEBUG) console.info("symlink done", inode);
      cb(0);
    },
    async readlink(path, cb) {
      const node = await db.nodeFromPath(path);
      if (!node) return cb(Fuse.ENOENT);
      cb(0, node.ln);
    },
    statfs(path, cb) {
      const maxFiles = Number.MAX_SAFE_INTEGER;
      const usedFiles = db.inodes.count;
      const usedBlocks = Math.ceil(db.db.storage.nextAddr * 4096 / EXTENT_SIZE);
      const totalBlocks = Math.ceil(
        Math.max(FS_SIZE / EXTENT_SIZE, usedBlocks),
      );
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
}
