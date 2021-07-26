# btrdbfs

Run a filesystem on btrdb!

Implements FUSE filesystem using Node.js with the binding
[fuse-native](https://github.com/fuse-friends/fuse-native). Only tested on
Linux.

- [x] create, open, mkdir, release
- [x] readdir, getattr
- [x] read, write
- [x] chmod, chown, utimens, truncate
- [x] unlink, rmdir, rename
- [x] statfs
- [x] link, symlink

## Performance

About 50 MB/s sequential read/write on i5-3320M with `big_writes` option.

The bottleneck is the CPU.

```
$ dd if=/dev/zero of=mnt/zeros bs=1M status=progress
893386752 bytes (893 MB, 852 MiB) copied, 15 s, 59.6 MB/s^C
853+0 records in
853+0 records out
894435328 bytes (894 MB, 853 MiB) copied, 15.0697 s, 59.4 MB/s
```

```
$ dd if=mnt/bigfile of=/dev/null bs=1M status=progress
1083179008 bytes (1.1 GB, 1.0 GiB) copied, 22 s, 49.2 MB/s
1078+0 records in
1078+0 records out
1130364928 bytes (1.1 GB, 1.1 GiB) copied, 22.9497 s, 49.3 MB/s
```

## Design

Using two document sets for inodes and extents.

### Inodes

For inodes, use "paid" index to get all items under the directory on
`readdir()`, and use "paid_name" index to find a node specific name under the
specific directory when finding an inode from path string.

```js
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

const inodes = await db.createSet("inodes", "doc");
inodes.useIndexes({
  "paid": (x) => x.paid,
  "paid_name": (x) => x.paid + "_" + x.name,
});
```

### Extents

Each "Extent" document saving a Uint8array as the extent data. The "ino_pos"
index is used to find an extent of an inode.

```js
/**
 * @typedef {Object} Extent
 * @property {number} id
 * @property {number} ino - inode id
 * @property {number} pos
 * @property {Uint8Array} data
 */

const EXTENT_SIZE = 4 * 4096;

const extents = await db.createSet("extents", "doc");
extents.useIndexes({
  "ino_pos": (x) => x.ino + "_" + x.pos,
});
```
