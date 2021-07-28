import Fuse from "fuse-native";
import { argv, getgid, getuid } from "process";
import { DB, EXTENT_SIZE, KIND_DIR, KIND_FILE, KIND_SYMLINK } from "./db.mjs";
import { getOps } from "./ops.mjs";

const dbFile = argv[2];
const mnt = argv[3];

const [gid, uid] = [getgid(), getuid()];

const db = new DB();
await db.openFile(dbFile);
await db.initialize(uid, gid);

const ops = getOps(db, uid, gid);
db.startCommitingTask();

const fuse = new Fuse(mnt, ops, {
  // debug: true,
  mkdir: true,
  force: true,
  bigWrites: true,
  // directIO: true,
});

fuse.mount(function (err) {
  if (err) console.error(err);
  else {
    console.info("Mounted on " + mnt);
  }
  //   fs.readFile(path.join(mnt, 'test'), function (err, buf) {
  //     // buf should be 'hello world'
  //   })
});
