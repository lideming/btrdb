# btrdb - B-tree DataBase

- [x] Deno runtime
- [ ] Node.js runtime
- [x] B-Tree
- [x] Fully [Copy-on-Write](https://en.wikipedia.org/wiki/Copy-on-write) and
  [log-structured](https://en.wikipedia.org/wiki/Log-structured_file_system)
- [x] Snapshots
  - [ ] Named snapshots
- [x] Key-Value sets
- [x] Document sets
  - [ ] Indexes
  - [ ] BSON instead of JSON on disk (?)
- [x] AC<del>I</del>D
  - [x] Isolation with concurrent reader
  - [ ] Concurrent writer (?)
- [ ] Client / Server (?)
- [ ] Replication (?)
- [ ] GC (?)
- [ ] Auto-commit (?)

## Usage

### ⚠️ Warning! ⚠️

This project is just started. It's under heavy development!

The on-disk structre and the API are NOT stable yet.

Please do NOT use it in any serious production.

### Create/open database file

```ts
import { Database } from "https://github.com/lideming/btrdb/raw/main/mod.ts";

const db = new Database();
await db.openFile("data.db");
// Will create new database if the file doesn't exist.
```

### Use key-value set

```ts
const configSet = await db.createSet("config");
// Get the set or create if not exist.

await configSet.set("username", "yuuza");
console.info(await configSet.get("username")); // "yuuza"

await db.commit();
// Commit to persist the changes.
```

See also `test.ts`.

## Design

![design.svg](./docs/design.svg)

## License

MIT License
