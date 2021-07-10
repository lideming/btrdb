# btrdb - B-tree DataBase

- [x] Deno runtime
- [ ] Node.js runtime
- [x] B-Tree
- [x] Fully [Copy-on-Write](https://en.wikipedia.org/wiki/Copy-on-write) and
  [log-structured](https://en.wikipedia.org/wiki/Log-structured_file_system)
- [x] Performance
  ([set 10k records in 150ms (single
  commit)](https://github.com/lideming/btrdb/runs/2995614665#step:4:261))
- [x] Snapshots
  - [ ] Named snapshots
- [x] Key-Value sets
- [x] Document sets
  - [x] Auto-id
  - [x] Indexes
  - [ ] BSON instead of JSON on disk (?)
- [x] ACID
  - [x] Readers/writer lock
  - [x] Isolation with concurrent reader on snapshots
- [ ] Client / Server (?)
- [ ] Replication (?)
- [ ] GC (?)
- [ ] Auto-commit (?)

[![codecov](https://codecov.io/gh/lideming/btrdb/branch/main/graph/badge.svg?token=EWISTK2KWU)](https://codecov.io/gh/lideming/btrdb)

## Usage

### ⚠️ Warning! ⚠️

This project is just started. It's under heavy development!

The on-disk format structure and the API are NOT stable yet.

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

### Use document set

**Create set**

```ts
interface User {
  id: number; // A property named "id" is required.
  username: string;
  status: "online" | "offline";
}

const configSet = await db.createSet<User>("users", "doc");
// Get the set or create if not exist.
```

**Insert**

```ts
await configSet.insert({ username: "yuuza", status: "offline" });
// Insert a new document, auto id when it's not specified.

console.info(await configSet.get(1));
// { id: 1, username: "yuuza", status: "offline" }

await configSet.insert({ username: "yuuza", status: "offline" });
// Insert a new document, auto id when it's not specified.

await db.commit();
// Commit to persist the changes.
```

**Upsert**

`upsert` will update the document with the same id, or insert a new document if
the id does not exist.

```ts
const user = await configSet.get(1);
user.status = "online";
// Get user and set its status

await configSet.upsert(user);
// Use upsert to apply the change.

console.info(await configSet.get(1));
// { id: 1, username: "yuuza", status: "online" }

await db.commit();
// Commit to persist the changes.
```

See also `test.ts`.

## Design

![design.svg](./docs/design.svg)

## License

MIT License
