# btrdb - B-tree DataBase

btrdb is a NoSQL database engine with Copy-on-Write inspired by btrfs.

[![CI](https://github.com/lideming/btrdb/actions/workflows/ci.yml/badge.svg)](https://github.com/lideming/btrdb/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/lideming/btrdb/branch/main/graph/badge.svg?token=EWISTK2KWU)](https://codecov.io/gh/lideming/btrdb)

- [x] Deno runtime
- [x] Node.js runtime
  - [x] Compatibility layer
  - [x] ES module bundle
  - [ ] CommonJS bundle
  - [x] Publish to NPM registry
- [x] B-Tree
- [x] Single file
- [x] Fully [Copy-on-Write](https://en.wikipedia.org/wiki/Copy-on-write) and
  [log-structured](https://en.wikipedia.org/wiki/Log-structured_file_system)
- [x] Good performance even written in pure TypeScript
  - [x] [Set 100k key-value pairs under 1.2s](https://github.com/lideming/btrdb/runs/3079877766?check_suite_focus=true#step:4:296)
  - [x] [Insert 100k documents under 3.3s](https://github.com/lideming/btrdb/runs/3079877766?check_suite_focus=true#step:4:320)
- [x] [Snapshots](#Use-snapshots)
  - [x] Named snapshots
- [x] [Key-Value sets](#Use-key-value-set)
- [x] [Document sets](#Use-document-set)
  - [x] Auto-id
  - [x] [Indexes](#Indexes)
  - [ ] Querying with `AND`, `OR`, `<`, `=`, `>` (?)
  - [ ] BSON instead of JSON on disk (?)
- [x] ACID
  - [x] Readers/writer lock
  - [x] Isolation with concurrent reader on snapshots
- [x] Auto-commit
- [ ] Client / Server (?)
- [ ] Replication (?)
- [ ] GC (?)

## ⚠️ Warning ⚠️

This project is just started. It's under heavy development!

The on-disk format structure and the API are NOT stable yet.

Please do NOT use it in any serious production.

## Usage

### Import the module

**Deno:**

```ts
import { Database } from "https://deno.land/x/btrdb@v0.1.2/mod.ts";
```

**Node.js:**

Install from NPM registry:

```
npm i @yuuza/btrdb
```

Import from ES module:

```js
import { Database } from "@yuuza/btrdb";
```

Import from CommonJS module:

```js
import("@yuuza/btrdb").then(({ Database }) => {
  // Use Database here...
});
```

### Create/open database file

```ts
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

#### Create set

```ts
interface User {
  id: number; // A property named "id" is required.
  username: string;
  status: "online" | "offline";
}

const userSet = await db.createSet<User>("users", "doc");
// Get the set or create if not exist.
```

#### Insert

```ts
await userSet.insert({ username: "yuuza", status: "offline" });
// Insert a new document, auto id when it's not specified.

console.info(await userSet.get(1));
// { id: 1, username: "yuuza", status: "offline" }

await db.commit();
// Commit to persist the changes.
```

#### Upsert

`upsert` will update the document with the same id, or insert a new document if
the id does not exist.

```ts
const user = await userSet.get(1);
user.status = "online";
// Get user and set its status

await userSet.upsert(user);
// Use upsert to apply the change.

console.info(await userSet.get(1));
// { id: 1, username: "yuuza", status: "online" }

await db.commit();
// Commit to persist the changes.
```

#### Indexes

```ts
interface User {
  id: number;
  username: string;
  status: "online" | "offline";
  role: "admin" | "user";
}

const userSet = await db.createSet<User>("users", "doc");

// Define indexes on the set and update indexes if needed.
userSet.useIndexes({
  status: (u) => u.status,
  // define "status" index, which indexing the value of user.status for each user in the set

  username: { unique: true, key: (u) => u.username },
  // define "username" unique index, which does not allow duplicated username.

  onlineAdmin: (u) => u.status == "online" && u.role == "admin",
  // define "onlineAdmin" index, the value is a computed boolean.
});

await userSet.insert({ username: "yuuza", status: "online", role: "admin" });
await userSet.insert({ username: "foo", status: "offline", role: "user" });
await userSet.insert({ username: "bar", status: "online", role: "admin" });
await db.commit();

// Get all online users
console.info(await userSet.findIndex("status", "online"));
// [
//   { username: "yuuza", status: "online", role: "user", id: 1 },
//   { username: "bar", status: "online", role: "admin", id: 3 }
// ]

// Get all users named 'yuuza'
console.info(await userSet.findIndex("username", "yuuza"));
// [ { username: "yuuza", status: "online", role: "user", id: 1 } ]

// Get all online admins
console.info(await userSet.findIndex("onlineAdmin", true));
// [ { username: "bar", status: "online", role: "admin", id: 3 } ]
```

### Use snapshots

Since btrdb uses CoW and never overwrites data on-disk, creating "snapshot" have
almost no cost.

```ts
const dataSet = await db.createSet("data");
await dataSet.set("foo", "bar");

// Commit then create a "named snapshot"
await db.createSnapshot("backup");

await dataSet.set("someone", "messed up your data!");
await dataSet.set("foo", "no bar!");
await db.commit();

// Get a "named snapshot".
const snap = await db.getSnapshot("backup");

// Read data from the snapshot
console.info(await snap.getSet("data").get("foo"));
```

Also, `db.getPrevCommit()` can be used to get previous commit as a snapshot.

### More example in the test code

See [test.ts](./test.ts).

## Design

(Outdated. To be added: documents tree, indexes tree, data pages, named
snapshots)

![design.svg](./docs/design.svg)

## License

MIT License
