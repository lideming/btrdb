# btrdb - B-tree DataBase

btrdb is a persistent, embedded NoSQL database engine written in TypeScript. It features a B-tree Copy-on-Write (CoW) architecture inspired by **btrfs**, enabling high-performance reads, crash safety, and instant point-in-time snapshots.

[![CI](https://github.com/lideming/btrdb/actions/workflows/ci.yml/badge.svg)](https://github.com/lideming/btrdb/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/lideming/btrdb/branch/main/graph/badge.svg?token=EWISTK2KWU)](https://codecov.io/gh/lideming/btrdb)

## ✨ Key Features

  * **Universal Runtime:** Runs on **Deno** (native) and **Node.js**.
  * **Hybrid Storage:** Supports both Key-Value sets and Document sets in the same database.
  * **Copy-on-Write (CoW):** Never overwrites data. Ensures database integrity and enables instant snapshots.
  * **Time Travel:** Create named snapshots and read data from any previous point in time.
  * **Advanced Querying:** SQL-like tagged template queries or functional query builders.
  * **Efficient Indexing:** Support for unique, compound, and computed indices.
  * **Transactions:** ACID compliance with concurrent reader isolation.
  * **btrdbfs:** Includes a FUSE filesystem implementation (mount your DB as a folder\!).

> **⚠️ Warning**: This project is under heavy development. The on-disk format and APIs are subject to change. Do not use in critical production environments yet.

-----

## 📦 Installation

### Deno

Import directly from `deno.land`:

```ts
import { Database } from "https://deno.land/x/btrdb/mod.ts";
```

### Node.js

Install via NPM:

```bash
npm install @yuuza/btrdb
```

Import in your project:

```js
import { Database } from "@yuuza/btrdb";
// or const { Database } = require("@yuuza/btrdb");
```

-----

## 🚀 Quick Start

```ts
import { Database } from "https://deno.land/x/btrdb/mod.ts"; // or @yuuza/btrdb

// 1. Open database (creates file if not exists)
const db = new Database();
await db.openFile("data.db");

// 2. Create a Document Set
const users = await db.createSet("users", "doc");

// 3. Insert Data
await users.insert({ username: "yuuza", role: "admin", active: true });
await users.insert({ username: "guest", role: "visitor", active: false });

// 4. Commit changes to disk
await db.commit();

// 5. Query Data
const admin = await users.query(query`role == ${"admin"}`);
console.log(admin); 

db.close();
```

-----

## 📖 Usage Guide

### 1\. Key-Value Store

Simple, persistent string-to-value storage.

```ts
const config = await db.createSet("config", "kv"); // 'kv' is default if omitted

// Set values
await config.set("theme", "dark");
await config.set("max_connections", 100);

// Get values
const theme = await config.get("theme"); // "dark"

// Iterate
await config.forEach((key, val) => {
  console.log(`${key}: ${val}`);
});
```

### 2\. Document Store & Indexing

Store JSON-like objects with automatic IDs and powerful indexing.

```ts
interface User {
  id: number;
  username: string;
  email: string;
  age: number;
}

const users = await db.createSet<User>("users", "doc");

// Define Indices (Critical for query performance)
await users.useIndexes({
  // Simple index
  age: (u) => u.age,
  
  // Unique index
  username: { unique: true, key: (u) => u.username },
  
  // Computed/Compound index
  active_adult: (u) => u.age >= 18 && u.active, 
});

// Upsert (Insert or Update by ID)
await users.upsert({ id: 1, username: "john", email: "john@test.com", age: 30 });
```

### 3\. Querying

btrdb provides a safe, tagged template literal syntax for queries.

**Operators:** `==`, `!=`, `>`, `<`, `<=`, `>=`, `AND`, `OR`, `NOT`, `SKIP`, `LIMIT`.

```ts
import { query } from "@yuuza/btrdb";

// Find users older than 20 excluding specific IDs
const results = await users.query(query`
  age > ${20} 
  AND NOT id == ${1}
  LIMIT ${10}
`);

// You can also use functional builders
import { AND, EQ, GT } from "@yuuza/btrdb";
const results2 = await users.query(
  AND(GT("age", 20), EQ("active", true))
);
```

### 4\. Transactions

Transactions guarantee atomicity. If an error occurs, changes are rolled back.

```ts
await db.runTransaction(async () => {
  const bank = await db.getSet("bank", "kv");
  
  const balance = await bank.get("user_1");
  if (balance < 100) throw new Error("Insufficient funds");
  
  await bank.set("user_1", balance - 100);
  await bank.set("user_2", (await bank.get("user_2")) + 100);
});
// Automatically commits here if successful
```

### 5\. Snapshots & Time Travel

Because btrdb is Copy-on-Write, creating snapshots is instant and cheap.

```ts
// 1. Commit current state
await db.commit();

// 2. Create a named snapshot
await db.createSnapshot("backup_v1");

// 3. Make destructive changes
await users.delete(1);
await db.commit();

// 4. Time Travel: Access the old data
const snapshot = await db.getSnapshot("backup_v1");
const oldUsers = snapshot.getSet("users", "doc");
console.log(await oldUsers.get(1)); // The user still exists here!
```

-----

## 🛠️ Advanced

### btrdbfs (FUSE Filesystem)

You can mount a btrdb database as a real folder on Linux/macOS using FUSE.
See [btrdbfs/README.md](https://www.google.com/search?q=./btrdbfs/README.md) for details.

```bash
# Install CLI
npm install -g @yuuza/btrdbfs

# Mount database
btrdbfs my_data.db ./mountpoint
```

### HTTP API

btrdb includes a basic HTTP server for remote access.

```ts
import { HttpApiServer } from "@yuuza/btrdb";
// ... open db ...
new HttpApiServer(db).serve(Deno.listen({ port: 1234 }));
```

-----

## 🧠 Architecture

btrdb uses a **B-tree Copy-on-Write** structure.

1.  **Pages:** Data is stored in fixed-size pages (default 4KB).
2.  **Immutability:** When a page is modified, it is not overwritten. Instead, it is copied to a new location, modified there, and the parent node is updated to point to the new address.
3.  **Root Tree:** This bubble-up effect reaches the "SuperPage" (Root). By keeping a reference to an old Root address, you effectively have a snapshot of the entire database at that moment.

![design-tree.svg](./docs/design-tree.svg)
![design-cow.svg](./docs/design-cow.svg)

## License

MIT License.