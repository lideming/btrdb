这是一个为 Coding Agent（如 GitHub Copilot, Cursor AI, 或我）准备的 `AGENT.md` 文件。

这个文件的目的是**建立上下文索引**，帮助 AI 快速理解项目的核心架构、关键文件位置、开发规范以及“隐性知识”（比如 Copy-on-Write 的具体实现逻辑），从而提高代码生成和修改的准确率。

你可以将以下内容保存为项目根目录下的 `AGENT.md`。

---

# AGENT.md - Project Context for btrdb

## 1. Project Overview
**btrdb** is a persistent, embedded NoSQL database engine written in TypeScript/JavaScript.
* **Core Philosophy:** Inspired by **btrfs**, it uses a **B-tree Copy-on-Write (CoW)** architecture.
* **Key Features:** ACID transactions, Snapshots (Time Travel), Key-Value & Document stores, Custom binary serialization (`binval`).
* **Runtime:** Primarily targets **Deno**, but includes a build step to bundle for **Node.js**.
* **Sub-project:** `btrdbfs` (FUSE filesystem implementation on top of btrdb).

## 2. High-Level Architecture & Mental Model

### The "Forest of B-trees"
The database is structured as a collection of **B+ trees** rooted in a single `SuperPage`.
*   **Internal Nodes:** Store keys and pointers to children. Used for routing.
*   **Leaf Nodes:** Store keys and values/pointers.
*   **Traversal:** `findKeyRecursive` uses internal nodes for routing and leaf nodes for data retrieval. Range scans use efficient O(log N) tree traversal to find the next leaf node, ensuring robustness in Copy-on-Write environment.
1.  **SuperPage (Addr 0):** The only mutable page (conceptually). Points to the current valid `RootPage`.
2.  **RootPage:** The entry point for a snapshot. Contains pointers to:
    * **User Data Sets:** Mapped by name (e.g., `k_config`, `d_users`).
    * **RefTree:** Tracks reference counts for pages (crucial for CoW and cleaning up).
    * **FreeTree:** Tracks reclaimed/free pages.
3.  **Copy-on-Write (CoW) Flow:**
    * **Read:** Traverse the tree from `RootPage`.
    * **Write:** Never overwrite an on-disk page.
        1.  Load page into memory.
        2.  Call `getDirty()` -> Creates a new in-memory copy (or returns existing dirty copy).
        3.  Modify the dirty copy.
        4.  Recursively mark parents as dirty up to the `RootPage` and `SuperPage`.
        5.  On `commit()`, write all dirty pages to new disk addresses (append-only logic usually).

### Page Types (`src/pages/page.ts`)
* **SuperPage (0):** Header, pointers to Root.
* **RootPage (1):** Metadata, pointers to RefTree/FreeTree.
* **SetPage (3/5):** Head of a KV Set or Doc Set. Contains metadata (indexes, counts).
* **NodePage (2/4/6/8...):** Internal B-tree nodes.
* **DataPage (9):** Stores raw binary data (blobs/documents) that might span multiple pages.

### Serialization (`binval`)
* **Location:** `src/utils/binval.ts`
* **Format:** A custom, compact BSON-like binary format.
* **Usage:** Used for serializing keys and values before writing to pages. **Do not use `JSON.stringify` for disk storage.**

## 3. Directory Map & Key Files

| Path | Description |
| :--- | :--- |
| **`src/database/`** | **High-Level API**. `database.ts` (Entry), `DbSetBase.ts`, `transaction.ts`. |
| **`src/pages/`** | **Core Engine**. |
| `src/pages/page.ts` | Page definitions (`SuperPage`, `NodePage`, etc.) & Binary layout. |
| `src/pages/tree.ts` | B-tree logic: `findKey`, `insertAt`, `deleteAt`, `postChange` (split/merge). |
| `src/pages/storage.ts` | IO abstraction (`InFileStorage`, `InMemoryStorage`). Manages `dirtyPages` & caching. |
| **`src/query/`** | Query engine logic (`query.ts`) and Parser (`queryString.ts`). |
| **`src/utils/`** | Utilities. `binval.ts` (Serialization), `buffer.ts` (Binary writer/reader). |
| **`btrdbfs/`** | FUSE Filesystem implementation node wrapper. |
| **`nodeCompat/`** | Compatibility layer to make Deno code run on Node.js. |

## 4. Coding Conventions & Guidelines

### 1. Runtime Compatibility
* **Primary:** Deno.
* **Secondary:** Node.js (via `deno bundle`).
* **Rule:** Do not use Deno global namespaces (like `Deno.open`) directly in core logic. Use the wrapper `Runtime` object imported from `src/utils/runtime.ts`.

### 2. Async/Await
* Almost all storage operations are async (`readPage`, `commit`, `get`, `set`).
* Ensure locks (`OneWriterLock`) are correctly acquired and released in `finally` blocks.

### 3. Page Mutation (Crucial)
* **NEVER** modify a page instance that has `dirty === false` or a valid `addr`.
* **ALWAYS** call `page.getDirty()` before modifying. This ensures the CoW mechanism works.
* **Reference Counting:** When a node is modified, the `RefTree` must be updated during commit. This is largely handled in `storage.ts`, but be aware of it when manipulating tree structures.

### 4. Binary Data
* Use `src/utils/buffer.ts` (`Buffer` class) for reading/writing low-level bytes.
* Use `src/utils/value.ts` wrappers (`JSValue`, `StringValue`) for B-tree keys/values.

## 5. Development Tasks & Commands

* **Run Tests:** `dendeno run -A tests/test.ts run`
* **Build for Node:** `pnpm run build` (This runs the deno bundle command defined in `package.json`).
* **Debug:** `debug.ts` has toggles like `debug_allocate` and `debug_node`. Set them to `true` to trace internal engine behavior.

## 6. Known Complexities
* **Rebalancing:** The `deleteAt` logic in `src/pages/tree.ts` handles merging (underflow) and `postChange` handles splitting (overflow). This is the most fragile part of the B-tree implementation.
* **RefTree Updates:** Updating the reference count tree *while* calculating the reference counts for the commit itself is tricky. See `PageStorage.commitMark()`.
