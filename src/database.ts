import { DbDocSet } from "./DbDocSet.ts";
import { DbSet } from "./DbSet.ts";
import { DocSetPage, SetPage, SuperPage } from "./page.ts";
import {
  InFileStorage,
  InMemoryData,
  InMemoryStorage,
  PageStorage,
} from "./storage.ts";
import { OneWriterLock } from "./util.ts";
import { KeyComparator, KValue, StringValue, UIntValue } from "./value.ts";
import { BugError } from "./errors.ts";
import { Runtime } from "./runtime.ts";
import { Node } from "./tree.ts";
import type {
  Database as IDB,
  DbObjectType,
  DbSetType,
  Transaction,
} from "./btrdb.d.ts";
import { TransactionService } from "./transaction.ts";
export type Database = IDB;

export interface EngineContext {
  storage: PageStorage;
}

const _setTypeInfo = {
  kv: { page: SetPage, dbset: DbSet },
  doc: { page: DocSetPage, dbset: DbDocSet },
};

export class DatabaseEngine implements EngineContext, IDB {
  storage: PageStorage = undefined as any;
  transaction: TransactionService = new TransactionService(this);
  private snapshot: SuperPage | null = null;

  autoCommit = false;
  autoCommitWaitWriting = true;
  defaultWaitWriting = true;

  commitLock = new OneWriterLock();

  get superPage() {
    return this.snapshot || this.storage.superPage;
  }

  getTree() {
    return new Node(this.superPage!);
  }

  async openFile(path: string, options?: { fsync?: InFileStorage["fsync"] }) {
    const stor = new InFileStorage();
    if (options) Object.assign(stor, options);
    await stor.openPath(path);
    await stor.init();
    this.storage = stor;
    // console.log('openFile():', this.superPage);
  }

  async openMemory(data?: InMemoryData) {
    const stor = new InMemoryStorage(data ?? new InMemoryData());
    await stor.init();
    this.storage = stor;
  }

  static async openFile(...args: Parameters<DatabaseEngine["openFile"]>) {
    const db = new DatabaseEngine();
    await db.openFile(...args);
    return db;
  }

  static async openMemory(data: InMemoryData) {
    const db = new DatabaseEngine();
    await db.openMemory(data);
    return db;
  }

  async createSet(name: string, type: "kv"): Promise<DbSet>;
  async createSet(name: string, type: "doc"): Promise<DbDocSet>;
  async createSet(
    name: string,
    type: DbSetType = "kv",
  ): Promise<DbSet | DbDocSet> {
    let lockWriter = false;
    const lock = this.commitLock;
    await lock.enterReader();
    try {
      let set = await this._getSet(name, type as any, false);
      if (set) return set;

      await lock.enterWriterFromReader();
      lockWriter = true;

      // double check after entered lock writer, another writer may have create it before.
      set = await this._getSet(name, type as any, false);
      if (set) return set;

      const prefixedName = this._getPrefixedName(type, name);
      const { dbset: Ctordbset, page: Ctorpage } = _setTypeInfo[type];
      const setPage = new Ctorpage(this.storage).getDirty(true);
      setPage.prefixedName = prefixedName;
      const keyv = new StringValue(prefixedName);
      await this.getTree().set(
        new KeyComparator(keyv),
        new KValue(keyv, new UIntValue(setPage.addr)),
        "no-change",
      );
      this.superPage!.setCount++;
      if (this.autoCommit) await this._autoCommit();
      return new Ctordbset(setPage as any, this, name, !!this.snapshot) as any;
    } finally {
      if (lockWriter) lock.exitWriter();
      else lock.exitReader();
    }
  }

  async getSet(name: string, type: "kv"): Promise<DbSet | null>;
  async getSet(name: string, type: "doc"): Promise<DbDocSet | null>;
  getSet(
    name: string,
    type: DbSetType = "kv",
  ): Promise<DbSet | DbDocSet | null> {
    if (type as DbObjectType == "snapshot") {
      throw new Error("Cannot call getSet() with type 'snapshot'");
    }
    return this._getSet(name, type, true);
  }

  private async _getSet(
    name: string,
    type: DbSetType,
    useLock: boolean,
  ): Promise<DbSet | DbDocSet | null> {
    const lock = this.commitLock;
    if (useLock) await lock.enterReader();
    try {
      const prefixedName = this._getPrefixedName(type, name);
      const r = await this.getTree().findKeyRecursive(
        new KeyComparator(new StringValue(prefixedName)),
      );
      if (!r.found) return null;
      const { dbset: Ctordbset, page: Ctorpage } = _setTypeInfo[type];
      const setPage = await this.storage.readPage(
        r.val!.value.val,
        Ctorpage as any,
      ) as SetPage;
      setPage.prefixedName = prefixedName;
      return new Ctordbset(setPage as any, this, name, !!this.snapshot);
    } finally {
      if (useLock) lock.exitReader();
    }
  }

  async deleteSet(name: string, type: DbSetType): Promise<boolean> {
    return await this.deleteObject(name, type);
  }

  async deleteObject(name: string, type: DbObjectType): Promise<boolean> {
    const lock = this.commitLock;
    await lock.enterWriter();
    try {
      const prefixedName = this._getPrefixedName(type, name);
      const { action } = await this.getTree().set(
        new KeyComparator(new StringValue(prefixedName)),
        null,
        "no-change",
      );
      if (action == "removed" && type != "snapshot") {
        this.superPage!.setCount--;
        if (this.autoCommit) await this._autoCommit();
        return true;
      } else if (action == "noop") {
        return false;
      } else {
        throw new BugError("Unexpected return value: " + action);
      }
    } finally {
      lock.exitWriter();
    }
  }

  async getSetCount() {
    return this.superPage!.setCount;
  }

  async getObjects() {
    const lock = this.commitLock;
    await lock.enterReader();
    try {
      return await this._getObjectsNoLock();
    } finally {
      lock.exitReader();
    }
  }

  async _getObjectsNoLock() {
    return (await this.getTree().getAllValues()).map((x) => {
      return this._parsePrefixedName(x.key.str);
    });
  }

  async createSnapshot(name: string, overwrite = false) {
    const lock = this.commitLock;
    await lock.enterWriter();
    try {
      await this._autoCommit();

      const prefixedName = "s_" + name;
      const kv = new KValue(
        new StringValue(prefixedName),
        new UIntValue(this.storage.cleanSuperPage!.addr),
      );
      // console.info("[createSnapshot]", kv);
      await this.getTree().set(
        new KeyComparator(kv.key),
        kv,
        overwrite ? "can-change" : "no-change",
      );
      if (this.autoCommit) await this._autoCommit();
    } finally {
      lock.exitWriter();
    }
  }

  async getSnapshot(name: string) {
    const lock = this.commitLock;
    await lock.enterReader();
    try {
      const prefixedName = "s_" + name;
      const result = await this.getTree().findKeyRecursive(
        new KeyComparator(new StringValue(prefixedName)),
      );
      if (!result.found) return null;
      return await this._getSnapshotByAddr(result.val.value.val);
    } finally {
      lock.exitReader();
    }
  }

  runTransaction<T>(fn: Transaction<T>) {
    return this.transaction.run(fn);
  }

  async commit(waitWriting?: boolean) {
    await this.commitLock.enterWriter();
    try {
      return await this._commitNoLock(waitWriting ?? this.defaultWaitWriting);
    } finally {
      this.commitLock.exitWriter();
    }
  }

  async _commitNoLock(waitWriting: boolean) {
    // console.log("==========COMMIT==========");
    try {
      const r = await this.storage.commit(waitWriting);
      return r;
    } catch (err) {
      console.error("[commit error]", err);
      throw err;
    } finally {
      // console.log("========END COMMIT========");
    }
  }

  _autoCommit() {
    return this._commitNoLock(this.autoCommitWaitWriting);
  }

  async getPrevCommit() {
    if (!this.superPage?.prevSuperPageAddr) return null;
    return await this._getSnapshotByAddr(this.superPage.prevSuperPageAddr);
  }

  async _getSnapshotByAddr(addr: number) {
    var snapshot = new DatabaseEngine();
    snapshot.storage = this.storage;
    snapshot.snapshot = await this.storage.readPage(addr, SuperPage);
    return snapshot;
  }

  _getPrefixedName(type: DbObjectType, name: string) {
    const prefix = type == "kv"
      ? "k"
      : type == "doc"
      ? "d"
      : type == "snapshot"
      ? "s"
      : null;
    if (!prefix) throw new Error("Unknown type '" + type + "'");
    return prefix + "_" + name;
  }

  _parsePrefixedName(prefixedName: string) {
    const prefix = prefixedName[0];
    if (prefixedName[1] != "_") {
      throw new Error("Unexpected prefixedName '" + prefixedName + "'");
    }
    const type: DbObjectType | null = prefix == "k"
      ? "kv"
      : prefix == "d"
      ? "doc"
      : prefix == "s"
      ? "snapshot"
      : null;
    if (!type) throw new Error("Unknown prefix '" + prefix + "'");
    return { type, name: prefixedName.substr(2) };
  }

  waitWriting() {
    return this.storage.waitDeferWriting();
  }

  async rollback() {
    await this.commitLock.enterWriter();
    try {
      this.storage.rollback();
    } finally {
      this.commitLock.exitWriter();
    }
  }

  close() {
    this.storage.close();
  }

  async _cloneToNoLock(other: DatabaseEngine) {
    const sets = (await this._getObjectsNoLock())
      .filter((x) => x.type != "snapshot");
    for (const { name, type } of sets) {
      const oldSet = await this._getSet(name, type as any, false);
      const newSet = await other.createSet(name, type as any);
      await oldSet!._cloneTo(newSet as any);
    }
  }

  async rebuild() {
    // TODO: DbSet instances will still hold the old page.
    let lockWriter = false;
    await this.commitLock.enterReader();
    try {
      if (this.storage instanceof InFileStorage) {
        // Create temp database file
        const dbPath = (this.storage as InFileStorage).filePath!;
        const tempPath = dbPath + ".tmp";
        try {
          await Runtime.remove(tempPath);
        } catch {}
        const tempdb = await DatabaseEngine.openFile(tempPath);

        // Clone to temp database
        await this._cloneToNoLock(tempdb);
        await tempdb.commit(true);

        // Close current storage
        // Acquire the writer lock to ensure no one reading from the closed storage.
        await this.waitWriting();
        await this.commitLock.enterWriterFromReader();
        lockWriter = true;
        this.storage.close();

        // Replace the file and the storage instance.
        await Runtime.rename(tempPath, dbPath);
        this.storage = tempdb.storage;
      } else if (this.storage instanceof InMemoryStorage) {
        // Create temp database file
        const tempData = new InMemoryData();
        const tempdb = await DatabaseEngine.openMemory(tempData);

        // Clone to temp database
        await this._cloneToNoLock(tempdb);
        await tempdb.commit(true);

        // Close current storage
        // Acquire the writer lock to ensure no one reading from the closed storage.
        await this.waitWriting();
        await this.commitLock.enterWriterFromReader();
        lockWriter = true;
        this.storage.close();

        this.storage.data.pageBuffers = tempData.pageBuffers;
        this.storage = tempdb.storage;
      }
    } finally {
      if (lockWriter) this.commitLock.exitWriter();
      else this.commitLock.exitReader();
    }
  }

  async dump() {
    const obj: DbDump = {
      btrdbDumpVersion: "0",
      sets: [],
    };
    for (const { type, name } of (await this.getObjects())) {
      if (type == "snapshot") {
        // TODO: not supported yet
      } else if (type == "kv") {
        const set = await this.getSet(name, "kv");
        obj.sets.push({
          type,
          name,
          kvs: await set!.getAll(),
        });
      } else if (type == "doc") {
        const set = await this.getSet(name, "doc");
        obj.sets.push({
          type,
          name,
          indexes: await set!.getIndexes(),
          docs: await set!.getAll(),
        });
      } else {
        throw new Error(`Unknown type '${type}'`);
      }
    }
    return JSON.stringify(obj);
  }

  async import(data: string) {
    const obj = JSON.parse(data) as DbDump;
    if (obj.btrdbDumpVersion != "0") {
      throw new Error(`Unknown version '${obj.btrdbDumpVersion}'`);
    }
    for (const setData of obj.sets) {
      if (setData.type == "doc") {
        const set = await this.createSet(setData.name, "doc");
        for (const idx of Object.values(setData.indexes) as any) {
          idx.key = (0, eval)(idx.key);
        }
        await set.useIndexes(setData.indexes);
        for (const doc of setData.docs) {
          await set.insert(doc);
        }
      } else if (setData.type == "kv") {
        const set = await this.createSet(setData.name, "kv");
        for (const kv of setData.kvs) {
          await set.set(kv.key, kv.value);
        }
      }
    }
  }
}

interface DbDump {
  btrdbDumpVersion: "0";
  sets: any[];
}

export const Database: typeof IDB = DatabaseEngine as any;
