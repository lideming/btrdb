import { DbDocSet, IDocument } from "./DbDocSet.ts";
import type { IDbDocSet } from "./DbDocSet.ts";
import { DbSet } from "./DbSet.ts";
import type { IDbSet } from "./DbSet.ts";
import { DocSetPage, SetPage, SuperPage } from "./page.ts";
import { InFileStorage, PageStorage } from "./storage.ts";
import { OneWriterLock } from "./util.ts";
import { KeyComparator, KValue, StringValue, UIntValue } from "./value.ts";
import { BugError } from "./errors.ts";

export interface EngineContext {
  storage: PageStorage;
}

export type DbSetType = keyof typeof _setTypeInfo;

export type DbObjectType = DbSetType | "snapshot";

const _setTypeInfo = {
  kv: { page: SetPage, dbset: DbSet },
  doc: { page: DocSetPage, dbset: DbDocSet },
};

export class DatabaseEngine implements EngineContext, Database {
  storage: PageStorage = undefined as any;
  private snapshot: SuperPage | null = null;
  autoCommit = false;
  autoCommitWaitWriting = true;
  defaultWaitWriting = true;

  commitLock = new OneWriterLock();

  get superPage() {
    return this.snapshot || this.storage.superPage;
  }

  async openFile(path: string, options?: { fsync?: InFileStorage["fsync"] }) {
    const stor = new InFileStorage();
    if (options) Object.assign(stor, options);
    await stor.openPath(path);
    await stor.init();
    this.storage = stor;
    // console.log('openFile():', this.superPage);
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
      await this.superPage!.insert(
        new KValue(new StringValue(prefixedName), new UIntValue(setPage.addr)),
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
      const superPage = this.superPage!;
      const r = await superPage.findKeyRecursive(
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
      const { action } = await this.superPage!.set(
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
      return (await this.superPage!.getAllValues()).map((x) => {
        return this._parsePrefixedName(x.key.str) as any;
      });
    } finally {
      lock.exitReader();
    }
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
      await this.superPage!.set(
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
      const result = await this.superPage!.findKeyRecursive(
        new KeyComparator(new StringValue(prefixedName)),
      );
      if (!result.found) return null;
      return await this._getSnapshotByAddr(result.val.value.val);
    } finally {
      lock.exitReader();
    }
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
    // console.log('==========COMMIT==========');
    const r = await this.storage.commit(waitWriting);
    // console.log('========END COMMIT========');
    return r;
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
    const type = prefix == "k"
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

  close() {
    this.storage.close();
  }
}

export function numberIdGenerator(lastId: number | null) {
  if (lastId == null) return 1;
  return lastId + 1;
}

export interface Database {
  autoCommit: boolean;
  autoCommitWaitWriting: boolean;
  defaultWaitWriting: boolean;

  openFile(
    path: string,
    options?: { fsync?: InFileStorage["fsync"] },
  ): Promise<void>;

  createSet(name: string, type?: "kv"): Promise<IDbSet>;
  createSet<T extends IDocument>(
    name: string,
    type: "doc",
  ): Promise<IDbDocSet<T>>;

  getSet(name: string, type?: "kv"): Promise<IDbSet | null>;
  getSet<T extends IDocument>(
    name: string,
    type: "doc",
  ): Promise<IDbDocSet<T> | null>;

  deleteSet(name: string, type: DbSetType): Promise<boolean>;

  getSetCount(): Promise<number>;
  getObjects(): Promise<{ name: string; type: DbObjectType }[]>;
  deleteObject(name: string, type: DbObjectType): Promise<boolean>;

  createSnapshot(name: string, overwrite?: boolean): Promise<void>;
  getSnapshot(name: string): Promise<Database | null>;
  getPrevCommit(): Promise<Database | null>;

  commit(): Promise<boolean>;
  waitWriting(): Promise<void>;
  close(): void;
}

export const Database: { new (): Database } = DatabaseEngine as any;
