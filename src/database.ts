import { DbDocSet, IDocument } from "./DbDocSet.ts";
import type { IDbDocSet } from "./DbDocSet.ts";
import { DbSet } from "./DbSet.ts";
import type { IDbSet } from "./DbSet.ts";
import { DocSetPage, RecordsPage, SetPage, SuperPage } from "./page.ts";
import { InFileStorage, PageStorage } from "./storage.ts";
import { OneWriterLock } from "./util.ts";
import { JSONValue, KValue, StringValue, UIntValue } from "./value.ts";
import { BugError } from "./errors.ts";

export interface EngineContext {
  storage: PageStorage;
}

export type DbSetType = keyof typeof _setTypeInfo;

const _setTypeInfo = {
  kv: { page: SetPage, dbset: DbSet },
  doc: { page: DocSetPage, dbset: DbDocSet },
};

export class DatabaseEngine implements EngineContext {
  storage: PageStorage = undefined as any;
  private snapshot: SuperPage | null = null;

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

      // double check
      set = await this._getSet(name, type as any, false);
      if (set) return set;

      const { dbset: Ctordbset, page: Ctorpage } = _setTypeInfo[type];
      const setPage = new Ctorpage(this.storage).getDirty(true);
      setPage.name = name;
      await this.superPage!.insert(
        new KValue(new StringValue(name), new UIntValue(setPage.addr)),
      );
      this.superPage!.setCount++;
      return new Ctordbset(setPage, this, name, !!this.snapshot) as any;
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
      const superPage = this.superPage!;
      const r = await superPage.findIndexRecursive(new StringValue(name));
      if (!r.found) return null;
      const { dbset: Ctordbset, page: Ctorpage } = _setTypeInfo[type];
      const setPage = await this.storage.readPage(r.val!.value.val, Ctorpage);
      setPage.name = name;
      return new Ctordbset(setPage, this, name, !!this.snapshot) as any;
    } finally {
      if (useLock) lock.exitReader();
    }
  }

  async deleteSet(name: string): Promise<boolean> {
    let lockWriter = false;
    const lock = this.commitLock;
    await lock.enterWriter();
    try {
      const done = await this.superPage!.set(
        new StringValue(name),
        null,
        false,
      );
      if (done == "removed") {
        this.superPage!.setCount--;
        return true;
      } else if (done == "noop") {
        return false;
      } else {
        throw new BugError("Unexpected return value: " + done);
      }
    } finally {
      lock.exitWriter();
    }
  }

  async getSetCount() {
    return this.superPage!.setCount;
  }

  async getSetNames() {
    const lock = this.commitLock;
    await lock.enterReader();
    try {
      return (await this.superPage!.getAllValues()).map((x) => x.key.str);
    } finally {
      lock.exitReader();
    }
  }

  async commit() {
    await this.commitLock.enterWriter();
    try {
      // console.log('==========COMMIT==========');
      const r = await this.storage.commit();
      // console.log('========END COMMIT========');
      return r;
    } finally {
      this.commitLock.exitWriter();
    }
  }

  async getPrevSnapshot() {
    if (!this.superPage?.prevSuperPageAddr) return null;
    var prev = new DatabaseEngine();
    prev.storage = this.storage;
    prev.snapshot = await this.storage.readPage(
      this.superPage.prevSuperPageAddr,
      SuperPage,
    );
    return prev;
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
  openFile(path: string): Promise<void>;

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

  deleteSet(name: string): Promise<boolean>;

  getSetCount(): Promise<number>;
  getSetNames(): Promise<string[]>;

  commit(): Promise<void>;
  getPrevSnapshot(): Promise<Database | null>;
  close(): void;
}

export const Database: { new (): Database } = DatabaseEngine as any;
