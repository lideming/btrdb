import { DocSetPage, RecordsPage, SetPage, SuperPage } from "./page.ts";
import { InFileStorage, PageStorage } from "./storage.ts";
import { OneWriterLock } from "./util.ts";
import { JSONValue, KValue, StringValue, UIntValue } from "./value.ts";

export interface EngineContext {
  storage: PageStorage;
}

export class DbSet {
  constructor(
    private _page: SetPage,
    protected _db: DatabaseEngine,
    public readonly name: string,
    protected isSnapshot: boolean,
  ) {}

  private get page() {
    if (this.isSnapshot) return this._page;
    return this._page = this._page.getLatestCopy();
  }

  get count() {
    return this.page.count;
  }

  async get(key: string): Promise<string | null> {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      const { found, val } = await this.page.findIndexRecursive(
        new StringValue(key),
      );
      if (!found) return null;
      return val!.value.str;
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  protected async _getAllRaw() {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      return (await this.page.getAllValues());
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  async getAll(): Promise<{ key: string; value: string }[]> {
    return (await this._getAllRaw()).map((x) => ({
      key: x.key.str,
      value: x.value.str,
    }));
  }

  async getKeys(): Promise<string[]> {
    return (await this._getAllRaw()).map((x) => x.key.str);
  }

  async set(key: string, val: string | null) {
    if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
    const keyv = new StringValue(key);
    const valv = !val ? null : new KValue(keyv, new StringValue(val));

    const lockpage = this.page;
    await this._db.commitLock.enterWriter();
    await lockpage.lock.enterWriter();
    try { // BEGIN WRITE LOCK
      const done = await this.page.set(keyv, valv);
      if (done == "added") {
        this.page.count += 1;
      } else if (done == "removed") {
        this.page.count -= 1;
      }
    } finally { // END WRITE LOCK
      lockpage.lock.exitWriter();
      this._db.commitLock.exitWriter();
    }
  }

  delete(key: string) {
    return this.set(key, null);
  }
}

//@ts-expect-error
export class DocDbSet extends DbSet {
  declare private _page: DocSetPage;
  declare private page: DocSetPage;

  async get(key: string): Promise<any | null> {
    const { found, val } = await this.page.findIndexRecursive(
      new JSONValue(key),
    );
    if (!found) return null;
    return (val!.value as any).val;
  }

  async getAll(): Promise<{ key: any; value: any }[]> {
    return (await this._getAllRaw() as any[]).map((x) => ({
      key: x.key.val,
      value: x.value.val,
    }));
  }

  async getKeys(): Promise<any[]> {
    return (await this._getAllRaw() as any[]).map((x) => x.key.val);
  }

  async set(key: any, val: any) {
    if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
    const keyv = new JSONValue(key);
    const valv = !val ? null : new KValue(keyv, new JSONValue(val));

    const lockpage = this.page;
    await this._db.commitLock.enterWriter();
    await lockpage.lock.enterWriter();
    try { // BEGIN WRITE LOCK
      const done = await this.page.set(keyv, valv);
      if (done == "added") {
        this.page.count += 1;
      } else if (done == "removed") {
        this.page.count -= 1;
      }
    } finally { // END WRITE LOCK
      lockpage.lock.exitWriter();
      this._db.commitLock.exitWriter();
    }
  }

  delete(key: string) {
    return this.set(key, null);
  }
}

export type DbSetType = keyof typeof _setTypeInfo;

const _setTypeInfo = {
  kv: { page: SetPage, dbset: DbSet },
  doc: { page: DocSetPage, dbset: DocDbSet },
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
  async createSet(name: string, type: "doc"): Promise<DocDbSet>;
  async createSet(
    name: string,
    type: DbSetType = "kv",
  ): Promise<DbSet | DocDbSet> {
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
      console.info("exit", lockWriter ? "writer" : "reader", name);
    }
  }

  async getSet(name: string, type: "kv"): Promise<DbSet | null>;
  async getSet(name: string, type: "doc"): Promise<DocDbSet | null>;
  getSet(
    name: string,
    type: DbSetType = "kv",
  ): Promise<DbSet | DocDbSet | null> {
    return this._getSet(name, type, true);
  }

  private async _getSet(
    name: string,
    type: DbSetType,
    useLock: boolean,
  ): Promise<DbSet | DocDbSet | null> {
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

export interface Database {
  openFile(path: string): Promise<void>;

  createSet(name: string, type?: "kv"): Promise<DbSet>;
  createSet(name: string, type: "doc"): Promise<DocDbSet>;

  getSet(name: string, type?: "kv"): Promise<DbSet | null>;
  getSet(name: string, type: "doc"): Promise<DocDbSet | null>;

  getSetCount(): Promise<number>;
  getSetNames(): Promise<string[]>;

  commit(): Promise<void>;
  getPrevSnapshot(): Promise<Database | null>;
  close(): void;
}

export const Database: { new (): Database } = DatabaseEngine as any;
