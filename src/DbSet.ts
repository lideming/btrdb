import { DatabaseEngine } from "./database.ts";
import { KVNodeType, SetPage } from "./page.ts";
import { KeyComparator, KValue, StringValue } from "./value.ts";

export interface IDbSet {
  readonly count: number;
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<boolean>;
  getAll(): Promise<{ key: string; value: string }[]>;
  getKeys(): Promise<string[]>;
  delete(key: string): Promise<boolean>;
}

export class DbSet implements IDbSet {
  protected _page: SetPage;

  constructor(
    page: SetPage,
    protected _db: DatabaseEngine,
    public readonly name: string,
    protected isSnapshot: boolean,
  ) {
    this._page = page;
  }

  protected get page() {
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
      const { found, val } = await this.page.findKeyRecursive(
        new KeyComparator(new StringValue(key)),
      );
      if (!found) return null;
      return (val as KVNodeType)!.value.str;
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
      key: (x as KVNodeType).key.str,
      value: (x as KVNodeType).value.str,
    }));
  }

  async getKeys(): Promise<string[]> {
    return (await this._getAllRaw()).map((x) => (x as KVNodeType).key.str);
  }

  async set(key: string, val: string | null) {
    if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
    const keyv = new StringValue(key);
    const valv = val == null ? null : new KValue(keyv, new StringValue(val));

    await this._db.commitLock.enterWriter();
    const lockpage = await this.page.enterCoWLock();
    try { // BEGIN WRITE LOCK
      const { action } = await lockpage.set(
        new KeyComparator(keyv),
        valv,
        "can-change",
      );
      if (action == "added") {
        lockpage.count += 1;
      } else if (action == "removed") {
        lockpage.count -= 1;
      }
      if (action == "noop") {
        return false;
      } else {
        return true;
      }
    } finally { // END WRITE LOCK
      lockpage.lock.exitWriter();
      this._db.commitLock.exitWriter();
    }
  }

  delete(key: string) {
    return this.set(key, null);
  }

  async _dump() {
    return { kvTree: await this.page._dumpTree() };
  }
}
