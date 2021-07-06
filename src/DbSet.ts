import { DatabaseEngine } from "./database.ts";
import { SetPage } from "./page.ts";
import { KValue, StringValue } from "./value.ts";

export interface IDbSet {
  readonly count: number;
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  getAll(): Promise<{ key: string; value: string }[]>;
  getKeys(): Promise<string[]>;
  delete(key: string): Promise<void>;
}

export class DbSet implements IDbSet {
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

    await this._db.commitLock.enterWriter();
    const lockpage = await this.page.enterCoWLock();
    try { // BEGIN WRITE LOCK
      const done = await lockpage.set(keyv, valv, true);
      if (done == "added") {
        lockpage.count += 1;
      } else if (done == "removed") {
        lockpage.count -= 1;
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
