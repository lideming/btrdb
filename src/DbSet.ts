import type { IDbSet, SetKeyType, SetValueType } from "./btrdb.d.ts";
import { DatabaseEngine } from "./database.ts";
import { KEYSIZE_LIMIT, KVNodeType, SetPage } from "./page.ts";
import { Node } from "./tree.ts";
import { JSValue, KeyComparator, KValue } from "./value.ts";

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

  protected get node() {
    return new Node(this.page);
  }

  get count() {
    return this.page.count;
  }

  async get(key: SetKeyType): Promise<SetValueType | null> {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      const { found, val } = await this.node.findKeyRecursive(
        new KeyComparator(new JSValue(key)),
      );
      if (!found) return null;
      return (await this.readValue(val as KVNodeType)).val;
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  protected async _getAllRaw() {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      return (await this.node.getAllValues());
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  async getAll(): Promise<{ key: SetKeyType; value: SetValueType }[]> {
    const result = [];
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      for await (const key of this.node.iterateKeys()) {
        result.push({
          key: key.key.val,
          value: (await this.readValue(key)).val,
        });
      }
      return result;
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  async getKeys(): Promise<string[]> {
    return (await this._getAllRaw()).map((x) => (x as KVNodeType).key.val);
  }

  async forEach(
    fn: (key: any, value: any) => (void | Promise<void>),
  ): Promise<void> {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      for await (const key of this.node.iterateKeys()) {
        await fn(key.key.val, (await this.readValue(key)).val);
      }
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  async set(key: SetKeyType, val: SetValueType | null) {
    if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
    const keyv = new JSValue(key);
    if (keyv.byteLength > KEYSIZE_LIMIT) {
      throw new Error(
        `The key size is too large (${keyv.byteLength}), the limit is ${KEYSIZE_LIMIT}`,
      );
    }

    await this._db.commitLock.enterWriter();
    const lockpage = this.page.getDirty(false);
    await lockpage.lock.enterWriter();
    try { // BEGIN WRITE LOCK
      const dataAddr = this.page.storage.addData(new JSValue(val));
      const valv = val == null ? null : new KValue(keyv, dataAddr);
      const { action } = await this.node.set(
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
        if (this._db.autoCommit) await this._db._autoCommit();
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

  readValue(node: KVNodeType) {
    return this.page.storage.readData(node.value, JSValue);
  }

  async _cloneTo(other: DbSet) {
    const otherStorage = other._page.storage;
    for await (
      const kv of this.node.iterateKeys() as AsyncIterable<KVNodeType>
    ) {
      const newKv = new KValue(
        kv.key,
        otherStorage.addData(await this.readValue(kv)),
      );
      await other.node.set(new KeyComparator(newKv.key), newKv, "no-change");
    }
    other.page.count = this.page.count;
  }

  async _dump() {
    return { kvTree: await this.node._dumpTree() };
  }
}
