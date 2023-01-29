import type { IDbSet, SetKeyType, SetValueType } from "../btrdb.d.ts";
import { DbSetBase } from "./DbSetBase.ts";
import { KEYSIZE_LIMIT, KVNodeType, SetPage } from "../pages/page.ts";
import { Node } from "../pages/tree.ts";
import { JSValue, KeyComparator, KValue } from "../utils/value.ts";

export class DbKvSet extends DbSetBase<SetPage> implements IDbSet {
  async get(key: SetKeyType): Promise<SetValueType | null> {
    const lock = await this.getPageEnterLock();
    try { // BEGIN READ LOCK
      const { found, val, node, pos } = await lock.node.findKeyRecursive(
        new KeyComparator(new JSValue(key)),
      );
      if (!found) return null;
      return (await this.readValue(val as KVNodeType)).val;
    } finally { // END READ LOCK
      lock.exitLock();
    }
  }

  protected async _getAllRaw() {
    const lock = await this.getPageEnterLock();
    try { // BEGIN READ LOCK
      return (await lock.node.getAllValues());
    } finally { // END READ LOCK
      lock.exitLock();
    }
  }

  async getAll(): Promise<{ key: SetKeyType; value: SetValueType }[]> {
    const result = [];
    const lock = await this.getPageEnterLock();
    try { // BEGIN READ LOCK
      for await (const key of lock.node.iterateKeys()) {
        result.push({
          key: key.key.val,
          value: (await this.readValue(key)).val,
        });
      }
      return result;
    } finally { // END READ LOCK
      lock.exitLock();
    }
  }

  async getKeys(): Promise<string[]> {
    return (await this._getAllRaw()).map((x) => (x as KVNodeType).key.val);
  }

  async forEach(
    fn: (key: any, value: any) => void | Promise<void>,
  ): Promise<void> {
    const lock = await this.getPageEnterLock();
    try { // BEGIN READ LOCK
      for await (const key of lock.node.iterateKeys()) {
        await fn(key.key.val, (await this.readValue(key)).val);
      }
    } finally { // END READ LOCK
      lock.exitLock();
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

    const lock = await this.getPageEnterLock(true);
    const dirtypage = lock.page.getDirty();
    const dirtynode = new Node(dirtypage);
    try { // BEGIN WRITE LOCK
      const valv = val == null
        ? null
        : new KValue(keyv, await lock.page.storage.addData(new JSValue(val)));
      const { action } = await dirtynode.set(
        new KeyComparator(keyv),
        valv,
        "can-change",
      );
      if (action == "added") {
        dirtypage.count += 1;
      } else if (action == "removed") {
        dirtypage.count -= 1;
      }
      if (action == "noop") {
        return false;
      } else {
        if (dirtypage !== lock.page) {
          await dirtypage.getDirtyWithAddr();
          await this._db._updateSetPage(dirtypage);
        }
        if (this._db.autoCommit) await this._db._autoCommit();
        return true;
      }
    } finally { // END WRITE LOCK
      lock.exitLock();
    }
  }

  delete(key: string) {
    return this.set(key, null);
  }

  readValue(node: KVNodeType) {
    return this._db.storage.readData(node.value, JSValue);
  }

  async _cloneTo(other: DbKvSet) {
    const otherStorage = other._db.storage;
    const otherPage = (await other.getPage())!;
    const otherNode = new Node(otherPage);
    for await (
      const kv of new Node((await this.getPage())!)
        .iterateKeys() as AsyncIterable<KVNodeType>
    ) {
      const newKv = new KValue(
        kv.key,
        await otherStorage.addData(await this.readValue(kv)),
      );
      await otherNode.set(new KeyComparator(newKv.key), newKv, "no-change");
    }
    otherPage.count = otherPage.count;
  }

  async _dump() {
    return { kvTree: await new Node((await this.getPage())!)._dumpTree() };
  }
}
