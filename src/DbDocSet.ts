import { DatabaseEngine } from "./database.ts";
import {
  DocNodeType,
  DocSetPage,
  IndexInfo,
  IndexTopPage,
  KEYSIZE_LIMIT,
  PageAddr,
} from "./page.ts";
import {
  DocumentValue,
  JSValue,
  KeyComparator,
  KValue,
  PageOffsetValue,
} from "./value.ts";
import { AlreadyExistError, BugError } from "./errors.ts";
import { Runtime } from "./runtime.ts";
import { EQ, Query } from "./query.ts";
import { Node } from "./tree.ts";
import type { IDbDocSet, IndexDef } from "./btrdb.d.ts";
import { nanoid } from "./nanoid.ts";

function _numberIdGenerator(lastId: number | null) {
  if (lastId == null) return 1;
  return lastId + 1;
}

export function numberIdGenerator() {
  return _numberIdGenerator;
}

export function nanoIdGenerator(size = 21) {
  return (_lastId: number | null) => nanoid(size);
}

const enum Op {
  insert,
  upsert,
  update,
  delete,
}

export class DbDocSet implements IDbDocSet {
  protected _page: DocSetPage;

  constructor(
    page: DocSetPage,
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

  idGenerator: (lastId: any) => any = numberIdGenerator();

  async get(key: string): Promise<any | null> {
    const { found, val } = await this.node.findKeyRecursive(
      new KeyComparator(new JSValue(key)),
    );
    if (!found) return null;
    const docVal = await this._readDocument((val as DocNodeType).value);
    return docVal.val;
  }

  protected async _getAllRaw() {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    const thisnode = this.node;
    try { // BEGIN READ LOCK
      return (await thisnode.getAllValues());
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  async getAll(): Promise<any[]> {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    const thisnode = this.node;
    try { // BEGIN READ LOCK
      const result = [];
      for await (const kv of thisnode.iterateKeys()) {
        const doc = await this._readDocument(kv.value);
        result.push(doc.val);
      }
      return result;
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  async forEach(fn: (doc: any) => void | Promise<void>): Promise<void> {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    const thisnode = this.node;
    try { // BEGIN READ LOCK
      for await (const kv of thisnode.iterateKeys()) {
        const doc = await this._readDocument(kv.value);
        await fn(doc.val);
      }
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  async getIds(): Promise<any[]> {
    return (await this._getAllRaw() as DocNodeType[]).map((x) => x.key.val);
  }

  async insert(doc: any) {
    await this._set(doc.id, doc, Op.insert);
  }

  async update(doc: any) {
    const key = doc.id;
    if (key == null) throw new Error('"id" property doesn\'t exist');
    await this._set(key, doc, Op.update);
  }

  async upsert(doc: any) {
    const key = doc.id;
    if (key == null) throw new Error('"id" property doesn\'t exist');
    await this._set(key, doc, Op.upsert);
  }

  async delete(key: any) {
    const { action } = await this._set(key, null, Op.delete);
    return action == "removed";
  }

  async _set(key: any, doc: any, op: Op) {
    if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");

    await this._db.commitLock.enterWriter();
    const lockpage = this.page.getDirty(false);
    await lockpage.lock.enterWriter();
    const thisnode = this.node;
    try { // BEGIN WRITE LOCK
      let dataPos;
      let vKey;
      let vPair;
      let action, oldDoc;
      let retryCount = 0;
      let autoId = false;
      while (true) { // retry in case of duplicated auto id
        if (op === Op.insert) {
          if (key == null) {
            autoId = true;
            key = doc.id = this.idGenerator(lockpage.lastId.val);
          }
        }
        vKey = new JSValue(key);
        if (vKey.byteLength > KEYSIZE_LIMIT) {
          throw new Error(
            `The id size is too large (${vKey.byteLength}), the limit is ${KEYSIZE_LIMIT}`,
          );
        }
        dataPos = !doc
          ? null
          : await lockpage.storage.addData(new DocumentValue(doc));
        vPair = !doc ? null : new KValue(vKey, dataPos!);
        try {
          ({ action, oldValue: oldDoc } = await thisnode.set(
            new KeyComparator(vKey),
            vPair,
            op === Op.insert
              ? "no-change"
              : op === Op.update
              ? "change-only"
              : "can-change",
          ));
        } catch (err) {
          if (autoId && err instanceof AlreadyExistError) {
            if (++retryCount > 10) {
              throw new Error(
                `Duplicated auto id after 10 retries, last id attempted: ${
                  Runtime.inspect(key)
                }`,
              );
            }
            key = doc.id = null;
            continue; // retry with another key
          }
          throw err;
        }
        break;
      }
      if (action == "added") {
        if (vKey.compareTo(lockpage.lastId) > 0) {
          lockpage.lastId = vKey;
        }
        lockpage.count += 1;
      } else if (action == "removed") {
        lockpage.count -= 1;
      }

      // TODO: rollback changes on (unique) index failed?
      // The following try-catch won't work well because some indexes may have changed.
      // try {
      let nextSeq = 0;
      for (
        const [indexName, indexInfo] of Object.entries(
          await lockpage.ensureIndexes(),
        )
      ) {
        const seq = nextSeq++;
        const index = (await lockpage.storage.readPage(
          lockpage.indexesAddrs[seq],
          IndexTopPage,
        ))
          .getDirty(false);
        const indexNode = new Node(index);
        if (oldDoc) {
          const oldKey = new JSValue(
            indexInfo.func(
              (await this._readDocument((oldDoc as DocNodeType).value)).val,
            ),
          );
          const setResult = await indexNode.set(
            new KValue(oldKey, (oldDoc as DocNodeType).value),
            null,
            "no-change",
          );
          if (setResult.action != "removed") {
            throw new BugError(
              "BUG: can not remove index key: " +
                Runtime.inspect({ oldDoc, indexInfo, oldKey, setResult }),
            );
          }
        }
        if (doc) {
          const kv = new KValue(new JSValue(indexInfo.func(doc)), dataPos!);
          if (kv.key.byteLength > KEYSIZE_LIMIT) {
            throw new Error(
              `The index key size is too large (${kv.key.byteLength}), the limit is ${KEYSIZE_LIMIT}`,
            );
          }
          const setResult = await indexNode.set(
            indexInfo.unique ? new KeyComparator(kv.key) : kv,
            kv,
            "no-change",
          );
          if (setResult.action != "added") {
            throw new BugError(
              "BUG: can not add index key: " +
                Runtime.inspect({ kv, indexInfo, setResult }),
            );
          }
        }

        const newIndexAddr = indexNode.page.getDirty(true).addr;
        lockpage.indexesAddrs[seq] = newIndexAddr;
        lockpage.indexesAddrMap[indexName] = newIndexAddr;
      }
      // } catch (error) {
      //   // Failed in index updating (duplicated key in unique index?)
      //   // Rollback the change.
      //   await lockpage.set(keyv, oldDoc, true);
      //   throw error;
      // }
      if (action !== "noop") {
        if (this._db.autoCommit) await this._db._autoCommit();
      }
      return { action, key: vKey };
    } finally { // END WRITE LOCK
      lockpage.lock.exitWriter();
      this._db.commitLock.exitWriter();
    }
  }

  async getIndexes(): Promise<
    Record<string, { key: string; unique: boolean }>
  > {
    await this.page.ensureIndexes();
    return Object.fromEntries(
      Object.entries(this.page.indexes!)
        .map(([k, v]) => [k, { key: v.funcStr, unique: v.unique }]),
    );
  }

  async useIndexes(indexDefs: IndexDef<any>): Promise<void> {
    const toBuild: string[] = [];
    const toRemove: string[] = [];
    const currentIndex = await this.page.ensureIndexes();

    for (const key in indexDefs) {
      if (Object.prototype.hasOwnProperty.call(indexDefs, key)) {
        if (key == "id") throw new Error("Cannot use 'id' as index name");
        const func = indexDefs[key];
        if (
          !Object.prototype.hasOwnProperty.call(currentIndex, key) ||
          currentIndex[key].funcStr != func.toString()
        ) {
          toBuild.push(key);
        }
      }
    }

    for (const key in currentIndex) {
      if (Object.prototype.hasOwnProperty.call(currentIndex, key)) {
        if (!Object.prototype.hasOwnProperty.call(indexDefs, key)) {
          toRemove.push(key);
        }
      }
    }

    if (toBuild.length || toRemove.length) {
      if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
      await this._db.commitLock.enterWriter();
      const lockpage = this.page.getDirty(false);
      await lockpage.lock.enterWriter();
      const thisnode = this.node;
      try { // BEGIN WRITE LOCK
        const newIndexes = { ...currentIndex };
        const newAddrs = { ...lockpage.indexesAddrMap! };
        for (const key of toRemove) {
          delete newIndexes[key];
          delete newAddrs[key];
        }
        for (const key of toBuild) {
          const obj = indexDefs[key];
          const func = typeof obj == "function" ? obj : obj.key;
          const unique = typeof obj == "object" && obj.unique == true;
          const info: IndexInfo = new IndexInfo(
            func.toString(),
            unique,
            func,
          );
          const index = new IndexTopPage(lockpage.storage).getDirty(true);
          const indexNode = new Node(index);
          await thisnode.traverseKeys(async (k: DocNodeType) => {
            const doc = await this._readDocument(k.value);
            const indexKV = new KValue(new JSValue(func(doc.val)), k.value);
            if (indexKV.key.byteLength > KEYSIZE_LIMIT) {
              throw new Error(
                `The index key size is too large (${indexKV.key.byteLength}), the limit is ${KEYSIZE_LIMIT}`,
              );
            }
            await indexNode.set(
              unique ? new KeyComparator(indexKV.key) : indexKV,
              indexKV,
              "no-change",
            );
          });
          newAddrs[key] = index.addr;
          newIndexes[key] = info;
        }
        lockpage.setIndexes(newIndexes, newAddrs);
        thisnode.postChange();
        if (this._db.autoCommit) await this._db._autoCommit();
      } finally { // END WRITE LOCK
        lockpage.lock.exitWriter();
        this._db.commitLock.exitWriter();
      }
    }
  }

  async query(query: Query): Promise<any[]> {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      const result = [];
      for await (const docAddr of query.run(this.node)) {
        result.push(await this._readDocument(docAddr));
      }
      return result.sort((a, b) => a.key.compareTo(b.key))
        .map((doc) => doc.val);
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  findIndex(index: string, val: any): Promise<any[]> {
    return this.query(EQ(index, val));
  }

  _readDocument(dataAddr: PageOffsetValue) {
    return this.page.storage.readData(dataAddr, DocumentValue);
  }

  async _cloneTo(other: DbDocSet) {
    const thisStorage = this.page.storage;
    const otherStorage = other.page.storage;
    const dataAddrMap = new Map<number, number>();
    for await (
      const key of this.node.iterateKeys() as AsyncIterable<DocNodeType>
    ) {
      const doc = await thisStorage.readData(key.value, DocumentValue);
      const newAddr = await otherStorage.addData(doc);
      dataAddrMap.set(key.value.encode(), newAddr.encode());
      const newKey = new KValue(key.key, newAddr);
      await new Node(other.page).set(newKey, newKey, "no-change");
    }
    const indexes = await this.page.ensureIndexes();
    const newIndexes: Record<string, IndexInfo> = {};
    const newAddrs: Record<string, PageAddr> = {};
    for (const [name, info] of Object.entries(indexes)) {
      const indexPage = await thisStorage.readPage(
        this.page.indexesAddrMap[name],
        IndexTopPage,
      );
      const otherIndex = new IndexTopPage(otherStorage).getDirty(true);
      const otherIndexNode = new Node(otherIndex);
      for await (const key of new Node(indexPage).iterateKeys()) {
        const newKey = new KValue(
          key.key,
          PageOffsetValue.fromEncoded(
            dataAddrMap.get(key.value.encode())!,
          ),
        );
        await otherIndexNode.set(newKey, newKey, "no-change");
      }
      newIndexes[name] = info;
      newAddrs[name] = otherIndex.addr;
    }
    other.page.setIndexes(newIndexes, newAddrs);
    other.node.postChange();
    other.page.count = this.page.count;
    other.page.lastId = this.page.lastId;
  }

  async _dump() {
    return {
      docTree: await this.node._dumpTree(),
      indexes: Object.fromEntries(
        await Promise.all(
          Object.entries(await this.page.ensureIndexes()).map(
            async ([name, info]) => {
              const indexPage = await this.page.storage.readPage(
                this.page.indexesAddrMap[name],
                IndexTopPage,
              );
              return [name, await new Node(indexPage)._dumpTree()];
            },
          ),
        ),
      ),
    };
  }
}
