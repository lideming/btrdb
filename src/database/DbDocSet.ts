import {
  DocNodeType,
  DocSetPage,
  IndexInfo,
  IndexTopPage,
  KEYSIZE_LIMIT,
  PageAddr,
} from "../pages/page.ts";
import {
  DocumentValue,
  JSValue,
  KeyComparator,
  KValue,
  PageOffsetValue,
} from "../utils/value.ts";
import { AlreadyExistError, BugError } from "../utils/errors.ts";
import { Runtime } from "../utils/runtime.ts";
import { EQ, Query } from "../query/query.ts";
import { Node } from "../pages/tree.ts";
import type { IDbDocSet, IndexDef } from "../btrdb.d.ts";
import { nanoid } from "../utils/nanoid.ts";
import { DbSetBase } from "./DbSetBase.ts";

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

export class DbDocSet extends DbSetBase<DocSetPage> implements IDbDocSet {
  idGenerator: (lastId: any) => any = numberIdGenerator();

  async get(key: string): Promise<any | null> {
    const lock = await this.getPageEnterLock();
    try {
      const { found, val } = await lock.node.findKeyRecursive(
        new KeyComparator(new JSValue(key)),
      );
      if (!found) return null;
      const docVal = await this._readDocument((val as DocNodeType).value);
      return docVal.val;
    } finally {
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

  async getAll(): Promise<any[]> {
    const lock = await this.getPageEnterLock();
    try { // BEGIN READ LOCK
      const result = [];
      for await (const kv of lock.node.iterateKeys()) {
        const doc = await this._readDocument(kv.value);
        result.push(doc.val);
      }
      return result;
    } finally { // END READ LOCK
      lock.exitLock();
    }
  }

  async forEach(fn: (doc: any) => void | Promise<void>): Promise<void> {
    const lock = await this.getPageEnterLock();
    try { // BEGIN READ LOCK
      for await (const kv of lock.node.iterateKeys()) {
        const doc = await this._readDocument(kv.value);
        await fn(doc.val);
      }
    } finally { // END READ LOCK
      lock.exitLock();
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

    const lock = await this.getPageEnterLock(true);
    const dirtypage = lock.page.getDirty();
    const dirtynode = new Node(dirtypage);
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
            key = doc.id = this.idGenerator(dirtypage.lastId.val);
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
          : await dirtypage.storage.addData(new DocumentValue(doc));
        vPair = !doc ? null : new KValue(vKey, dataPos!);
        try {
          ({ action, oldValue: oldDoc } = await dirtynode.set(
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
        if (vKey.compareTo(dirtypage.lastId) > 0) {
          dirtypage.lastId = vKey;
        }
        dirtypage.count += 1;
      } else if (action == "removed") {
        dirtypage.count -= 1;
      }

      // TODO: rollback changes on (unique) index failed?
      // The following try-catch won't work well because some indexes may have changed.
      // try {
      let nextSeq = 0;
      for (
        const [indexName, indexInfo] of Object.entries(
          await dirtypage.ensureIndexes(),
        )
      ) {
        const seq = nextSeq++;
        const index = (await dirtypage.storage.readPage(
          dirtypage.indexesAddrs[seq],
          IndexTopPage,
        ))
          .getDirty();
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

        const newIndexAddr = (await indexNode.page.getDirtyWithAddr()).addr;
        dirtypage.indexesAddrs[seq] = newIndexAddr;
        dirtypage.indexesAddrMap[indexName] = newIndexAddr;
      }
      // } catch (error) {
      //   // Failed in index updating (duplicated key in unique index?)
      //   // Rollback the change.
      //   await dirtypage.set(keyv, oldDoc, true);
      //   throw error;
      // }
      if (action !== "noop") {
        if (lock.page !== dirtypage) {
          await dirtypage.getDirtyWithAddr();
          await this._db._updateSetPage(dirtypage);
        }
        if (this._db.autoCommit) await this._db._autoCommit();
      }
      return { action, key: vKey };
    } finally { // END WRITE LOCK
      lock.exitLock();
    }
  }

  async getIndexes(): Promise<
    Record<string, { key: string; unique: boolean }>
  > {
    const indexes = await (await this.getPage()).ensureIndexes();
    return Object.fromEntries(
      Object.entries(indexes!)
        .map(([k, v]) => [k, { key: v.funcStr, unique: v.unique }]),
    );
  }

  async useIndexes(indexDefs: IndexDef<any>): Promise<void> {
    const toBuild: string[] = [];
    const toRemove: string[] = [];
    const currentIndex = await (await this.getPage()).ensureIndexes();

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
      const lock = await this.getPageEnterLock();
      const dirtypage = lock.page.getDirty();
      const dirtynode = new Node(dirtypage);
      try { // BEGIN WRITE LOCK
        const newIndexes = { ...currentIndex };
        const newAddrs = { ...dirtypage.indexesAddrMap! };
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
          const index = await new IndexTopPage(dirtypage.storage)
            .getDirtyWithAddr();
          const indexNode = new Node(index);
          await dirtynode.traverseKeys(async (k: DocNodeType) => {
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
        await dirtypage.setIndexes(newIndexes, newAddrs);
        await dirtynode.postChange();

        if (lock.page !== dirtypage) {
          await dirtypage.getDirtyWithAddr();
          await this._db._updateSetPage(dirtypage);
        }
        if (this._db.autoCommit) await this._db._autoCommit();
      } finally { // END WRITE LOCK
        lock.exitLock();
      }
    }
  }

  async query(query: Query): Promise<any[]> {
    const lock = await this.getPageEnterLock();
    try { // BEGIN READ LOCK
      const result = [];
      for await (const docAddr of query.run(lock.node)) {
        result.push(await this._readDocument(docAddr));
      }
      return result.sort((a, b) => a.key.compareTo(b.key))
        .map((doc) => doc.val);
    } finally { // END READ LOCK
      lock.exitLock();
    }
  }

  findIndex(index: string, val: any): Promise<any[]> {
    return this.query(EQ(index, val));
  }

  _readDocument(dataAddr: PageOffsetValue) {
    return this._db.storage.readData(dataAddr, DocumentValue);
  }

  async _cloneTo(other: DbDocSet) {
    const thisStorage = this._db.storage;
    const otherStorage = other._db.storage;
    const dataAddrMap = new Map<number, number>();
    const thispage = await this.getPage();
    const thisnode = new Node(thispage);
    const otherpage = await other.getPage();
    const othernode = new Node(otherpage);
    for await (
      const key of thisnode.iterateKeys() as AsyncIterable<DocNodeType>
    ) {
      const doc = await thisStorage.readData(key.value, DocumentValue);
      const newAddr = await otherStorage.addData(doc);
      dataAddrMap.set(key.value.encode(), newAddr.encode());
      const newKey = new KValue(key.key, newAddr);
      await new Node(otherpage).set(newKey, newKey, "no-change");
    }
    const indexes = await thispage.ensureIndexes();
    const newIndexes: Record<string, IndexInfo> = {};
    const newAddrs: Record<string, PageAddr> = {};
    for (const [name, info] of Object.entries(indexes)) {
      const indexPage = await thisStorage.readPage(
        thispage.indexesAddrMap[name],
        IndexTopPage,
      );
      const otherIndex = await new IndexTopPage(otherStorage)
        .getDirtyWithAddr();
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
    await otherpage.setIndexes(newIndexes, newAddrs);
    await othernode.postChange();
    otherpage.count = thispage.count;
    otherpage.lastId = thispage.lastId;
  }

  async _dump() {
    const thispage = await this.getPage();
    const thisnode = new Node(thispage);
    return {
      docTree: await thisnode._dumpTree(),
      indexes: Object.fromEntries(
        await Promise.all(
          Object.entries(await thispage.ensureIndexes()).map(
            async ([name, info]) => {
              const indexPage = await thispage.storage.readPage(
                thispage.indexesAddrMap[name],
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
