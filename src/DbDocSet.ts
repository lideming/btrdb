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
import { BugError } from "./errors.ts";
import { Runtime } from "./runtime.ts";
import { EQ, Query } from "./query.ts";

export type IdType<T> = T extends { id: infer U } ? U : never;

export type OptionalId<T extends IDocument> =
  & Partial<Pick<T, "id">>
  & Omit<T, "id">;

export interface IDocument {
  id: string | number;
}

export type KeySelector<T> = (doc: T) => any;
export type IndexDef<T> = Record<
  string,
  KeySelector<T> | { key: KeySelector<T>; unique?: boolean }
>;

export interface IDbDocSet<
  T extends IDocument = any,
> {
  /** Documents count of this set. */
  readonly count: number;

  /**
   * Get/set a function used to generate next id when inserting document.
   * `numberIdGenerator` is used by default.
   */
  idGenerator: (lastId: IdType<T> | null) => IdType<T>;

  /** Get a document by id */
  get(id: IdType<T>): Promise<T>;

  /** Insert a document with auto-id. */
  insert(doc: OptionalId<T>): Promise<void>;

  /** Update the document if the id exists, or insert the docuemnt if the id not exists. */
  upsert(doc: T): Promise<void>;

  /** Get all documents from this set. */
  getAll(): Promise<T[]>;

  /** Get all ids from this set. */
  getIds<T>(): Promise<IdType<T>[]>;

  /** Delete a document by id. */
  delete(id: IdType<T>): Promise<boolean>;

  /**
   * Define indexes on this set.
   * The new set of index definitions will overwrite the old one.
   * If some definition is added/changed/removed, the index will be added/changed/removed accrodingly.
   */
  useIndexes(indexDefs: IndexDef<T>): Promise<void>;

  /** Find values from the index. Returns matched documents. It equals to `query(EQ(index, key))`. */
  findIndex(index: string, key: any): Promise<T[]>;

  /** Do a query on this set. Returns matched documents. */
  query(query: Query): Promise<T[]>;
}

export function numberIdGenerator(lastId: number | null) {
  if (lastId == null) return 1;
  return lastId + 1;
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

  get count() {
    return this.page.count;
  }

  idGenerator: (lastId: any) => any = numberIdGenerator;

  async get(key: string): Promise<any | null> {
    const { found, val } = await this.page.findKeyRecursive(
      new KeyComparator(new JSValue(key)),
    );
    if (!found) return null;
    const docVal = await this._readDocument((val as DocNodeType).value);
    return docVal.val;
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

  async getAll(): Promise<any[]> {
    return Promise.all(
      (await this._getAllRaw() as DocNodeType[]).map(async (x) => {
        const doc = await this._readDocument(x.value);
        return doc.val;
      }),
    );
  }

  async getIds(): Promise<any[]> {
    return (await this._getAllRaw() as DocNodeType[]).map((x) => x.key.val);
  }

  async insert(doc: any) {
    if (doc["id"] != null) {
      throw new Error('"id" property should not exist on inserting');
    }
    await this._set(null, doc, true);
  }

  async upsert(doc: any) {
    const key = doc["id"];
    if (key == null) throw new Error('"id" property doesn\'t exist');
    await this._set(key, doc, false);
  }

  async _set(key: any, doc: any, inserting: boolean) {
    if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");

    await this._db.commitLock.enterWriter();
    const lockpage = await this.page.enterCoWLock();
    try { // BEGIN WRITE LOCK
      if (inserting) {
        if (key == null) {
          key = doc["id"] = this.idGenerator(lockpage.lastId.val);
        }
        lockpage.lastId = new JSValue(key);
      }
      const dataPos = !doc
        ? null
        : await lockpage.storage.addData(new DocumentValue(doc));
      const keyv = new JSValue(key);
      if (keyv.byteLength > KEYSIZE_LIMIT) {
        throw new Error(
          `The id size is too large (${keyv.byteLength}), the limit is ${KEYSIZE_LIMIT}`,
        );
      }
      const valv = !doc ? null : new KValue(keyv, dataPos!);
      const { action, oldValue: oldDoc } = await lockpage.set(
        new KeyComparator(keyv),
        valv,
        inserting ? "no-change" : "can-change",
      );
      if (action == "added") {
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
        if (oldDoc) {
          const oldKey = new JSValue(
            indexInfo.func(
              (await this._readDocument((oldDoc as DocNodeType).value)).val,
            ),
          );
          const setResult = await index.set(
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
          const setResult = await index.set(
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

        const newIndexAddr = index.getLatestCopy().getDirty(true).addr;
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
      return { action, key: keyv };
    } finally { // END WRITE LOCK
      lockpage.lock.exitWriter();
      this._db.commitLock.exitWriter();
    }
  }

  async delete(key: any) {
    const { action } = await this._set(key, null, false);
    return action == "removed";
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
      const lockpage = await this.page.enterCoWLock();
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
          const unique = typeof obj == "function"
            ? false
            : (obj.unique ?? false);
          const info: IndexInfo = new IndexInfo(
            func.toString(),
            unique,
            func,
          );
          const index = new IndexTopPage(lockpage.storage).getDirty(true);
          await lockpage.traverseKeys(async (k: DocNodeType) => {
            const doc = await this._readDocument(k.value);
            const indexKV = new KValue(new JSValue(func(doc.val)), k.value);
            if (indexKV.key.byteLength > KEYSIZE_LIMIT) {
              throw new Error(
                `The index key size is too large (${indexKV.key.byteLength}), the limit is ${KEYSIZE_LIMIT}`,
              );
            }
            await index.set(
              unique ? new KeyComparator(indexKV.key) : indexKV,
              indexKV,
              "no-change",
            );
          });
          newAddrs[key] = index.addr;
          newIndexes[key] = info;
        }
        lockpage.setIndexes(newIndexes, newAddrs);
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
      for await (const docAddr of query.run(lockpage)) {
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
    const dataAddrMap = new Map<number, number>();
    for await (
      const key of this.page.iterateKeys() as AsyncIterable<DocNodeType>
    ) {
      const doc = await this.page.storage.readData(key.value, DocumentValue);
      const newAddr = await other.page.storage.addData(doc);
      dataAddrMap.set(key.value.encode(), newAddr.encode());
      const newKey = new KValue(key.key, newAddr);
      await other.page.set(newKey, newKey, "no-change");
    }
    const indexes = await this.page.ensureIndexes();
    const newIndexes: Record<string, IndexInfo> = {};
    const newAddrs: Record<string, PageAddr> = {};
    for (const [name, info] of Object.entries(indexes)) {
      const indexPage = await this.page.storage.readPage(
        this.page.indexesAddrMap[name],
        IndexTopPage,
      );
      const otherIndex = new IndexTopPage(other.page.storage).getDirty(true);
      for await (const key of indexPage.iterateKeys()) {
        const newKey = new KValue(
          key.key,
          PageOffsetValue.fromEncoded(
            dataAddrMap.get(key.value.encode())!,
          ),
        );
        await otherIndex.set(newKey, newKey, "no-change");
      }
      newIndexes[name] = info;
      newAddrs[name] = otherIndex.addr;
    }
    other.page.setIndexes(newIndexes, newAddrs);
    other.page.count = this.page.count;
  }

  async _dump() {
    return {
      docTree: await this.page._dumpTree(),
      indexes: Object.fromEntries(
        await Promise.all(
          Object.entries(await this.page.ensureIndexes()).map(
            async ([name, info]) => {
              const indexPage = await this.page.storage.readPage(
                this.page.indexesAddrMap[name],
                IndexTopPage,
              );
              return [name, await indexPage._dumpTree()];
            },
          ),
        ),
      ),
    };
  }
}
