import { DatabaseEngine, numberIdGenerator } from "./database.ts";
import {
  DocNodeType,
  DocSetPage,
  IndexInfo,
  IndexTopPage,
  KEYSIZE_LIMIT,
} from "./page.ts";
import {
  DocumentValue,
  JSONValue,
  KeyComparator,
  KeyLeftmostComparator,
  KValue,
} from "./value.ts";
import { DbSet } from "./DbSet.ts";
import { BugError } from "./errors.ts";
import { Runtime } from "./runtime.ts";

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
  readonly count: number;
  idGenerator: (lastId: IdType<T> | null) => IdType<T>;
  get(id: IdType<T>): Promise<T>;
  insert(doc: OptionalId<T>): Promise<void>;
  upsert(doc: T): Promise<void>;
  getAll(): Promise<T[]>;
  getIds<T>(): Promise<IdType<T>[]>;
  delete(id: IdType<T>): Promise<boolean>;
  useIndexes(indexDefs: IndexDef<T>): Promise<void>;
  findIndex(index: string, key: any): Promise<T[]>;
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
      new KeyComparator(new JSONValue(key)),
    );
    if (!found) return null;
    const docVal = await this._readDocument(val as DocNodeType);
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
        const doc = await this._readDocument(x);
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
        if (key == null) key = doc["id"] = this.idGenerator(lockpage.lastId);
        lockpage.lastId = key;
      }
      const dataPos = !doc
        ? null
        : await lockpage.storage.addData(new DocumentValue(doc));
      const keyv = new JSONValue(key);
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
      for (const [indexName, indexInfo] of Object.entries(lockpage.indexes)) {
        const index =
          (await lockpage.storage.readPage(indexInfo.addr, IndexTopPage))
            .getDirty(false);
        if (oldDoc) {
          const oldKey = new JSONValue(
            indexInfo.func(
              (await this._readDocument(oldDoc as DocNodeType)).val,
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
          const kv = new KValue(new JSONValue(indexInfo.func(doc)), dataPos!);
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
        // The indexesInfo size should not change, skip setIndexes() here.
        lockpage.indexes[indexName].addr =
          index.getLatestCopy().getDirty(true).addr;
      }
      // } catch (error) {
      //   // Failed in index updating (duplicated key in unique index?)
      //   // Rollback the change.
      //   await lockpage.set(keyv, oldDoc, true);
      //   throw error;
      // }

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
    const currentIndex = this.page.indexes;

    for (const key in indexDefs) {
      if (Object.prototype.hasOwnProperty.call(indexDefs, key)) {
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
        for (const key of toRemove) {
          delete newIndexes[key];
        }
        for (const key of toBuild) {
          const obj = indexDefs[key];
          const func = typeof obj == "function" ? obj : obj.key;
          const unique = typeof obj == "function"
            ? false
            : (obj.unique ?? false);
          const info: IndexInfo = new IndexInfo(
            func.toString(),
            -1,
            unique,
            func,
          );
          const index = new IndexTopPage(lockpage.storage).getDirty(true);
          await lockpage.traverseKeys(async (k: DocNodeType) => {
            const doc = await this._readDocument(k);
            const indexKV = new KValue(new JSONValue(func(doc.val)), k.value);
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
          info.addr = index.addr;
          newIndexes[key] = info;
        }
        lockpage.setIndexes(newIndexes);
      } finally { // END WRITE LOCK
        lockpage.lock.exitWriter();
        this._db.commitLock.exitWriter();
      }
    }
  }

  async findIndex(index: string, key: any): Promise<any[]> {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      const info = lockpage.indexes[index];
      if (!info) throw new Error("Specified index does not exist.");
      const indexPage = await this.page.storage.readPage(
        info.addr,
        IndexTopPage,
      );
      const keyv = new JSONValue(key);
      const indexResult = await indexPage.findKeyRecursive(
        new KeyLeftmostComparator(keyv),
      );

      const result: DocumentValue[] = [];

      let node = indexResult.node;
      let pos = indexResult.pos;

      while (true) {
        const val = node.keys[pos];
        if (val) {
          const comp = keyv.compareTo(val.key);
          if (comp === 0) {
            // Get one result and go right
            const doc = await this._readDocument(val);
            result.push(doc);
          } else {
            break;
          }
        }
        pos++;
        if (node.children[pos]) {
          // Go left down to child
          do {
            node = await node.readChildPage(pos);
            pos = 0;
          } while (node.children[0]);
        }
        if (node.children.length == pos) {
          // The end of this node, try go up
          if (node.parent) {
            pos = node.posInParent!;
            node = node.parent;
          } else {
            break;
          }
        }
      }
      return result.sort((a, b) => a.key.compareTo(b.key))
        .map((doc) => doc.val);
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }

  _readDocument(key: DocNodeType) {
    return this.page.storage.readData(key.value, DocumentValue);
  }

  async _dump() {
    return {
      docTree: await this.page._dumpTree(),
      indexes: Object.fromEntries(
        await Promise.all(
          Object.entries(this.page.indexes).map(async ([name, info]) => {
            const indexPage = await this.page.storage.readPage(
              info.addr,
              IndexTopPage,
            );
            return [name, await indexPage._dumpTree()];
          }),
        ),
      ),
    };
  }
}
