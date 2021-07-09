import { DatabaseEngine, numberIdGenerator } from "./database.ts";
import { DocNodeType, DocSetPage, IndexInfo, IndexTopPage } from "./page.ts";
import { DocumentValue, JSONValue, KValue } from "./value.ts";
import { DbSet } from "./DbSet.ts";
import { BugError } from "./errors.ts";

export type IdType<T> = T extends { id: infer U } ? U : never;

export type OptionalId<T extends IDocument> =
  & Partial<Pick<T, "id">>
  & Omit<T, "id">;

export interface IDocument {
  id: string | number;
}

export type IndexDef<T> = Record<string, (doc: T) => any>;

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
  getFromIndex(index: string, key: any): Promise<T | void>;
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
    const { found, val } = await this.page.findIndexRecursive(
      new JSONValue(key),
    );
    if (!found) return null;
    return (val as DocNodeType)!.val;
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

  async getAll(): Promise<{ key: any; value: any }[]> {
    return (await this._getAllRaw() as DocNodeType[]).map((x) => x.val);
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
      const keyv = new JSONValue(key);
      const valv = !doc ? null : new DocumentValue(doc);
      const { action, oldValue: oldDoc } = await lockpage.set(
        keyv,
        valv,
        !inserting,
      );
      if (action == "added") {
        lockpage.count += 1;
      } else if (action == "removed") {
        lockpage.count -= 1;
      }

      try {
        const newIndexes: Record<string, IndexInfo> = {};
        for (const [indexName, indexInfo] of Object.entries(lockpage.indexes)) {
          const index =
            (await this.page.storage.readPage(indexInfo.addr, IndexTopPage))
              .getDirty(false);
          if (oldDoc) {
            const oldKey = new JSONValue(
              indexInfo.func((oldDoc as DocNodeType).val),
            );
            const setResult = await index.set(oldKey, null, false);
            if (setResult.action != "removed") {
              throw new BugError(
                "BUG: can not remove index key: " +
                  Deno.inspect({ oldDoc, indexInfo, oldKey, setResult }),
              );
            }
          }
          if (doc) {
            const kv = new KValue(new JSONValue(indexInfo.func(doc)), keyv);
            await index.set(kv.key, kv, false);
          }
          newIndexes[indexName] = new IndexInfo(
            indexInfo.funcStr,
            index.getLatestCopy().getDirty(true).addr,
            indexInfo.cachedFunc,
          );
        }
        lockpage.setIndexes(newIndexes);
      } catch (error) {
        // Failed in index updating (duplicated key in unique index?)
        // Rollback the change.
        await lockpage.set(keyv, oldDoc, true);
        throw error;
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
          const func = indexDefs[key];
          const info: IndexInfo = new IndexInfo(func.toString(), -1, func);
          const index = new IndexTopPage(lockpage.storage).getDirty(true);
          await lockpage.traverseKeys(async (k: DocumentValue) => {
            const indexKV = new KValue(new JSONValue(func(k.val)), k.key);
            await index.set(indexKV.key, indexKV, false);
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

  async getFromIndex(index: string, key: any): Promise<any> {
    const lockpage = this.page;
    await lockpage.lock.enterReader();
    try { // BEGIN READ LOCK
      const info = lockpage.indexes[index];
      if (!info) throw new Error("Specified index does not exist.");
      const indexPage = await this.page.storage.readPage(
        info.addr,
        IndexTopPage,
      );
      const indexResult = await indexPage.findIndexRecursive(
        new JSONValue(key),
      );
      if (!indexResult.found) return null;
      const docKey = indexResult.val.value;
      const docResult = await lockpage.findIndexRecursive(docKey);
      if (!docResult.found) {
        throw new BugError("BUG: found in index but document does not exist.");
      }
      return (docResult.val as DocNodeType).val;
    } finally { // END READ LOCK
      lockpage.lock.exitReader();
    }
  }
}
