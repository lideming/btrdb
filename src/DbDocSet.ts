import { numberIdGenerator } from "./database.ts";
import { DocNodeType, DocSetPage } from "./page.ts";
import { JSONValue, KValue } from "./value.ts";
import { DbSet } from "./DbSet.ts";

export type IdType<T> = T extends { id: infer U } ? U : never;

export type OptionalId<T extends IDocument> =
  & Partial<Pick<T, "id">>
  & Omit<T, "id">;

export interface IDocument {
  id: string | number;
}

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
}

export class DbDocSet extends DbSet implements IDbDocSet {
  protected get page() { return super.page as DocSetPage; }

  idGenerator: (lastId: any) => any = numberIdGenerator;

  async get(key: string): Promise<any | null> {
    const { found, val } = await this.page.findIndexRecursive(
      new JSONValue(key),
    );
    if (!found) return null;
    return (val as DocNodeType)!.value.val;
  }

  async getAll(): Promise<{ key: any; value: any }[]> {
    return (await this._getAllRaw() as DocNodeType[]).map((x) => x.value.val);
  }

  async getIds(): Promise<any[]> {
    return (await this._getAllRaw() as DocNodeType[]).map((x) => x.key.val);
  }

  insert(doc: any) {
    if (doc["id"] != null) {
      throw new Error('"id" property should not exist on inserting');
    }
    return this._set(doc, true);
  }

  upsert(doc: any) {
    if (!("id" in doc)) throw new Error('"id" property doesn\'t exist');
    return this._set(doc, false);
  }

  async _set(doc: any, inserting: boolean) {
    if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");

    await this._db.commitLock.enterWriter();
    const lockpage = await this.page.enterCoWLock();
    try { // BEGIN WRITE LOCK
      let key = doc["id"];
      if (inserting) {
        if (key == null) key = doc["id"] = this.idGenerator(lockpage.lastId);
        lockpage.lastId = key;
      }
      const keyv = new JSONValue(key);
      const valv = !doc ? null : new KValue(keyv, new JSONValue(doc));
      const done = await lockpage.set(keyv, valv, !inserting);
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

  async delete(key: any) {
    if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");

    await this._db.commitLock.enterWriter();
    const lockpage = await this.page.enterCoWLock();
    try { // BEGIN WRITE LOCK
      const keyv = new JSONValue(key);
      const done = await lockpage.set(keyv, null, false);
      if (done == "removed") {
        lockpage.count -= 1;
        return true;
      } else if (done == "noop") {
        return false;
      } else {
        throw new Error("Unexpected return value from NodePage.set: " + done);
      }
    } finally { // END WRITE LOCK
      lockpage.lock.exitWriter();
      this._db.commitLock.exitWriter();
    }
  }
}
