/**
 * @example
 * const db = await new Database().openFile('data.db');
 *
 * const keyValues = await db.createSet('my_set');
 * await keyValues.set('key1', 'value1');
 *
 * const documents = await db.createSet('my_documents', 'doc');
 * await documents.insert({ username: 'yuuza', age: 20 });
 *
 * db.close();
 */
export class Database {
  /** (default: false) Whether to auto-commit on changes (i.e. on every call on `set`/`insert`/`upsert` methods) */
  autoCommit: boolean;

  /** (default: true) Whether to wait page writing in auto-commit. */
  autoCommitWaitWriting: boolean;

  /** (default: true) Whether to wait page writing in manual commit. */
  defaultWaitWriting: boolean;

  /** Open a database file. Create a new file if not exists. */
  openFile(
    path: string,
    options?: { fsync?: "final-only" | "strict" | boolean },
  ): Promise<void>;

  /** Open a database file as a new database instance. Create a new file if not exists. */
  static openFile(...args: Parameters<Database["openFile"]>): Promise<Database>;

  createSet(name: string, type?: "kv"): Promise<IDbSet>;
  createSet<T extends IDocument>(
    name: string,
    type: "doc",
  ): Promise<IDbDocSet<T>>;

  getSet(name: string, type?: "kv"): Promise<IDbSet | null>;
  getSet<T extends IDocument>(
    name: string,
    type: "doc",
  ): Promise<IDbDocSet<T> | null>;

  /** Delete a key-value set or a document set. */
  deleteSet(name: string, type: DbSetType): Promise<boolean>;

  /** Get count of key-value sets and document sets */
  getSetCount(): Promise<number>;

  /** Get info of key-value sets, document sets and named snapshots. */
  getObjects(): Promise<{ name: string; type: DbObjectType }[]>;

  /** Delete a key-value set, a document set or a named snapshot. */
  deleteObject(name: string, type: DbObjectType): Promise<boolean>;

  /** Create a named snapshot. */
  createSnapshot(name: string, overwrite?: boolean): Promise<void>;

  /** Get a named snapshot. */
  getSnapshot(name: string): Promise<Database | null>;

  /** Get the previous commit as a snapshot. */
  getPrevCommit(): Promise<Database | null>;

  /**
   * Commit and write the changes to the disk.
   * @param waitWriting (default to `defaultWaitWriting`) whether to wait writing before resoving. If false, "deferred writing" is used.
   */
  commit(waitWriting?: boolean): Promise<boolean>;

  /** Wait for previous deferred writing tasks. */
  waitWriting(): Promise<void>;

  /**
   * Close the opened database file.
   * If deferred writing is used, ensure to await `waitWriting()` before closing.
   */
  close(): void;

  rebuild(): Promise<void>;

  /** Dump all sets as a JSON string. */
  dump(): Promise<string>;

  /** Import sets from a JSON string. */
  import(data: string): Promise<void>;
}

export type DbSetType = "kv" | "doc";

export type DbObjectType = DbSetType | "snapshot";

export type SetKeyType = string | number;
export type SetValueType = string | number | any[] | object;

export interface IDbSet {
  readonly count: number;
  get(key: SetKeyType): Promise<SetValueType | null>;
  set(key: SetKeyType, value: SetValueType): Promise<boolean>;
  getAll(): Promise<{ key: SetKeyType; value: SetValueType }[]>;
  getKeys(): Promise<SetKeyType[]>;
  forEach(
    fn: (key: SetKeyType, value: SetValueType) => (void | Promise<void>),
  ): Promise<void>;
  delete(key: SetKeyType): Promise<boolean>;
}

export type IdType<T> = T extends { id: infer U } ? U : never;

/** @deprecated use OptionalId */
export type NoId<T extends IDocument> = OptionalId<T>;

export type OptionalId<T extends IDocument> =
  & Omit<T, "id">
  & { id?: T["id"] };

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

  /** Update the document if the id exists, or throw if the id not exists. */
  update(doc: T): Promise<void>;

  /** Update the document if the id exists, or insert the docuemnt if the id not exists. */
  upsert(doc: T): Promise<void>;

  /** Get all documents from this set. */
  getAll(): Promise<T[]>;

  /** Get all ids from this set. */
  getIds<T>(): Promise<IdType<T>[]>;

  /** Iterate through all documents. */
  forEach(fn: (doc: T) => (void | Promise<void>)): Promise<void>;

  /** Delete a document by id. */
  delete(id: IdType<T>): Promise<boolean>;

  /**
   * Define indexes on this set.
   * The new set of index definitions will overwrite the old one.
   * If some definition is added/changed/removed, the index will be added/changed/removed accrodingly.
   */
  useIndexes(indexDefs: IndexDef<T>): Promise<void>;

  /** Get current index definitions on this set. */
  getIndexes(): Promise<Record<string, { key: string; unique: boolean }>>;

  /** Find values from the index. Returns matched documents. It equals to `query(EQ(index, key))`. */
  findIndex(index: string, key: any): Promise<T[]>;

  /** Do a query on this set. Returns matched documents. */
  query(query: Query): Promise<T[]>;
}

export function numberIdGenerator(lastId: number | null): number;

export type IndexDef<T> = Record<
  string,
  KeySelector<T> | { key: KeySelector<T>; unique?: boolean }
>;

export type IndexValue =
  | string
  | number
  | boolean
  | IndexValue[]
  | null
  | undefined;

export type KeySelector<T> = (doc: T) => IndexValue;

export interface IDocument {
  id: string | number;
}

export interface Query {
  run(node: any): any;
}

export function query(plainText: TemplateStringsArray, ...args: any[]): Query;

export function EQ(index: string, val: any): Query;
export function NE(index: string, val: any): Query;
export function GT(index: string, val: any): Query;
export function GE(index: string, val: any): Query;
export function LT(index: string, val: any): Query;
export function LE(index: string, val: any): Query;
export function BETWEEN(
  index: string,
  min: any,
  max: any,
  minInclusive: boolean,
  maxInclusive: boolean,
): Query;
export function AND(...queries: Query[]): Query;
