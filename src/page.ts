export const PAGESIZE = 4096;

import { Buffer } from "./buffer.ts";
import { AlreadyExistError, BugError, NotExistError } from "./errors.ts";
import { PageStorage } from "./storage.ts";
import { OneWriterLock } from "./util.ts";
import {
  DocumentValue,
  IKey,
  IValue,
  JSONValue,
  KeyOf,
  KeyType,
  KValue,
  StringValue,
  UIntValue,
} from "./value.ts";

export type PageAddr = number;

export type InlinablePage<T> = PageAddr | T;

export const enum PageType {
  None,
  Super = 1,
  RootTreeNode,
  Set,
  Records,
  DocSet,
  DocRecords,
  IndexTop,
  Index,
}

export interface PageClass<T extends Page> {
  new (storage: PageStorage): T;
}

export abstract class Page {
  storage: PageStorage;
  addr: PageAddr = -1;
  abstract get type(): PageType;

  constructor(storage: PageStorage) {
    this.storage = storage;
    this.init();
  }

  /** Should not change pages on disk, we should always copy on write */
  dirty = false;

  get hasAddr() {
    return this.addr != -1;
  }

  _newerCopy: this | null = null;

  /** Should be maintained by the page when changing data */
  freeBytes: number = PAGESIZE - 4;

  init() {}

  /**
     * Create a dirty copy of this page or return this page if it's already dirty.
     * @param addDirty {boolean} whether to assign the page address
     */
  getDirty(addDirty: boolean): this {
    if (this._newerCopy) throw new BugError("getDirty on out-dated page");
    if (this.dirty) {
      if (addDirty && !this.hasAddr) this.storage.addDirty(this);
      return this;
    } else {
      let dirty = new this._thisCtor(this.storage);
      dirty.dirty = true;
      this._copyTo(dirty);
      this._newerCopy = dirty;
      if (addDirty) this.storage.addDirty(dirty);
      return dirty;
    }
  }

  getLatestCopy(): this {
    let p = this;
    while (p._newerCopy) p = p._newerCopy;
    return p;
  }

  writeTo(buf: Buffer) {
    if (this.freeBytes < 0) {
      console.error(this);
      throw new BugError(`BUG: page content overflow (free ${this.freeBytes})`);
    }
    const beginPos = buf.pos;
    buf.writeU8(this.type);
    buf.writeU8(0);
    buf.writeU16(0);
    this._writeContent(buf);
    if (buf.pos - beginPos != PAGESIZE - this.freeBytes) {
      throw new BugError(
        `BUG: buffer written (${buf.pos - beginPos}) != space used (${PAGESIZE -
          this.freeBytes})`,
      );
    }
  }
  readFrom(buf: Buffer) {
    const beginPos = buf.pos;
    const type = buf.readU8();
    if (type != this.type) {
      throw new Error(
        `Wrong type in disk, should be ${this.type}, got ${type}`,
      );
    }
    if (buf.readU8() != 0) throw new Error("Non-zero reserved field");
    if (buf.readU16() != 0) throw new Error("Non-zero reserved field");
    this._readContent(buf);
    if (buf.pos - beginPos != PAGESIZE - this.freeBytes) {
      throw new BugError(
        `BUG: buffer read (${buf.pos - beginPos}) != space used (${PAGESIZE -
          this.freeBytes})`,
      );
    }
  }

  _debugView() {
    return {
      type: this.type,
      addr: this.addr,
      dirty: this.dirty,
    };
  }

  protected _copyTo(page: this) {
    if (Object.getPrototypeOf(this) != Object.getPrototypeOf(page)) {
      throw new Error("_copyTo() with different types");
    }
  }
  protected _writeContent(buf: Buffer) {}
  protected _readContent(buf: Buffer) {}
  protected get _thisCtor(): PageClass<this> {
    return Object.getPrototypeOf(this).constructor as PageClass<this>;
  }
}

/** A page as an B+ tree node */
export abstract class NodePage<T extends IKey<unknown>> extends Page {
  parent?: NodePage<T> = undefined;
  posInParent?: number = undefined;
  keys: T[] = [];
  children: PageAddr[] = [];

  override init() {
    super.init();
    this.freeBytes -= 2; // keysCount
  }

  setKeys(newKeys: T[], newChildren: PageAddr[]) {
    // console.log([newKeys.length, newChildren.length]);
    if (
      !((newKeys.length == 0 && newChildren.length == 0) ||
        (newKeys.length + 1 == newChildren.length) ||
        (newChildren.length == newKeys.length))
    ) {
      throw new Error("Invalid args");
    }
    if (this.keys) {
      this.freeBytes += calcSizeOfKeys(this.keys) + this.children.length * 4;
    }
    if (newChildren.length !== 0 && newChildren.length === newKeys.length) {
      newChildren.push(0);
    }
    if (newKeys) {
      this.freeBytes -= calcSizeOfKeys(newKeys) + newChildren.length * 4;
    }
    this.keys = newKeys;
    this.children = newChildren;
  }

  /**
     * Remove and return a range of keys, and/or, insert a key.
     * @returns `delCount` keys and `delCount` children.
     **/
  spliceKeys(
    pos: number,
    delCount: number,
    key?: T,
    leftChild?: PageAddr,
  ): [deletedKeys: T[], deletedChildren: PageAddr[]] {
    if (leftChild! < 0) throw new BugError("Invalid leftChild");
    // this.writeTo(new Buffer(new Uint8Array(4096), 0));
    let deleted: T[];
    let deletedChildren: PageAddr[];
    if (key) {
      deleted = this.keys.splice(pos, delCount, key);
      deletedChildren = this.children.splice(pos, delCount, leftChild || 0);
      if (delCount == 0 && this.keys.length == 1) {
        this.freeBytes -= 4;
        this.children.push(0);
      }
      this.freeBytes -= key.byteLength + 4;
    } else {
      deleted = this.keys.splice(pos, delCount);
      deletedChildren = this.children.splice(pos, delCount);
      if (delCount && this.keys.length == 0) {
        this.freeBytes += 4;
        if (this.children.pop()! != 0) throw new Error("Not implemented");
      }
    }
    this.freeBytes += calcSizeOfKeys(deleted) + delCount * 4;
    // this.writeTo(new Buffer(new Uint8Array(4096), 0));
    return [deleted, deletedChildren];
  }

  setChild(pos: number, child: PageAddr) {
    if (pos < 0 || this.children.length <= pos) {
      throw new BugError("pos out of range");
    }
    this.children[pos] = child;
  }

  setKey(pos: number, key: T) {
    if (pos < 0 || this.keys.length <= pos) {
      throw new BugError("pos out of range");
    }
    this.freeBytes -= key.byteLength - this.keys[pos].byteLength;
    this.keys[pos] = key;
  }

  findIndex(key: KeyOf<T>):
    | { found: true; pos: number; val: T }
    | { found: false; pos: number; val: undefined } {
    const keys = this.keys;
    let l = 0, r = keys.length - 1;
    while (l <= r) {
      const m = Math.round((l + r) / 2);
      const c = key.compareTo(keys[m].key);
      // console.log("compare", key, c == 0 ? '==' : c > 0 ? '>' : '<', keys[m]);
      if (c == 0) return { found: true, pos: m, val: keys[m] };
      else if (c > 0) l = m + 1;
      else r = m - 1;
    }
    return { found: false, pos: l, val: undefined };
  }

  async getAllValues(array?: T[]): Promise<T[]> {
    if (!array) array = [];
    await this.traverseKeys((key) => {
      array!.push(key as any);
    });
    return array;
  }

  async traverseKeys(
    func: (key: T, page: this, pos: number) => Promise<void> | void,
  ) {
    for (let pos = 0; pos < this.children.length; pos++) {
      const leftAddr = this.children[pos];
      if (leftAddr) {
        const leftPage = await this.storage.readPage(leftAddr, this._childCtor);
        await leftPage.traverseKeys(func as any);
      }
      if (pos < this.keys.length) {
        await func(this.keys[pos], this, pos);
      }
    }
  }

  async findIndexRecursive(key: KeyOf<T>): Promise<
    | { found: true; node: NodePage<T>; pos: number; val: T }
    | { found: false; node: NodePage<T>; pos: number; val: undefined }
  > {
    let node = this as NodePage<T>;
    while (true) {
      const { found, pos, val } = node.findIndex(key);
      if (found) return { found: true, node, pos, val: val as T };
      const childAddr = node.children[pos];
      if (!childAddr) return { found: false, node, pos, val: val as undefined };
      const childNode = await this.storage.readPage(childAddr, this._childCtor);
      childNode.parent = node;
      childNode.posInParent = pos;
      node = childNode;
    }
  }

  async insert(val: T) {
    const { found, node, pos } = await this.findIndexRecursive(
      val.key as KeyOf<T>,
    );
    const dirtyNode = node.getDirty(false);
    dirtyNode.insertAt(pos, val);
    dirtyNode.postChange();
  }

  async set(
    key: KeyOf<T>,
    val: T | null,
    allowChange: boolean | "change-only",
  ) {
    const { found, node, pos, val: oldValue } = await this.findIndexRecursive(
      key,
    );
    let action: "added" | "removed" | "changed" | "noop" = "noop";

    if (node._newerCopy) {
      throw new BugError("BUG: set() -> findIndex() returns old copy.");
    }

    if (val != null) {
      const dirtyNode = node.getDirty(false);
      if (found) {
        if (!allowChange) throw new AlreadyExistError("key already exists");
        dirtyNode.setKey(pos, val);
        action = "changed";
      } else {
        if (allowChange === "change-only") {
          throw new NotExistError("key doesn't exists");
        }
        dirtyNode.insertAt(pos, val);
        action = "added";
      }
      dirtyNode.postChange();
    } else {
      if (found) {
        const dirtyNode = node.getDirty(false);
        dirtyNode.spliceKeys(pos, 1);
        dirtyNode.postChange();
        action = "removed";
      } // else noop
    }
    return { action, oldValue: oldValue ?? null };
  }

  insertAt(pos: number, key: T, leftChild: PageAddr = 0) {
    this.spliceKeys(pos, 0, key, leftChild);
  }

  /**
   * Finish copy-on-write on this node and parent nodes.
   * Also split this node if the node is overflow.
   */
  postChange() {
    if (this._newerCopy) throw new BugError("BUG: postChange() on old copy.");
    if (!this.dirty) throw new BugError("BUG: postChange() on non-dirty page.");
    if (this.freeBytes < 0) {
      if (this.keys.length <= 2) {
        throw new Error("Not implemented");
      }
      // console.log('spliting node with key count:', this.keys.length);
      // console.log(this.keys.length, this.children.length);

      // split this node
      const leftSib = new this._childCtor(this.storage).getDirty(true);
      const leftCount = Math.floor(this.keys.length / 2);
      const leftKeys = this.spliceKeys(0, leftCount);
      leftSib.setKeys(leftKeys[0], leftKeys[1]);
      const [[middleKey], [middleLeftChild]] = this.spliceKeys(0, 1);
      leftSib.setChild(leftCount, middleLeftChild);

      if (this.parent) {
        // insert the middle key with the left sibling to parent
        this.getDirty(true);
        this.getParentDirty();
        this.parent.setChild(this.posInParent!, this.addr);
        this.parent.insertAt(this.posInParent!, middleKey, leftSib.addr);
        this.parent.postChange();
        //          ^^^^^^^^^^ makeDirtyToRoot() inside
      } else {
        // make this node a parent of two nodes...
        const rightChild = new this._childCtor(this.storage).getDirty(true);
        rightChild.setKeys(this.keys, this.children);
        this.setKeys([middleKey], [leftSib.addr, rightChild.addr]);
        this.getDirty(true);
        this.makeDirtyToRoot();
      }
    } else {
      this.getDirty(true);
      if (this.parent) {
        this.makeDirtyToRoot();
      }
    }
  }

  getParentDirty(): NodePage<T> {
    return this.parent = this.parent!.getDirty(true);
  }

  makeDirtyToRoot() {
    if (!this.dirty) {
      throw new BugError("BUG: makeDirtyToRoot() on non-dirty page");
    }
    let up = this as NodePage<T>;
    while (up.parent) {
      if (up.parent.dirty) break;
      const upParent = up.parent = up.parent.getDirty(true);
      upParent!.children[up.posInParent!] = up.addr;
      up = upParent;
    }
  }

  override _debugView() {
    return {
      ...super._debugView(),
      parentAddr: this.parent?.addr,
      posInParent: this.posInParent,
      keys: this.keys,
    };
  }

  protected override _writeContent(buf: Buffer) {
    super._writeContent(buf);
    buf.writeU16(this.keys.length);
    for (let i = 0; i < this.keys.length; i++) {
      this.keys[i].writeTo(buf);
    }
    for (let i = 0; i < this.children.length; i++) {
      buf.writeU32(this.children[i]);
    }
  }
  protected override _readContent(buf: Buffer) {
    super._readContent(buf);
    const keyCount = buf.readU16();
    const posBefore = buf.pos;
    for (let i = 0; i < keyCount; i++) {
      this.keys.push(this._readValue(buf));
    }
    const childrenCount = keyCount ? keyCount + 1 : 0;
    for (let i = 0; i < childrenCount; i++) {
      this.children.push(buf.readU32());
    }
    this.freeBytes -= buf.pos - posBefore;
  }
  protected override _copyTo(page: this) {
    super._copyTo(page);
    page.parent = this.parent;
    page.posInParent = this.posInParent;
    page.keys = [...this.keys];
    page.children = [...this.children];
    page.freeBytes = this.freeBytes;
  }

  protected abstract _readValue(buf: Buffer): T;
  protected get _childCtor(): PageClass<NodePage<T>> {
    return this._thisCtor;
  }
}

function calcSizeOfKeys<T>(keys: Iterable<IKey<T>>) {
  let sum = 0;
  for (const it of keys) {
    sum += it.byteLength;
  }
  return sum;
}

function buildTreePageClasses<TKey extends IKey<any>>(options: {
  valueReader: (buf: Buffer) => TKey;
  childPageType: PageType;
  topPageType: PageType;
}) {
  class ChildNodePage extends NodePage<TKey> {
    constructor(...args: any[]) {
      super(...(args as [PageStorage]));
    }
    get type(): PageType {
      return options.childPageType;
    }
    protected _readValue(buf: Buffer): TKey {
      return options.valueReader(buf);
    }
    protected override get _childCtor() {
      return ChildNodePage;
    }
  }

  class TopNodePage extends ChildNodePage {
    override get type(): PageType {
      return options.topPageType;
    }
    rev: number = 1;
    count: number = 0;

    override init() {
      super.init();
      this.freeBytes -= 8;
    }

    override _debugView() {
      return {
        ...super._debugView(),
        rev: this.rev,
        count: this.count,
      };
    }

    override _writeContent(buf: Buffer) {
      buf.writeU32(this.rev);
      buf.writeU32(this.count);
      super._writeContent(buf);
    }

    override _readContent(buf: Buffer) {
      this.rev = buf.readU32();
      this.count = buf.readU32();
      super._readContent(buf);
    }

    override _copyTo(page: this) {
      super._copyTo(page as any);
      page.rev = this.rev;
      page.count = this.count;
    }
  }

  return { top: TopNodePage, child: ChildNodePage };
}

function buildSetPageClass<
  T extends ReturnType<typeof buildTreePageClasses>["top"],
>(baseClass: T) {
  class SetPageBase extends baseClass {
    name: string = "";
    lock = new OneWriterLock();

    async enterCoWLock() {
      var node = this.getLatestCopy();
      await node.lock.enterWriter();
      var node2 = node.getDirty(false);
      if (node !== node2) {
        await node2.lock.enterWriter();
        node.lock.exitWriter();
      }
      return node2;
    }

    override _copyTo(page: this) {
      super._copyTo(page as any);
      page.name = this.name;
    }

    override getDirty(addDirty: boolean) {
      var r = super.getDirty(addDirty);
      if (r != this) {
        this.storage.dirtySets.push(r as any);
      }
      return r;
    }
  }
  return SetPageBase;
}

export type KVNodeType = KValue<StringValue, StringValue>;

const { top: SetPageBase, child: RecordsPage } = buildTreePageClasses<
  KValue<StringValue, StringValue>
>({
  valueReader: (buf: Buffer) =>
    KValue.readFrom(buf, StringValue.readFrom, StringValue.readFrom),
  topPageType: PageType.Set,
  childPageType: PageType.Records,
});

export { RecordsPage };

export const SetPage = buildSetPageClass(SetPageBase);
export type SetPage = InstanceType<typeof SetPage>;

export type DocNodeType = DocumentValue;

const { top: DocSetPageBase1, child: DocsPage } = buildTreePageClasses<
  DocNodeType
>({
  valueReader: (buf: Buffer) => DocumentValue.readFrom(buf),
  topPageType: PageType.DocSet,
  childPageType: PageType.DocRecords,
});

const DocSetPageBase2 = buildSetPageClass(DocSetPageBase1);

export class DocSetPage extends DocSetPageBase2 {
  _lastId: any = null;
  _lastIdLen = 5;

  get lastId() {
    return this._lastId;
  }
  set lastId(val) {
    this.freeBytes += this._lastIdLen;
    this._lastId = val;
    this._lastIdLen = Buffer.calcStringSize(JSON.stringify(val));
    this.freeBytes -= this._lastIdLen;
  }

  indexes: Record<string, IndexInfo> = {};

  setIndexes(newIndexes: this["indexes"]) {
    this.freeBytes += calcIndexInfoSize(this.indexes);
    this.freeBytes -= calcIndexInfoSize(newIndexes);
    this.indexes = newIndexes;
  }

  override init() {
    super.init();
    this.freeBytes -= 5 + 1;
  }

  override _writeContent(buf: Buffer) {
    super._writeContent(buf);
    buf.writeString(JSON.stringify(this.lastId));

    const indexKeys = Object.keys(this.indexes);
    buf.writeU8(indexKeys.length);
    for (const k of indexKeys) {
      buf.writeString(k);
      buf.writeString(this.indexes[k].funcStr);
      buf.writeU32(this.indexes[k].addr);
    }
  }

  override _readContent(buf: Buffer) {
    super._readContent(buf);
    this.lastId = JSON.parse(buf.readString());

    const indexCount = buf.readU8();
    const indexBegin = buf.pos;
    for (let i = 0; i < indexCount; i++) {
      const k = buf.readString();
      this.indexes[k] = new IndexInfo(
        buf.readString(),
        buf.readU32(),
        null,
      );
    }
    this.freeBytes -= buf.pos - indexBegin;
  }

  override _copyTo(other: this) {
    super._copyTo(other);
    other._lastId = this._lastId;
    other._lastIdLen = this._lastIdLen;
    other.indexes = this.indexes;
  }
}

export class IndexInfo {
  constructor(
    public funcStr: string,
    public addr: PageAddr,
    public cachedFunc: null | ((doc: any) => any),
  ) {
  }

  get func() {
    if (!this.cachedFunc) {
      this.cachedFunc = (1, eval)(this.funcStr);
    }
    return this.cachedFunc!;
  }
}

function calcIndexInfoSize(indexes: Record<string, IndexInfo>) {
  let size = 0;
  for (const key in indexes) {
    if (Object.prototype.hasOwnProperty.call(indexes, key)) {
      const info = indexes[key];
      size += Buffer.calcStringSize(key) + Buffer.calcStringSize(info.funcStr) +
        4;
    }
  }
  return size;
}

const { top: IndexTopPage, child: IndexPage } = buildTreePageClasses<
  KValue<JSONValue, JSONValue>
>({
  valueReader: (buf: Buffer) =>
    KValue.readFrom(buf, JSONValue.readFrom, JSONValue.readFrom),
  topPageType: PageType.IndexTop,
  childPageType: PageType.Index,
});

export { IndexTopPage };
// export type IndexTopPage = InstanceType<typeof IndexTopPage>;

export type SetPageAddr = UIntValue;

export class RootTreeNode extends NodePage<KValue<StringValue, SetPageAddr>> {
  get type(): PageType {
    return PageType.RootTreeNode;
  }
  protected _readValue(buf: Buffer): KValue<StringValue, UIntValue> {
    return KValue.readFrom(buf, StringValue.readFrom, UIntValue.readFrom);
  }
  protected override get _childCtor() {
    return RootTreeNode;
  }
}

/**
 * SuperPage, also the root of RootTree.
 */
export class SuperPage extends RootTreeNode {
  override get type(): PageType {
    return PageType.Super;
  }

  version: number = 1;
  rev: number = 1;
  prevSuperPageAddr: PageAddr = 0;
  setCount: number = 0;

  override init() {
    super.init();
    this.freeBytes -= 4 * 4;
  }
  override _writeContent(buf: Buffer) {
    super._writeContent(buf);
    buf.writeU32(this.version);
    buf.writeU32(this.rev);
    buf.writeU32(this.prevSuperPageAddr);
    buf.writeU32(this.setCount);
  }
  override _readContent(buf: Buffer) {
    super._readContent(buf);
    this.version = buf.readU32();
    this.rev = buf.readU32();
    this.prevSuperPageAddr = buf.readU32();
    this.setCount = buf.readU32();
  }
  protected override _copyTo(other: this) {
    super._copyTo(other);
    other.rev = this.rev + 1;
    other.version = this.version;
    other.prevSuperPageAddr = this.prevSuperPageAddr;
    other.setCount = this.setCount;
  }
  override getDirty(addDirty: boolean) {
    var dirty = this.storage.superPage = super.getDirty(false);
    return dirty;
  }
  override _debugView() {
    return {
      ...super._debugView(),
      rev: this.rev,
      version: this.version,
      setCount: this.setCount,
    };
  }
}
