export const PAGESIZE = getPageSize() || 8192;

export const KEYSIZE_LIMIT = Math.floor(PAGESIZE / 4);

function getPageSize() {
  try {
    const val = Runtime.env.get("BTRDB_PAGESIZE");
    if (!val) return null;
    const num = parseInt(val);
    if (isNaN(num)) {
      console.error("BTRDB_PAGESIZE: expected an integer");
      return null;
    }
    return num;
  } catch (error) {
    return null;
  }
}

import { Buffer } from "../utils/buffer.ts";
import { debug_ref, debugLog } from "../utils/debug.ts";
import { BugError } from "../utils/errors.ts";
import { Runtime } from "../utils/runtime.ts";
import { PageStorage } from "../pages/storage.ts";
import { OneWriterLock } from "../utils/util.ts";
import {
  IKey,
  IValue,
  JSValue,
  KValue,
  PageOffsetValue,
  StringValue,
  UIntValue,
} from "../utils/value.ts";

export type PageAddr = number;

export type InlinablePage<T> = PageAddr | T;

export enum PageType {
  Super = 0,
  Root,
  RootTreeNode,
  Set,
  Records,
  DocSet,
  DocRecords,
  IndexTop,
  Index,
  Data,
  Ref = 0xfe,
  FreeSpaceRef = 0xff,
}

export interface PageClass<T extends Page> {
  new (storage: PageStorage): T;
}

let instanceId = 1;

export abstract class Page {
  storage: PageStorage;
  addr: PageAddr = -1;
  _instanceId = instanceId++;
  abstract get type(): PageType;

  constructor(storage: PageStorage) {
    this.storage = storage;
    this.init();
  }

  /** Should not change pages on disk, we should always copy on write */
  dirty = true;

  get hasAddr() {
    return this.addr != -1;
  }

  _newerCopy: this | null = null;
  _discard = false;

  /** Should be maintained by the page when changing data */
  freeBytes: number = PAGESIZE - 4;

  init() {}

  /**
   * Create a dirty copy of this page or return this page if it's already dirty.
   * @param addDirty {boolean} whether to assign the page address
   */
  getDirty(): this {
    if (this.hasNewerCopy()) {
      throw new BugError("getDirty on out-dated page");
    }
    if (this.dirty) {
      return this;
    } else {
      let dirty = new this._thisCtor(this.storage);
      dirty.dirty = true;
      this._copyTo(dirty);
      this._newerCopy = dirty;
      return dirty;
    }
  }

  /** getDirty() and ensure it has an address. */
  async getDirtyWithAddr(): Promise<this> {
    if (this.hasNewerCopy()) {
      throw new BugError("getDirty on out-dated page");
    }
    if (this.dirty) {
      if (!this.hasAddr) await this.storage.addDirty(this);
      return this;
    } else {
      let dirty = new this._thisCtor(this.storage);
      dirty.dirty = true;
      this._copyTo(dirty);
      this._newerCopy = dirty;
      await this.storage.addDirty(dirty);
      return dirty;
    }
  }

  removeDirty() {
    if (this.hasNewerCopy()) {
      throw new BugError("removeDirty on out-dated page");
    }
    if (!this.dirty) throw new BugError("removeDirty on non-dirty page");
    this._discard = true;
    // TODO
  }

  /** It is called when the refcount decreased to 0 */
  unref() {
    debug_ref &&
      debugLog(PageType[this.type], this.addr, "unref", [...this.getRefs()]);
    for (const it of this.getRefs()) {
      this.storage.changeRefCount(it, -1);
    }
  }

  /** It is called when the refcount increased to 1 */
  beref() {
    debug_ref &&
      debugLog(PageType[this.type], this.addr, "beref", [...this.getRefs()]);
    for (const it of this.getRefs()) {
      this.storage.changeRefCount(it, 1);
    }
  }

  getRefs(): Iterable<PageAddr> {
    return [];
  }

  hasNewerCopy() {
    if (this._newerCopy) {
      if (this._newerCopy._discard) {
        this._newerCopy = null;
        return false;
      } else {
        return true;
      }
    }
    return false;
  }

  getLatestCopy(): this {
    let p = this;
    while (p._newerCopy && !p._newerCopy._discard) p = p._newerCopy;
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
        `BUG: buffer written (${buf.pos - beginPos}) != space used (${
          PAGESIZE -
          this.freeBytes
        }), addr=${this.addr}`,
      );
    }
  }
  readFrom(buf: Buffer) {
    const beginPos = buf.pos;
    const type = buf.readU8();
    if (type != this.type) {
      throw new Error(
        `Wrong type in disk, should be ${PageType[this.type]}, got ${
          PageType[type]
        }, addr ${this.addr}`,
      );
    }
    if (buf.readU8() != 0) throw new Error("Non-zero reserved field");
    if (buf.readU16() != 0) throw new Error("Non-zero reserved field");
    this._readContent(buf);
    if (buf.pos - beginPos != PAGESIZE - this.freeBytes) {
      throw new BugError(
        `BUG: buffer read (${buf.pos - beginPos}) != space used (${
          PAGESIZE -
          this.freeBytes
        })`,
      );
    }
  }

  _debugView(): any {
    return {
      type: PageType[this.type],
      addr: this.addr,
      dirty: this.dirty,
      newerCopy: this._newerCopy?._debugView(),
    };
  }

  [Runtime.customInspect]() {
    return "Page(" + Runtime.inspect(this._debugView()) + ")";
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
        (newKeys.length + 1 == newChildren.length))
    ) {
      throw new Error("Invalid args");
    }
    if (this.keys) {
      this.freeBytes += calcSizeOfKeys(this.keys) + this.children.length * 4;
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
   */
  spliceKeys(
    pos: number,
    delCount: number,
    key?: T,
    leftChild?: PageAddr,
  ): [deletedKeys: T[], deletedChildren: PageAddr[]] {
    if (leftChild! < 0) throw new BugError("Invalid leftChild");
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
        if (this.children[0] === 0) {
          this.children.pop();
          this.freeBytes += 4;
        }
      }
    }
    this.freeBytes += calcSizeOfKeys(deleted) + delCount * 4;
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

  async readChildPage(pos: number) {
    const childPage = await this.storage.readPage(
      this.children[pos],
      this._childCtor,
    );
    return childPage;
  }

  *getRefs(): Iterable<number> {
    for (const it of this.children) {
      if (it) {
        yield it;
      }
    }
  }

  createChildPage() {
    return new this._childCtor(this.storage);
  }

  override _debugView() {
    return {
      ...super._debugView(),
      keys: this.keys,
    };
  }

  get _childrenPages() {
    return this.children.map((addr) =>
      addr
        ? this.storage.getCacheForPageType(Object.getPrototypeOf(this)).get(
          addr,
        )
        : 0
    );
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
    page.keys = [...this.keys];
    page.children = [...this.children];
    page.freeBytes = this.freeBytes;
  }

  protected abstract _readValue(buf: Buffer): T;
  protected get _childCtor(): PageClass<NodePage<T>> {
    return this._thisCtor;
  }
}

function calcSizeOfKeys(keys: Iterable<IValue>) {
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
    *getRefs(): Iterable<number> {
      yield* super.getRefs();
      for (
        const v of this.keys as unknown as Array<KValue<any, PageOffsetValue>>
      ) {
        yield v.value.addr;
      }
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
    prefixedName: string = "";
    lock = new OneWriterLock();

    override _copyTo(page: this) {
      super._copyTo(page as any);
      page.prefixedName = this.prefixedName;
    }
  }
  return SetPageBase;
}

export type KVNodeType = KValue<JSValue, PageOffsetValue>;

const { top: SetPageBase, child: RecordsPage } = buildTreePageClasses<
  KValue<JSValue, PageOffsetValue>
>({
  valueReader: (buf: Buffer) =>
    KValue.readFrom(buf, JSValue.readFrom, PageOffsetValue.readFrom),
  topPageType: PageType.Set,
  childPageType: PageType.Records,
});

export { RecordsPage };

export const SetPage = buildSetPageClass(SetPageBase);
export type SetPage = InstanceType<typeof SetPage>;

export type DocNodeType = KValue<JSValue, PageOffsetValue>;

const { top: DocSetPageBase1, child: DocsPage } = buildTreePageClasses<
  DocNodeType
>({
  valueReader: (buf: Buffer) =>
    KValue.readFrom(buf, JSValue.readFrom, PageOffsetValue.readFrom),
  topPageType: PageType.DocSet,
  childPageType: PageType.DocRecords,
});

const DocSetPageBase2 = buildSetPageClass(DocSetPageBase1);

export class DocSetPage extends DocSetPageBase2 {
  private _lastId: JSValue = new JSValue(null);

  get lastId() {
    return this._lastId;
  }
  set lastId(val) {
    this.freeBytes += this._lastId.byteLength;
    this._lastId = val;
    this.freeBytes -= this._lastId.byteLength;
  }

  indexes: IndexInfoMap | null = null;
  indexesInfoAddr = new PageOffsetValue(0, 0);
  indexesAddrs: PageAddr[] = [];
  indexesAddrMap: Record<string, PageAddr> = {};

  async setIndexes(newIndexes: IndexInfoMap, map: Record<string, PageAddr>) {
    const addrs = Object.values(map);
    this.freeBytes += this.indexesAddrs.length * 4;
    this.freeBytes -= addrs.length * 4;
    this.indexes = newIndexes;
    this.indexesAddrs = addrs;
    this.indexesInfoAddr = addrs.length == 0
      ? new PageOffsetValue(0, 0)
      : await this.storage.addData(
        new IndexesInfoValue(newIndexes),
      );
    this.indexesAddrMap = map;
  }

  async ensureIndexes() {
    if (!this.indexes) {
      if (this.indexesInfoAddr.addr == 0 && this.indexesInfoAddr.offset == 0) {
        this.indexes = {};
      } else {
        const value = await this.storage.readData(
          this.indexesInfoAddr,
          IndexesInfoValue,
        );
        this.indexes = value.indexes;
        this.indexesAddrMap = Object.fromEntries(
          Object.keys(this.indexes).map((x, i) => [x, this.indexesAddrs[i]]),
        );
      }
    }
    return this.indexes;
  }

  override init() {
    super.init();
    this.freeBytes -= 1 + 1 + 6;
  }

  *getRefs(): Iterable<number> {
    yield* super.getRefs();
    if (this.indexesInfoAddr.addr) {
      yield this.indexesInfoAddr.addr;
    }
    for (const addr of this.indexesAddrs) {
      yield addr;
    }
  }

  override _writeContent(buf: Buffer) {
    super._writeContent(buf);
    this._lastId.writeTo(buf);

    buf.writeU8(this.indexesAddrs.length);
    for (const indexAddr of this.indexesAddrs) {
      buf.writeU32(indexAddr);
    }
    this.indexesInfoAddr.writeTo(buf);
  }

  override _readContent(buf: Buffer) {
    super._readContent(buf);
    this.lastId = JSValue.readFrom(buf);

    const indexCount = buf.readU8();
    for (let i = 0; i < indexCount; i++) {
      this.indexesAddrs.push(buf.readU32());
    }
    this.indexesInfoAddr = PageOffsetValue.readFrom(buf);
    this.freeBytes -= 4 * indexCount;
  }

  override _copyTo(other: this) {
    super._copyTo(other);
    other._lastId = this._lastId;
    other.indexes = this.indexes; // cow on change
    other.indexesInfoAddr = this.indexesInfoAddr; // cow on change
    other.indexesAddrs = [...this.indexesAddrs];
    other.indexesAddrMap = { ...this.indexesAddrMap };
  }
}

class IndexesInfoValue {
  constructor(readonly indexes: IndexInfoMap) {
    let size = 1;
    for (const key in indexes) {
      if (Object.prototype.hasOwnProperty.call(indexes, key)) {
        const info = indexes[key];
        size += Buffer.calcLenEncodedStringSize(key) +
          Buffer.calcLenEncodedStringSize(info.funcStr) +
          1;
      }
    }
    this.byteLength = size;
  }
  byteLength: number;
  writeTo(buf: Buffer) {
    const indexes = Object.entries(this.indexes);
    buf.writeU8(indexes.length);
    for (const [name, info] of indexes) {
      buf.writeString(name);
      buf.writeString(info.funcStr);
      buf.writeU8(+info.unique);
    }
  }
  static readFrom(buf: Buffer) {
    const indexCount = buf.readU8();
    const indexes: any = {};
    for (let i = 0; i < indexCount; i++) {
      const k = buf.readString();
      indexes[k] = new IndexInfo(
        buf.readString(),
        !!buf.readU8(),
        null,
      );
    }
    return new IndexesInfoValue(indexes);
  }
}

export class IndexInfo {
  constructor(
    public funcStr: string,
    public unique: boolean,
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

export type IndexInfoMap = Record<string, IndexInfo>;

export type IndexNodeType = KValue<JSValue, PageOffsetValue>;

const { top: IndexTopPage, child: IndexPage } = buildTreePageClasses<
  IndexNodeType
>({
  valueReader: (buf: Buffer) =>
    KValue.readFrom(buf, JSValue.readFrom, PageOffsetValue.readFrom),
  topPageType: PageType.IndexTop,
  childPageType: PageType.Index,
});

export { IndexTopPage };
// export type IndexTopPage = InstanceType<typeof IndexTopPage>;

export class DataPage extends Page {
  get type(): PageType {
    return PageType.Data;
  }

  next: PageAddr = 0;

  buffer: Uint8Array | null = null;

  override init() {
    super.init();
    this.freeBytes -= 4;
  }

  createBuffer() {
    this.buffer = new Uint8Array(PAGESIZE - 8);
  }

  get usedBytes() {
    return PAGESIZE - this.freeBytes - 8;
  }

  addUsage(len: number) {
    this.freeBytes -= len;
  }

  getRefs(): Iterable<number> {
    return this.next ? [this.next] : [];
  }

  _writeContent(buf: Buffer) {
    super._writeContent(buf);
    buf.writeU32(this.next);
    buf.writeBuffer(this.buffer!.subarray(0, this.usedBytes));
  }
  _readContent(buf: Buffer) {
    super._readContent(buf);
    this.next = buf.readU32();
    this.buffer = buf.buffer.subarray(buf.pos, /* end: */ PAGESIZE);
    buf.pos = PAGESIZE;
    this.freeBytes = 0;
  }
}

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

  *getRefs() {
    yield* super.getRefs();
    // console.info("[root tree refs]", this.addr, this.keys.map(x => x.value.val));
    for (const key of this.keys) {
      yield key.value.val;
    }
  }
}

// For refcount >= 2, map (addr -> refcount)
export class RefPage extends NodePage<KValue<UIntValue, UIntValue>> {
  get type(): PageType {
    return PageType.Ref;
  }
  protected _readValue(buf: Buffer): KValue<UIntValue, UIntValue> {
    return KValue.readFrom(buf, UIntValue.readFrom, UIntValue.readFrom);
  }
  protected override get _childCtor() {
    return RefPage;
  }
  getRefs(): Iterable<number> {
    return [];
  }
}

// For refcount == 0 (free space)
export class FreeSpacePage extends NodePage<UIntValue> {
  get type(): PageType {
    return PageType.FreeSpaceRef;
  }
  protected _readValue(buf: Buffer): UIntValue {
    return UIntValue.readFrom(buf);
  }
  protected override get _childCtor() {
    return FreeSpacePage;
  }
  getRefs(): Iterable<number> {
    return [];
  }
}

/**
 * The root of RootTree, also stores refs to RefTree and FreeTree.
 */
export class RootPage extends RootTreeNode {
  override get type(): PageType {
    return PageType.Root;
  }

  version: number = 1;
  rev: number = 1;
  prevRootPageAddr: PageAddr = 0;
  setCount: number = 0;
  refTreeAddr: PageAddr = 0;
  freeTreeAddr: PageAddr = 0;
  size: number = 0;

  override init() {
    super.init();
    this.freeBytes -= 12 + 7 * 4;
  }
  override _writeContent(buf: Buffer) {
    buf.writeString("BtrdbSuper_");
    buf.writeU32(this.version);
    buf.writeU32(this.rev);
    buf.writeU32(this.prevRootPageAddr);
    buf.writeU32(this.setCount);
    buf.writeU32(this.refTreeAddr);
    buf.writeU32(this.freeTreeAddr);
    buf.writeU32(this.size);
    super._writeContent(buf);
  }
  override _readContent(buf: Buffer) {
    if (buf.readString() != "BtrdbSuper_") {
      throw new Error("Invalid SuperPage signature");
    }
    this.version = buf.readU32();
    if (this.version != 1) {
      throw new Error(`Unsupported SuperPage version ${this.version}`);
    }
    this.rev = buf.readU32();
    this.prevRootPageAddr = buf.readU32();
    this.setCount = buf.readU32();
    this.refTreeAddr = buf.readU32();
    this.freeTreeAddr = buf.readU32();
    this.size = buf.readU32();
    super._readContent(buf);
  }
  protected override _copyTo(other: this) {
    super._copyTo(other);
    other.rev = this.rev + 1;
    other.version = this.version;
    other.prevRootPageAddr = this.prevRootPageAddr;
    other.setCount = this.setCount;
    other.refTreeAddr = this.refTreeAddr;
    other.freeTreeAddr = this.freeTreeAddr;
    other.size = this.size;
  }
  override getDirty() {
    var dirty = this.storage.rootPage = super.getDirty();
    return dirty;
  }
  override getDirtyWithAddr(): Promise<this> {
    return Promise.resolve(this.getDirty());
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

/**
 * The SuperPage stores refs to RootTree.
 * It's the only page to be overwritten without CoW.
 */
export class SuperPage extends Page {
  get type(): PageType {
    return PageType.Super;
  }

  rootPageAddr: PageAddr = 0;
  prevRootPageAddr: PageAddr = 0;

  init() {
    this.freeBytes -= 31 + 2 * 4;
  }
  _writeContent(buf: Buffer) {
    buf.writeString("This is btrdb file version 1.\n");
    buf.writeU32(this.rootPageAddr);
    buf.writeU32(this.prevRootPageAddr);
  }
  _readContent(buf: Buffer) {
    if (buf.readString() != "This is btrdb file version 1.\n") {
      throw new Error("Invalid btrdb super page signature");
    }
    this.rootPageAddr = buf.readU32();
    this.prevRootPageAddr = buf.readU32();
  }
}

export const pageTypeMap: Record<number, typeof Page> = {
  0: SuperPage,
  1: RootPage,
  2: RootTreeNode,
  3: SetPage,
  4: RecordsPage,
  5: DocSetPage,
  6: DocsPage,
  7: IndexTopPage,
  8: IndexPage,
  9: DataPage,
  0xfe: RefPage,
  0xff: FreeSpacePage,
};
