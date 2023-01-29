import { Buffer } from "../utils/buffer.ts";
import { debug_allocate, debug_ref, debugLog } from "../utils/debug.ts";
import { BugError } from "../utils/errors.ts";
import { LRUMap } from "../utils/lru.ts";
import {
  DataPage,
  FreeSpacePage,
  Page,
  PageAddr,
  PageClass,
  PAGESIZE,
  PageType,
  pageTypeMap,
  RefPage,
  RootPage,
  SuperPage,
} from "../pages/page.ts";
import { Runtime, RuntimeFile } from "../utils/runtime.ts";
import { Node, NoRefcountNode } from "../pages/tree.ts";
import { OneWriterLock, TaskQueue } from "../utils/util.ts";
import {
  IValue,
  KeyComparator,
  KValue,
  PageOffsetValue,
  UIntValue,
  ValueType,
} from "../utils/value.ts";

const METADATA_CACHE_LIMIT = Math.round(8 * 1024 * 1024 / PAGESIZE);
const DATA_CACHE_LIMIT = Math.round(8 * 1024 * 1024 / PAGESIZE);
const TOTAL_CACHE_LIMIT = METADATA_CACHE_LIMIT + DATA_CACHE_LIMIT;

class PageStorageCounter {
  pageWrites = 0;
  pageFreebyteWrites = 0;

  acutalPageReads = 0;
  cachedPageReads = 0;
  cacheCleans = 0;

  dataAdds = 0;
  dataReads = 0;
}

export abstract class PageStorage {
  /** Use two cache pools for metadata and data */
  metaCache = new LRUMap<PageAddr, Page | Promise<Page>>();
  dataCache = new LRUMap<PageAddr, Page | Promise<Page>>();

  /** Pages that are dirty and pending to be written on-disk. */
  dirtyPages: Page[] = [];
  writingPages = new Set<Page>();

  /** Next address number that will be used for the next dirty page (being passed to `addDirty()`). */
  nextAddr: number = 0;

  superPage: SuperPage | undefined = undefined;

  /** The latest RootPage, might be dirty. */
  rootPage: RootPage | undefined = undefined;

  /** Keep a reference to the latest clean/on-disk RootPage. For concurrent querying and snapshop. */
  cleanRootPage: RootPage | undefined = undefined;

  /** Queue for committed pages being written to disk. */
  deferWritingQueue = new TaskQueue();

  /** last dirty DataPage used by addData */
  dataPage: DataPage | undefined = undefined;
  dataPageBuffer: Buffer | undefined = undefined;

  /** The last addr commited and written to the file. */
  writtenAddr: number = 0;

  pendingRefChange = new Map<PageAddr, number>();

  freeSpace = new Set<number>();

  newAllocated = new Map<PageAddr, Page>();

  perfCounter = new PageStorageCounter();

  /** Read the super page of existing database. Or create a empty database. */
  async init() {
    const lastAddr = await this._getLastAddr();
    if (lastAddr == 0) {
      this.superPage = new SuperPage(this).getDirty(true);
      this.rootPage = new RootPage(this).getDirty(true);
      this.changeRefCount(this.superPage.addr, 1);
      await this.commit(true);
    } else {
      this.nextAddr = lastAddr;
      this.superPage = await this.readPage(0, SuperPage, false);
      try {
        this.rootPage = await this.readPage(
          this.superPage.rootPageAddr,
          RootPage,
          false,
        );
      } catch (error) {
        console.error(error);
        console.info(
          "[RECOVERY] trying read another super page from addr " +
            (this.superPage.prevRootPageAddr),
        );
        this.rootPage = await this.readPage(
          this.superPage.prevRootPageAddr,
          RootPage,
          false,
        );
      }
      this.cleanRootPage = this.rootPage;
      this.writtenAddr = this.rootPage.addr;
      // TODO: make more functions async so we don't need to read the whole free tree
      const freeTree = await this.readPage(
        this.rootPage.freeTreeAddr,
        FreeSpacePage,
      );
      for await (const addr of new Node(freeTree).iterateKeys()) {
        this.freeSpace.add(addr.val);
      }
    }
  }

  readPage<T extends Page>(
    addr: PageAddr,
    type: PageClass<T> | null,
    nullOnTypeMismatch?: false,
  ): Promise<T>;
  readPage<T extends Page>(
    addr: PageAddr,
    type: PageClass<T>,
    nullOnTypeMismatch: true,
  ): Promise<T | null>;
  readPage<T extends Page>(
    addr: PageAddr,
    type: PageClass<T>,
    nullOnTypeMismatch = false,
  ): Promise<T | null> {
    // Return with the cached page or the reading promise in progress.
    // If it's the promise, `Promise.resolve` will return the promise as-is.
    // If cache not hitted, start a reading task and set the promise to `cache`.
    // This method ensures that no duplicated reading will happen.
    const cache = this.getCacheForPageType(type);
    const cached = cache.get(addr);
    if (cached) {
      this.perfCounter.cachedPageReads++;
      return Promise.resolve(cached as T)
        .then((cached) => {
          if (type && Object.getPrototypeOf(cached) !== type.prototype) {
            throw new BugError(
              "BUG: page type from cached mismatched: " +
                Runtime.inspect({
                  cached,
                  expected: PageType[new type(this).type],
                }),
            );
          }
          return cached;
        });
    }
    if (addr < 0 || addr >= this.nextAddr) {
      throw new Error("Invalid page addr " + addr);
    }
    this.perfCounter.acutalPageReads++;
    const buffer = new Uint8Array(PAGESIZE);
    const promise = this._readPageBuffer(addr, buffer).then(() => {
      const page = new (type || pageTypeMap[buffer[0]])(this);
      page.dirty = false;
      page.addr = addr;
      if (nullOnTypeMismatch && page.type != buffer[0]) return null;
      page.readFrom(new Buffer(buffer, 0));
      cache.set(page.addr, page);
      this.checkCache();
      return page;
    });
    cache.set(addr, promise as Promise<Page>);
    return promise;
  }

  getCacheForPageType(type: PageClass<any>) {
    if (type === DataPage) {
      return this.dataCache;
    } else {
      return this.metaCache;
    }
  }

  getCacheForPage(page: Page) {
    if (Object.getPrototypeOf(page) === DataPage.prototype) {
      return this.dataCache;
    } else {
      return this.metaCache;
    }
  }

  checkCache() {
    this._checkCache(METADATA_CACHE_LIMIT, this.metaCache);
    this._checkCache(DATA_CACHE_LIMIT, this.dataCache);
  }

  _checkCache(limit: number, cache: this["metaCache"]) {
    const dirtyCount = this.dirtyPages.length + this.writingPages.size;
    const cleanCacheSize = cache.size - dirtyCount;
    if (limit > 0 && cleanCacheSize > limit) {
      let deleteCount = cleanCacheSize - limit * 3 / 4;
      let deleted = 0;
      for (const page of cache.valuesFromOldest()) {
        if (
          page instanceof Page &&
          !page.dirty && !this.writingPages.has(page)
        ) {
          // console.info('clean ' + PageType[page.type] + ' ' + page.addr);
          this.perfCounter.cacheCleans++;
          // It's safe to remove on iterating.
          cache.delete(page.addr);
          if (++deleted == deleteCount) break;
        }
      }
    }
  }

  /** Mark a page as dirty. Only newly created pages or cloned pages can be marked. */
  addDirty(page: Page) {
    if (page.hasAddr) {
      if (page.dirty) {
        console.info("re-added dirty", PageType[page.type], page.addr);
        return;
      } else {
        throw new Error("Can't mark on-disk page as dirty");
      }
    }
    page.addr = this.allocate(page);
    this.dirtyPages.push(page);
    this.getCacheForPage(page).set(page.addr, page);
  }

  allocate(page: Page) {
    let addr: number;
    // if (false) {
    if (this.freeSpace.size) {
      // Allocate from free space
      [addr] = this.freeSpace;
      this.freeSpace.delete(addr);
      debug_allocate &&
        debugLog(`allocated type(${PageType[page.type]}) (free space)`, addr);
    } else {
      // Grow the backed file
      addr = this.nextAddr++;
      debug_allocate &&
        debugLog(`allocated type(${PageType[page.type]}) (growed)`, addr);
    }
    // console.trace("allocated addr", addr);
    this.newAllocated.set(addr, page);
    return addr;
  }

  /** Change ref count on a page address (delayed ref tree operation before commit) */
  changeRefCount(addr: PageAddr, delta: number) {
    if (typeof addr != "number") throw new BugError("Invalid addr: " + addr);
    if (addr < 0) throw new BugError(`addr < 0 (addr=${addr})`);
    const newDelta = (this.pendingRefChange.get(addr) ?? 0) + delta;
    debug_ref &&
      debugLog("changeRef addr", addr, "delta", delta, "newDelta", newDelta);
    if (newDelta == 0) {
      this.pendingRefChange.delete(addr);
    } else {
      this.pendingRefChange.set(addr, newDelta);
    }
  }

  /** Add a value into data pages and return its address (PageOffsetValue). */
  addData(val: IValue) {
    this.perfCounter.dataAdds++;
    if (!this.dataPage || this.dataPage.freeBytes == 0) {
      this.createDataPage(false);
    }
    const valLength = val.byteLength;
    const headerLength = Buffer.calcEncodedUintSize(valLength);
    const totalLength = headerLength + valLength;

    let pageAddr: number;
    let offset: number;

    if (this.dataPage!.freeBytes >= totalLength) {
      // Can write the whole value into the current data page.
      pageAddr = this.dataPage!.addr;
      offset = this.dataPageBuffer!.pos;
      this.dataPageBuffer!.writeEncodedUint(valLength);
      val.writeTo(this.dataPageBuffer!);
      this.dataPage!.freeBytes -= totalLength;
    } else {
      // We need to split it into pages.
      if (this.dataPage!.freeBytes < headerLength) {
        // If current page even cannot fit the header...
        this.createDataPage(false);
      }
      // Writing header
      pageAddr = this.dataPage!.addr;
      offset = this.dataPageBuffer!.pos;
      this.dataPageBuffer!.writeEncodedUint(valLength);
      this.dataPage!.freeBytes -= headerLength;
      // Make a temporary buffer and write value into it.
      const valBuffer = new Buffer(new Uint8Array(valLength), 0);
      val.writeTo(valBuffer);
      // Start writing to pages...
      let written = 0;
      while (written < valLength) {
        if (this.dataPage!.freeBytes == 0) {
          this.createDataPage(true);
        }
        const toWrite = Math.min(valLength - written, this.dataPage!.freeBytes);
        this.dataPageBuffer!.writeBuffer(
          valBuffer.buffer.subarray(written, written + toWrite),
        );
        written += toWrite;
        this.dataPage!.freeBytes -= toWrite;
      }
    }
    return new PageOffsetValue(pageAddr, offset);
  }

  /** Read a value from data page by a address (PageOffsetValue) */
  async readData<T extends IValue>(
    pageOffset: PageOffsetValue,
    type: ValueType<T>,
  ): Promise<T>;
  async readData<T extends IValue>(
    pageOffset: PageOffsetValue,
    type: null,
  ): Promise<Uint8Array>;
  async readData<T extends IValue>(
    pageOffset: PageOffsetValue,
    type: ValueType<T> | null,
  ) {
    this.perfCounter.dataReads++;
    let page = await this.readPage(pageOffset.addr, DataPage);
    let buffer = new Buffer(page.buffer!, pageOffset.offset);
    const valLength = buffer.readEncodedUint();
    let bufferLeft = buffer.buffer.length - buffer.pos;
    if (valLength <= bufferLeft) {
      return type
        ? type.readFrom(buffer)
        : buffer.buffer.subarray(buffer.pos, valLength);
    } else {
      const valBuffer = new Buffer(new Uint8Array(valLength), 0);
      while (valBuffer.pos < valLength) {
        if (bufferLeft == 0) {
          if (!page.next) throw new BugError("BUG: expected next page.");
          page = await this.readPage(page.next, DataPage);
          buffer = new Buffer(page.buffer!, 0);
          bufferLeft = buffer.buffer.length;
        }
        const toRead = Math.min(bufferLeft, valLength - valBuffer.pos);
        valBuffer.writeBuffer(
          buffer.pos || buffer.buffer.length != toRead
            ? buffer.buffer.subarray(buffer.pos, buffer.pos + toRead)
            : buffer.buffer,
        );
        bufferLeft -= toRead;
      }
      valBuffer.pos = 0;
      return type ? type.readFrom(valBuffer) : valBuffer.buffer;
    }
  }

  /**
   * Create a new data page.
   * @param continued Whether this page is a continued page after the previous one.
   */
  createDataPage(continued: boolean) {
    const prev = this.dataPage;
    this.dataPage = new DataPage(this);
    this.dataPage.createBuffer();
    this.dataPageBuffer = new Buffer(this.dataPage.buffer!, 0);
    this.addDirty(this.dataPage);
    // this.changeRefCount(this.dataPage.addr, 1);
    if (continued) prev!.next = this.dataPage.addr;
  }

  /** Mark all dirty pages as pending-commit. We will write them into disk later.  */
  async commitMark() {
    debug_allocate && debugLog(
      "[commit] pre free space",
      [...this.freeSpace.values()],
      "size",
      this.rootPage?.size,
    );

    if (!this.rootPage) throw new Error("rootPage does not exist.");
    if (!this.rootPage.dirty) {
      if (this.dirtyPages.length == 0) {
        // console.log("Nothing to commit");
        return [];
      } else {
        throw new Error("root page is not dirty");
      }
    }

    this.dataPage = undefined;
    this.dataPageBuffer = undefined;

    this.addDirty(this.rootPage);

    this.changeRefCount(this.rootPage.addr, 1);

    if (this.cleanRootPage) {
      this.changeRefCount(this.cleanRootPage.addr, -1);
      // this.superPage.prevSuperPageAddr = this.cleanSuperPage.addr;
    }

    // Update Ref tree and FreeSpace tree
    let refTree = this.rootPage.refTreeAddr
      ? new NoRefcountNode(
        await this.readPage(this.rootPage.refTreeAddr, RefPage),
      )
      : new NoRefcountNode(new RefPage(this));
    let freeTree = this.rootPage.freeTreeAddr
      ? new NoRefcountNode(
        await this.readPage(this.rootPage.freeTreeAddr, FreeSpacePage),
      )
      : new NoRefcountNode(new FreeSpacePage(this));
    refTree = refTree.getDirty(true);
    freeTree = freeTree.getDirty(true);
    const pendingFreeSpace = new Set<PageAddr>();

    await this.updateRefTree(freeTree, refTree, pendingFreeSpace);

    if (this.newAllocated.size) {
      for (const [addr, page] of this.newAllocated) {
        if (addr >= this.rootPage.size) {
          const vAddr = new UIntValue(addr);
          await freeTree.set(vAddr, vAddr, "no-change");
          debug_allocate &&
            debugLog("discard (free) newAllocated beyond db size", addr);
        } // else it should be already in the free tree
        pendingFreeSpace.add(addr);
      }
      this.newAllocated.clear();

      if (this.pendingRefChange.size) {
        await this.updateRefTree(freeTree, refTree, pendingFreeSpace);
      }
    }

    if (this.pendingRefChange.size) {
      throw new BugError(
        "BUG: pendingRefChange.size > 0 after updateRefTree()",
      );
    }

    this.rootPage.refTreeAddr = refTree.addr;
    this.rootPage.freeTreeAddr = freeTree.addr;

    // update free space cache after tree update,
    // otherwise the free space may be used by the tree immediately.
    for (const addr of pendingFreeSpace) {
      this.freeSpace.add(addr);
      this.metaCache.delete(addr);
      this.dataCache.delete(addr);
    }

    this.rootPage.size = this.nextAddr;

    this.superPage!.prevRootPageAddr = this.superPage!.rootPageAddr;
    this.superPage!.rootPageAddr = this.rootPage.addr;
    if (!this.superPage!.dirty) {
      this.superPage!.dirty = true;
      this.dirtyPages.push(this.superPage!);
    }

    for (const page of this.dirtyPages) {
      page.dirty = false;
    }
    const currentDirtyPages = this.dirtyPages;
    this.dirtyPages = [];
    this.cleanRootPage = this.rootPage;

    for (const page of currentDirtyPages) {
      this.writingPages.add(page);
    }

    debug_allocate && debugLog(
      "[commit] post free space",
      [...this.freeSpace.values()],
      "size",
      this.rootPage?.size,
    );

    return currentDirtyPages;
  }

  private async updateRefTree(
    freeTree: Node<UIntValue>,
    refTree: Node<KValue<UIntValue, UIntValue>>,
    pendingFreeSpace: Set<number>,
  ) {
    for (const [addr, delta] of this.pendingRefChange) {
      debug_ref && debugLog(`[update ref] addr ${addr} delta ${delta}`);
      this.pendingRefChange.delete(addr);
      const vAddr = new UIntValue(addr);
      const isNewAllocated = this.newAllocated.get(addr);
      if (isNewAllocated) {
        debug_allocate && debugLog("remove newAllocated flag addr", addr);
        this.newAllocated.delete(addr);
        isNewAllocated.beref();
      }
      let { found: freefound, node: freenode, pos: freepos, val } =
        await freeTree.findKeyRecursive(vAddr);
      if (freefound) {
        const refcount = 0 + delta;
        if (refcount < 0) {
          throw new BugError(`BUG: refcount ${refcount} < 0`);
        }
        await freenode.deleteAt(freepos);
        debug_ref && debugLog("[free->ref]", addr, refcount);
        pendingFreeSpace.delete(addr);
        if (refcount > 1) {
          await refTree.set(
            new KeyComparator(vAddr),
            new KValue(vAddr, new UIntValue(refcount)),
            "no-change",
          );
        }
        if (!isNewAllocated) {
          const page = await this.readPage(addr, null);
          page.beref();
        }
      } else {
        const vKey = new KeyComparator(vAddr);
        let { found, node, pos, val } = await refTree.findKeyRecursive(
          vKey,
        );
        const refcount = (isNewAllocated ? 0 : (val?.value.val ?? 1)) + delta;
        debug_ref && debugLog("[ref]", addr, refcount);
        if (refcount < 0) {
          this.pendingRefChange.set(addr, delta);
          // console.warn(`BUG?: refcount ${refcount} < 0, moved to end of queue`);
          // continue;
          throw new BugError(`BUG: refcount ${refcount} < 0`);
        }
        if (refcount < 2 && found) {
          await node.deleteAt(pos);
        }
        if (refcount > 1) {
          // debug_ref && debugLog("[shared]", addr, refcount);
          node = node.getDirty(true);
          if (found) {
            node.setKey(pos, new KValue(vAddr, new UIntValue(refcount)));
            node.postChange();
          } else {
            node.insertAt(pos, new KValue(vAddr, new UIntValue(refcount)));
            node.postChange();
          }
        }
        if (refcount == 0) {
          debug_ref && debugLog("[free]", addr);
          freenode = freenode.getDirty(true);
          freenode.insertAt(freepos, vAddr);
          freenode.postChange();
          pendingFreeSpace.add(addr);
          const page = await this.readPage(addr, null);
          page.unref();
        } else if (delta > 0 && refcount === delta) {
          debug_ref && debugLog("[un-free]", addr);
          pendingFreeSpace.delete(addr);
        }
      }
    }
  }

  // private async validateRefTree() {
  // }

  /** Commit all current changes into disk. */
  async commit(waitWriting: boolean) {
    const pages = await this.commitMark();
    this.deferWritingQueue.enqueue({
      run: () => {
        return this._commit(pages);
      },
    });
    if (waitWriting) {
      await this.waitDeferWriting();
    }
    return pages.length > 0;
  }

  rollback() {
    console.info("[rollback]");
    debug_allocate &&
      debugLog("[rollback] pre free space", [...this.freeSpace.values()]);
    if (this.rootPage!.dirty) {
      this.metaCache.delete(this.rootPage!.addr);
      this.rootPage = this.cleanRootPage;
      this.cleanRootPage!._newerCopy = null;
    }
    if (this.dirtyPages.length > 0) {
      for (const page of this.dirtyPages) {
        page._discard = true;
        if (Object.getPrototypeOf(page) == DataPage.prototype) {
          this.dataCache.delete(page.addr);
        } else {
          this.metaCache.delete(page.addr);
        }
        debug_allocate && debugLog("[rollback] discard dirty ", page.addr);
      }
      this.dirtyPages = [];
      this.nextAddr = this.rootPage!.size;
    }
    this.pendingRefChange.clear();
    for (const [addr] of this.newAllocated) {
      this.newAllocated.delete(addr);
      if (addr < this.nextAddr) {
        this.freeSpace.add(addr);
        debug_allocate && debugLog("[rollback] newAllocated free ", addr);
      }
    }
    this.dataPage = undefined;
    this.dataPageBuffer = undefined;

    debug_allocate &&
      debugLog("[rollback] post free space", [...this.freeSpace.values()]);
  }

  waitDeferWriting() {
    return this.deferWritingQueue.waitCurrentLastTask();
  }

  close() {
    if (this.deferWritingQueue.running) {
      throw new Error(
        "Some deferred writing tasks are still running. " +
          "Please `await waitDeferWriting()` before closing.",
      );
    }
    this._close();
  }

  protected abstract _commit(pages: Page[]): Promise<void>;
  protected abstract _readPageBuffer(
    addr: PageAddr,
    buffer: Uint8Array,
  ): Promise<void>;
  protected abstract _getLastAddr(): Promise<number>;
  protected abstract _close(): void;
}

const MAX_COMBINED = 32;

export class InFileStorage extends PageStorage {
  file: RuntimeFile | undefined = undefined;
  filePath: string | undefined = undefined;
  lock = new OneWriterLock();
  commitBuffer = new Buffer(new Uint8Array(PAGESIZE * MAX_COMBINED), 0);

  /**
   * `true | "strict"`: call fsync once before SuperPage and once after final writing.
   * This ensures the consistency on all (correctly implemented) systems.
   *
   * `false`: do not call fsync.
   * This should be used on systems with power backup. Also on some FileSystems like Btrfs.
   *
   * Because the underlying OSes and FileSystems does not guarantee the order of writing on-disk,
   * people usually do "write(file, data); fsync(file); write(file, superPage); fsync(file);"
   * to ensure the writing order.
   * This ensures the consistency on system crash or power loss during the commit.
   */
  fsync: "strict" | boolean = true;

  async openPath(path: string) {
    if (this.file) throw new Error("Already opened a file.");
    this.file = await Runtime.open(path, {
      read: true,
      write: true,
      create: true,
    });
    this.filePath = path;
  }
  protected async _readPageBuffer(
    addr: number,
    buffer: Uint8Array,
  ): Promise<void> {
    await this.lock.enterWriter();
    await this.file!.seek(addr * PAGESIZE, Runtime.SeekMode.Start);
    for (let i = 0; i < PAGESIZE;) {
      const nread = await this.file!.read(buffer.subarray(i));
      if (nread === null) throw new Error("Unexpected EOF");
      i += nread;
    }
    this.lock.exitWriter();
  }
  protected async _commit(pages: Page[]): Promise<void> {
    // If fsync is enable (which used to ensure the data is correctly written on-disk),
    // to finish a commit, usually there are 4 steps to do:
    // 1) Write pages except the SuperPage.
    // 2) Call fsync(), to ensure them being written on-disk before we start writing the SuperPage.
    // 3) Write the SuperPage.
    // 4) Call fsync().

    await this.lock.enterWriter();
    const buffer = this.commitBuffer;
    const pagesLen = pages.length;
    let filePos = -1;
    for (let i = 0; i < pagesLen; i++) {
      const beginAddr = pages[i].addr;
      const beginI = i;
      let combined = 1;
      while (
        i + 2 < pagesLen && pages[i + 1].addr === beginAddr + combined &&
        combined < MAX_COMBINED
      ) {
        i++;
        combined++;
      }
      for (let p = 0; p < combined; p++) {
        buffer.pos = p * PAGESIZE;
        const page = pages[beginI + p];
        page.writeTo(buffer);
        this.perfCounter.pageWrites++;
        this.perfCounter.pageFreebyteWrites += page.freeBytes;
      }
      const targerPos = beginAddr * PAGESIZE;
      if (filePos !== targerPos) {
        await this.file!.seek(targerPos, Runtime.SeekMode.Start);
      }
      const toWrite = combined * PAGESIZE;
      for (let i = 0; i < toWrite;) {
        const nwrite = await this.file!.write(
          buffer.buffer.subarray(i, toWrite),
        );
        if (nwrite <= 0) {
          throw new Error("Unexpected return value of write(): " + nwrite);
        }
        i += nwrite;
      }
      filePos = targerPos + toWrite;
      // console.info("written page addr", page.addr);
      buffer.buffer.set(InFileStorage.emptyBuffer.subarray(0, toWrite), 0);
      buffer.pos = 0;

      for (let p = 0; p < combined; p++) {
        const page = pages[beginI + p];
        this.writingPages.delete(page);
      }

      this.writtenAddr = beginAddr + combined - 1;
      if (i % TOTAL_CACHE_LIMIT === TOTAL_CACHE_LIMIT - 1) {
        this.checkCache();
      }

      // Assuming the last item in `pages` is the SuperPage.
      if (i === pagesLen - 2 && this.fsync) {
        // Call fsync() before the SuperPage
        await Runtime.fdatasync(this.file!.rid);
      }
    }
    if (this.fsync) {
      // Call the final fsync() to finish the commit.
      await Runtime.fdatasync(this.file!.rid);
    }
    this.lock.exitWriter();
  }
  protected async _getLastAddr() {
    return Math.floor(
      await this.file!.seek(0, Runtime.SeekMode.End) / PAGESIZE,
    );
  }
  protected _close() {
    this.file!.close();
  }
  private static readonly emptyBuffer = new Uint8Array(PAGESIZE * MAX_COMBINED);
}

export class InMemoryData {
  pageBuffers: Uint8Array[] = [];
}

export class InMemoryStorage extends PageStorage {
  data: InMemoryData;
  constructor(data: InMemoryData) {
    super();
    this.data = data;
  }
  protected async _commit(pages: Page[]): Promise<void> {
    var buf = new Buffer(null!, 0);
    for (const page of pages) {
      buf.buffer = new Uint8Array(PAGESIZE);
      buf.pos = 0;
      page.writeTo(buf);
      this.data.pageBuffers[page.addr] = buf.buffer;
      this.perfCounter.pageWrites++;
      this.perfCounter.pageFreebyteWrites += page.freeBytes;
    }
  }
  protected async _readPageBuffer(
    addr: number,
    buffer: Uint8Array,
  ): Promise<void> {
    buffer.set(this.data.pageBuffers[addr]);
  }
  protected _getLastAddr(): Promise<number> {
    return Promise.resolve(this.data.pageBuffers.length);
  }
  protected _close(): void {
  }
}
