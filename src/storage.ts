import { Buffer } from "./buffer.ts";
import { BugError, NotExistError } from "./errors.ts";
import { LRUMap } from "./lru.ts";
import {
  DataPage,
  Page,
  PageAddr,
  PageClass,
  PAGESIZE,
  SetPage,
  SuperPage,
} from "./page.ts";
import { Runtime, RuntimeFile } from "./runtime.ts";
import { Node } from "./tree.ts";
import { OneWriterLock, TaskQueue } from "./util.ts";
import {
  IValue,
  KeyComparator,
  KValue,
  PageOffsetValue,
  StringValue,
  UIntValue,
  ValueType,
} from "./value.ts";

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

  /** Next address number that will be used for the next dirty page (being passed to `addDirty()`). */
  nextAddr: number = 0;

  /** The latest SuperPage, might be dirty. */
  superPage: SuperPage | undefined = undefined;

  /** Keep a reference to the latest clean/on-disk SuperPage. For concurrent querying and snapshop. */
  cleanSuperPage: SuperPage | undefined = undefined;

  /** When a SetPage is dirty, it will be added into here. */
  dirtySets: SetPage[] = [];

  /** Queue for committed pages being written to disk. */
  deferWritingQueue = new TaskQueue();

  /** last dirty DataPage used by addData */
  dataPage: DataPage | undefined = undefined;
  dataPageBuffer: Buffer | undefined = undefined;

  /** The last addr commited. */
  get cleanAddr() {
    return this.cleanSuperPage?.addr ?? 0;
  }

  /** The last addr commited and written to the file. */
  writtenAddr: number = 0;

  perfCounter = new PageStorageCounter();

  async init() {
    const lastAddr = await this._getLastAddr();
    if (lastAddr == 0) {
      this.superPage = new SuperPage(this).getDirty(true);
      await this.commit(true);
    } else {
      this.nextAddr = lastAddr;
      // try read the last page as super page
      let rootAddr = lastAddr - 1;
      while (rootAddr >= 0) {
        try {
          const page = await this.readPage(rootAddr, SuperPage, true);
          if (!page) {
            rootAddr--;
            continue;
          }
          this.superPage = page;
          this.cleanSuperPage = page;
          this.writtenAddr = page.addr;
          break;
        } catch (error) {
          console.error(error);
          console.info(
            "[RECOVERY] trying read super page from addr " + (--rootAddr),
          );
        }
      }
      if (rootAddr < 0) {
        throw new Error("Failed to open database");
      }
    }
  }

  readPage<T extends Page>(
    addr: PageAddr,
    type: PageClass<T>,
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
      return Promise.resolve(cached as T);
      // .then((cached) => {
      //   if (Object.getPrototypeOf(cached) !== type.prototype) {
      //     throw new BugError(
      //       "BUG: page type from cached mismatched: " +
      //         Runtime.inspect({ cached, type }),
      //     );
      //   }
      //   return cached;
      // });
    }
    if (addr < 0 || addr >= this.nextAddr) {
      throw new Error("Invalid page addr " + addr);
    }
    this.perfCounter.acutalPageReads++;
    const buffer = new Uint8Array(PAGESIZE);
    const promise = this._readPageBuffer(addr, buffer).then(() => {
      const page = new type(this);
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
    const cleanCacheSize = cache.size -
      (this.nextAddr - 1 - this.writtenAddr);
    if (limit > 0 && cleanCacheSize > limit) {
      let deleteCount = cleanCacheSize - limit * 3 / 4;
      let deleted = 0;
      for (const page of cache.valuesFromOldest()) {
        if (page instanceof Page && page.addr <= this.writtenAddr) {
          // console.info('clean ' + page.type + ' ' + page.addr);
          this.perfCounter.cacheCleans++;
          // It's safe to remove on iterating.
          cache.delete(page.addr);
          if (++deleted == deleteCount) break;
        }
      }
    }
  }

  addDirty(page: Page) {
    if (page.hasAddr) {
      if (page.dirty) {
        console.info("re-added dirty", page.type, page.addr);
        return;
      } else {
        throw new Error("Can't mark on-disk page as dirty");
      }
    }
    page.addr = this.nextAddr++;
    this.dirtyPages.push(page);
    this.getCacheForPage(page).set(page.addr, page);
  }

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

  createDataPage(continued: boolean) {
    const prev = this.dataPage;
    this.dataPage = new DataPage(this);
    this.dataPage.createBuffer();
    this.dataPageBuffer = new Buffer(this.dataPage.buffer!, 0);
    this.addDirty(this.dataPage);
    if (continued) prev!.next = this.dataPage.addr;
  }

  async commitMark() {
    if (!this.superPage) throw new Error("superPage does not exist.");
    if (this.dirtySets.length) {
      const rootTree = new Node(this.superPage);
      for (const set of this.dirtySets) {
        if (set._newerCopy) {
          console.info(this.dirtySets.map((x) => [x.addr, x.prefixedName]));
          console.info("dirtySets length", this.dirtySets.length);
          throw new Error("non-latest page in dirtySets");
        }
        set.getDirty(true);
        try {
          await rootTree.set(
            new KeyComparator(new StringValue(set.prefixedName)),
            new KValue(
              new StringValue(set.prefixedName),
              new UIntValue(set.addr),
            ),
            "change-only",
          );
        } catch (error) {
          if (error instanceof NotExistError) {
            // This set is deleted.
            // TODO: remove dirty pages of this set.
            continue;
          }
          throw error;
        }
      }
      this.dirtySets = [];
    }
    if (!this.superPage.dirty) {
      if (this.dirtyPages.length == 0) {
        // console.log("Nothing to commit");
        return [];
      } else {
        throw new Error("super page is not dirty");
      }
    }
    this.dataPage = undefined;
    this.dataPageBuffer = undefined;
    if (this.cleanSuperPage) {
      this.superPage.prevSuperPageAddr = this.cleanSuperPage.addr;
    }
    this.addDirty(this.superPage);
    // console.log(
    //   "commit",
    //   this.dirtyPages
    //     .length + " pages",
    //   // .map(x => x._debugView())
    //   // .map(x => [x.addr, x.type])
    // );
    for (const page of this.dirtyPages) {
      page.dirty = false;
    }
    const currentDirtyPages = this.dirtyPages;
    this.dirtyPages = [];
    this.cleanSuperPage = this.superPage;
    return currentDirtyPages;
  }

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
     * `"final-only"` (default): call the fsync once only after final writing.
     * This ensures the consistency on some systems.
     *
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
     *
     * Since this DB engine is log-structured, the DB file is like a write-ahead-log,
     * and the SuperPage is always in the end, so only call the "final" fsync or not using fsync at all
     * is probably okay on some FileSystems (esp. on Btrfs).
     */
  fsync: "final-only" | "strict" | boolean = "final-only";

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

      this.writtenAddr = beginAddr + combined - 1;
      if (i % TOTAL_CACHE_LIMIT === TOTAL_CACHE_LIMIT - 1) {
        this.checkCache();
      }

      // Assuming the last item in `pages` is the SuperPage.
      if (i === pagesLen - 2 && this.fsync && this.fsync !== "final-only") {
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
