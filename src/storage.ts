import { Buffer } from "./buffer.ts";
import {
  Page,
  PageAddr,
  PageClass,
  PAGESIZE,
  PageType,
  SetPage,
  SuperPage,
} from "./page.ts";
import { KValue, StringValue, UIntValue } from "./value.ts";

const CACHE_LIMIT = 16 * 1024;

export abstract class PageStorage {
  cache = new Map<PageAddr, Page | Promise<Page>>();

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

  async init() {
    const lastAddr = await this._getLastAddr();
    if (lastAddr == 0) {
      this.superPage = new SuperPage(this).getDirty(true);
      await this.commit();
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
          this.cleanSuperPage = this.superPage;
          break;
        } catch (error) {
          console.error(error);
          console.log(
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
    const cached = this.cache.get(addr);
    if (cached) return Promise.resolve(cached as T);
    if (addr < 0 || addr >= this.nextAddr) {
      throw new Error("Invalid page addr " + addr);
    }
    const buffer = new Uint8Array(PAGESIZE);
    const promise = this._readPageBuffer(addr, buffer).then(() => {
      const page = new type(this);
      page.addr = addr;
      if (nullOnTypeMismatch && page.type != buffer[0]) return null;
      page.readFrom(new Buffer(buffer, 0));
      this.cache.set(page.addr, page);
      if (CACHE_LIMIT > 0 && this.cache.size > CACHE_LIMIT) {
        let deleteCount = CACHE_LIMIT / 2;
        for (const [addr, page] of this.cache) {
          if (page instanceof Page && !page.dirty) {
            // It's safe to delete on iterating.
            this.cache.delete(addr);
            if (--deleteCount == 0) break;
          }
        }
      }
      // console.log("readPage", page);
      return page;
    });
    this.cache.set(addr, promise as Promise<Page>);
    return promise;
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
    this.cache.set(page.addr, page);
  }

  async commit() {
    if (!this.superPage) throw new Error("superPage does not exist.");
    if (this.dirtySets.length) {
      for (const set of this.dirtySets) {
        if (set._newerCopy) {
          console.info(this.dirtySets.map((x) => [x.addr, x.name]));
          console.info("dirtySets length", this.dirtySets.length);
          throw new Error("non-latest page in dirtySets");
        }
        set.getDirty(true);
        await this.superPage.set(
          new StringValue(set.name),
          new KValue(new StringValue(set.name), new UIntValue(set.addr)),
          true,
        );
      }
      this.dirtySets = [];
    }
    if (!this.superPage.dirty) {
      if (this.dirtyPages.length == 0) {
        // console.log("Nothing to commit");
        return false;
      } else {
        throw new Error("super page is not dirty");
      }
    }
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
    await this._commit(this.dirtyPages);
    for (const page of this.dirtyPages) {
      page.dirty = false;
    }
    while (this.dirtyPages.pop()) {}
    this.cleanSuperPage = this.superPage;
    return true;
  }

  close() {
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

export class InFileStorage extends PageStorage {
  file: Deno.File | undefined = undefined;

  /**
     * `"final-only"` (default): call the fsync once only after final writing.
     * This ensures the consistency on most systems.
     *
     * `true | "strict"`: call fsync once before SuperPage and once after final writing.
     * This ensures the consistency on all (correctly implemented) systems.
     *
     * `false`: do not call fsync.
     * This should be used on systems with power backup. Also on some FileSystems like Btrfs.
     *
     * Because most of underlying OSes and FileSystems does not guarantee the order of writing on-disk,
     * we need to do "write(file, data); fsync(file); write(file, superPage); fsync(file);" to ensure
     * the writing order.
     * This ensures the consistency on system crash or power loss during the commit.
     *
     * But since this DB engine is log-structured, the DB file is like a write-ahead-log,
     * and the SuperPage is always in the end, so only call the "final" fsync or not using fsync at all
     * is probably okay for most FileSystems.
     */
  fsync: "final-only" | "strict" | boolean = "final-only";

  async openPath(path: string) {
    if (this.file) throw new Error("Already opened a file.");
    this.file = await Deno.open(path, {
      read: true,
      write: true,
      create: true,
    });
  }
  protected async _readPageBuffer(
    addr: number,
    buffer: Uint8Array,
  ): Promise<void> {
    await this.file!.seek(addr * PAGESIZE, Deno.SeekMode.Start);
    for (let i = 0; i < PAGESIZE;) {
      const nread = await this.file!.read(buffer.subarray(i));
      if (nread === null) throw new Error("Unexpected EOF");
      i += nread;
    }
  }
  protected async _commit(pages: Page[]): Promise<void> {
    // If fsync is enable (which used to ensure the data is correctly written on-disk),
    // to finish a commit, usually there are 4 steps to do:
    // 1) Write pages except the SuperPage.
    // 2) Call fsync(), to ensure them being written on-disk before we start writing the SuperPage.
    // 3) Write the SuperPage.
    // 4) Call fsync().

    const buffer = new Buffer(new Uint8Array(PAGESIZE), 0);
    for (let i = 0; i < pages.length; i++) {
      const page = pages[i];
      page.writeTo(buffer);
      await this.file!.seek(page.addr * PAGESIZE, Deno.SeekMode.Start);
      for (let i = 0; i < buffer.pos;) {
        const nwrite = await this.file!.write(buffer.buffer.subarray(i));
        if (nwrite <= 0) {
          throw new Error("Unexpected return value of write(): " + nwrite);
        }
        i += nwrite;
      }
      // console.info("written page addr", page.addr);
      buffer.buffer.set(InFileStorage.emptyBuffer, 0);
      buffer.pos = 0;

      // Assuming the last item in `pages` is the SuperPage.
      if (i === pages.length - 2 && this.fsync && this.fsync !== "final-only") {
        // Call fsync() before the SuperPage
        await Deno.fdatasync(this.file!.rid);
      }
    }
    if (this.fsync) {
      // Call the fsync() second time to finish the commit.
      await Deno.fdatasync(this.file!.rid);
    }
  }
  protected async _getLastAddr() {
    return Math.floor(await this.file!.seek(0, Deno.SeekMode.End) / 4096);
  }
  protected _close() {
    this.file!.close();
  }
  private static readonly emptyBuffer = new Uint8Array(PAGESIZE);
}
