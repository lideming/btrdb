import { Buffer } from "./buffer.ts";
import { Page, PageAddr, PageClass, PAGESIZE, SuperPage } from "./page.ts";
import { UIntValue } from "./value.ts";


export abstract class PageStorage {
    cache = new Map<PageAddr, Page>();
    dirtyPages: Page[] = [];
    nextAddr: number = 0;

    superPage: SuperPage | undefined = undefined;
    cleanSuperPage: SuperPage | undefined = undefined;

    async init() {
        const lastAddr = await this._getLastAddr();
        if (lastAddr == 0) {
            this.superPage = new SuperPage(this).getDirty();
            await this.commit();
        } else {
            // try read the last page as super page
            this.superPage = await this.readPage(lastAddr - 1, SuperPage);
        }
    }

    readPage<T extends Page>(addr: PageAddr, type: PageClass<T>): Promise<T> {
        const cached = this.cache.get(addr);
        if (cached) return Promise.resolve(cached as T);
        const buffer = new Uint8Array(PAGESIZE);
        return this._readPageBuffer(addr, buffer).then(() => {
            const page = new type(this);
            page.addr = addr;
            page.readFrom(new Buffer(buffer, 0));
            this.cache.set(page.addr, page);
            console.log("readPage", page);
            return page;
        });
    }

    addDirty(page: Page) {
        if (page.onDisk) throw new Error("Can't mark on-disk page as dirty");
        if (page.dirty) return;
        page.dirty = true;
        page.addr = this.nextAddr++;
        this.dirtyPages.push(page);
        this.cache.set(page.addr, page);
    }

    async commit() {
        console.log('==========COMMIT==========', this.dirtyPages);
        await this._commit(this.dirtyPages);
        for (const page of this.dirtyPages) {
            page.dirty = false;
        }
        while (this.dirtyPages.pop()) { }
        this.cleanSuperPage = this.superPage;
        console.log('==========END COMMIT==========', this.dirtyPages);
    }

    protected abstract _commit(pages: Page[]): Promise<void>;
    protected abstract _readPageBuffer(addr: PageAddr, buffer: Uint8Array): Promise<void>;
    protected abstract _getLastAddr(): Promise<number>;
}

export class InFileStorage extends PageStorage {
    file: Deno.File | undefined = undefined;
    async openPath(path: string) {
        if (this.file) throw new Error("Already opened a file.");
        this.file = await Deno.open(path, { read: true, write: true, create: true });
    }
    protected async _readPageBuffer(addr: number, buffer: Uint8Array): Promise<void> {
        await this.file!.seek(addr * PAGESIZE, Deno.SeekMode.Start);
        for (let i = 0; i < PAGESIZE;) {
            const nread = await this.file!.read(buffer.subarray(i));
            if (nread === null) throw new Error("Unexpected EOF");
            i += nread;
        }
    }
    protected async _commit(pages: Page[]): Promise<void> {
        const buffer = new Buffer(new Uint8Array(PAGESIZE), 0);
        for (const page of pages) {
            page.writeTo(buffer);
            await this.file!.seek(page.addr, Deno.SeekMode.Start);
            for (let i = 0; i < buffer.pos;) {
                const nwrite = await this.file!.write(buffer.buffer.subarray(i));
                if (nwrite <= 0) throw new Error("Unexpected return value of write(): " + nwrite);
                i += nwrite;
            }
            buffer.buffer.set(InFileStorage.emptyBuffer, 0);
            buffer.pos = 0;
        }
    }
    protected async _getLastAddr() {
        return Math.round(await this.file!.seek(0, Deno.SeekMode.End) / 4096);
    }
    private static readonly emptyBuffer = new Uint8Array(PAGESIZE);
}
