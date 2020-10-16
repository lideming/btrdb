import { Buffer } from "./buffer.ts";
import { Page, PageAddr, PageClass, PAGESIZE } from "./page.ts";


export abstract class PageStorage {
    cache = new Map<PageAddr, Page>();
    dirtyPages: Page[] = [];
    nextAddr: number = 0;

    readPage<T extends Page>(addr: PageAddr, type: PageClass<T>): Promise<T> {
        const cached = this.cache.get(addr);
        if (cached) return Promise.resolve(cached as T);
        const buffer = new Uint8Array(PAGESIZE);
        return this.readPageBuffer(addr, buffer).then(() => {
            const page = new type(this);
            page.addr = addr;
            page.readFrom(new Buffer(buffer, 0));
            return page;
        });
    }

    addDirty(page: Page) {
        if (page.onDisk) throw new Error("Can't mark on-disk page as dirty");
        if (page._dirty) return;
        page._dirty = true;
        page.addr = this.nextAddr++;
        this.dirtyPages.push(page);
    }

    abstract commit(): Promise<void>;
    abstract readPageBuffer(addr: PageAddr, buffer: Uint8Array): Promise<void>;
}
