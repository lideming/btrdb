import { Buffer, encoder } from "./buffer.ts";
import { PageStorage } from "./storage.ts";
import { IValue } from "./value.ts";


export const PAGESIZE = 4096;

export type PageAddr = number;

export interface PageClass<T extends Page> {
    new(storage: PageStorage): T;
}

export abstract class Page {
    storage: PageStorage;
    addr: PageAddr = -1;
    _dirty = false;

    constructor(storage: PageStorage) {
        this.storage = storage;
    }

    /** Should not change pages on disk, we should always copy on write */
    onDisk: boolean = false;

    /** Should be maintained by the page when changing data */
    freeBytes: number = PAGESIZE;

    get byteLength() { return PAGESIZE; }

    writeTo(buf: Buffer) {
        this._writeContent(buf);
    }
    readFrom(buf: Buffer) {
        this._readContent(buf);
        this.onDisk = true;
    }

    protected _copyTo(page: this) { }
    protected _writeContent(buf: Buffer) { }
    protected _readContent(buf: Buffer) { }
}

export class SuperPage extends Page {
    version: number = 1;
    rootPage: number = 1;
    constructor(stor: PageStorage) {
        super(stor);
        this.freeBytes -= 2 * 4;
    }
    _writeContent(buf: Buffer) {
        super._writeContent(buf);
        buf.writeU32(this.version);
        buf.writeU32(this.rootPage);
    }
    _readContent(buf: Buffer) {
        super._readContent(buf);
        this.version = buf.readU32();
        this.rootPage = buf.readU32();
    }
}

export abstract class NodePage<T extends IValue<T>> extends Page {
    parent?: NodePage<T> = undefined;
    keys: T[] = [];
    children: PageAddr[] = [];

    findIndex(key: T): [found: boolean, pos: number] {
        const keys = this.keys;
        let l = 0, r = keys.length - 1;
        while (l <= r) {
            const m = (l + r) / 2;
            const c = key.compareTo(keys[m]);
            if (c == 0) return [true, m];
            else if (c > 0) l = m + 1;
            else r = m - 1;
        }
        return [false, l];
    }

    protected _writeContent(buf: Buffer) {
        super._writeContent(buf);
        buf.writeU16(this.keys.length);
        for (let i = 0; i < this.keys.length; i++) {
            this.keys[i].writeTo(buf);
        }
        for (let i = 0; i < this.children.length; i++) {
            buf.writeU32(this.children[i]);
        }
    }
    protected _readContent(buf: Buffer) {
        super._readContent(buf);
        const keyCount = buf.readU16();
        for (let i = 0; i < keyCount; i++) {
            this.keys.push(this.readValue(buf));
        }
        const childrenCount = keyCount ? keyCount + 1 : 0;
        for (let i = 0; i < childrenCount; i++) {
            this.children.push(buf.readU32());
        }
    }
    protected _copyTo(page: this) {
        super._copyTo(page);
        page.keys = [...this.keys];
        page.children = [...this.children];
        page.freeBytes = this.freeBytes;
    }

    protected abstract readValue(buf: Buffer): T;
    protected abstract createPage(): NodePage<T>;
}

export class SetPage extends Page {
    rev: number = 1;
    count: number = 0;
}

export class RootPage extends Page {
    rev: number = 1;
}
