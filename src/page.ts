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
        this.init();
    }

    /** Should not change pages on disk, we should always copy on write */
    onDisk: boolean = false;

    /** Should be maintained by the page when changing data */
    freeBytes: number = PAGESIZE;

    init() { }

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
    init() {
        super.init();
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
    parent?: this = undefined;
    posInParent?: number = undefined;
    keys: T[] = [];
    children: PageAddr[] = [];

    init() {
        super.init();
        this.freeBytes -= 4; // keysCount
    }

    setKeys(newKeys: T[], newChildren: PageAddr[]) {
        if (!((newKeys.length == 0 && newChildren.length == 0)
            || (newKeys.length + 1 == newChildren.length)))
            throw new Error("Invalid args");
        if (this.keys) {
            this.freeBytes += calcSizeOfKeys(this.keys) + this.children.length * 4;
        }
        if (newKeys) {
            this.freeBytes -= calcSizeOfKeys(newKeys) + newChildren.length * 4;
        }
        this.keys = newKeys;
        this.children = newChildren;
    }

    /** @returns `delCount` keys and `delCount` children. */
    spliceKeys(pos: number, delCount: number, key?: T, leftChild?: PageAddr)
        : [deletedKeys: T[], deletedChildren: PageAddr[]] {
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
            deletedChildren = this.children.splice(pos, delCount, 0);
            if (delCount && this.keys.length == 0) {
                this.freeBytes += 4;
                if (this.children.pop()! != 0) throw new Error("Not implemented");
            }
        }
        this.freeBytes += calcSizeOfKeys(deleted) + delCount * 4;
        if (this.keys.length) this.freeBytes -= 4;
        return [deleted, deletedChildren];
    }

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

    async findIndexRecursive(key: T): Promise<[found: boolean, node: this, pos: number]> {
        let node = this;
        while (true) {
            const [found, pos] = this.findIndex(key);
            if (found) return [found, node, pos];
            const childAddr = this.children[pos];
            if (!childAddr) return [false, node, pos];
            const childNode = await this.storage.readPage(childAddr, this.classCtor);
            childNode.parent = node;
            childNode.posInParent = pos;
            node = childNode;
        };
    }

    async insert(key: T) {
        const [found, node, pos] = await this.findIndexRecursive(key);
        node.insertAt(pos, key);
    }

    insertAt(pos: number, key: T, leftChild: PageAddr = 0) {
        const node = this.getDirty();
        node.keys.splice(pos, 0, key);
        node.children.splice(pos, 0, leftChild);
        if (node.children.length == 1) node.children.push(0);
        node.freeBytes -= key.byteLength + 4;

        if (node.freeBytes < 0) {
            if (node.keys.length <= 2) {
                throw new Error("Not implemented");
            }

            // split node
            const leftSib = new this.classCtor(this.storage).getDirty();
            const leftCount = node.keys.length / 2;
            leftSib.keys = node.keys.splice(0, leftCount + 1);
            const middleKey = leftSib.keys.pop()!;
            leftSib.children = node.children.splice(0, leftCount);
            leftSib.children.push(0);
            const sizeDelta = calcSizeOfKeys(leftSib.keys) + leftSib.children.length * 4;
            leftSib.freeBytes -= sizeDelta + 4;
            node.freeBytes += sizeDelta + middleKey.byteLength + 4;

            // TODO
            if (node.parent) {
                // insert the middle key with the left sibling to parent
                node.parent = node.parent.getDirty();
                node.parent.insertAt(node.posInParent!, middleKey, leftSib.addr);
                //          ^^^^^^^^ made dirty inside
            } else {
                // make `node` a parent of two nodes...
                const newChild = new this.classCtor(this.storage);
                const keysSize = calcSizeOfKeys(node.keys);
                newChild.keys = node.keys;
                newChild.children = node.children;
                newChild.freeBytes -= keysSize + (node.keys.length + 1) * 4
                // TODO
            }
        } else {
            node.makeParentsDirty();
        }
    }

    getDirty(): this {
        if (this._dirty) return this;
        let dirty = this;
        if (this.onDisk) {
            dirty = new this.classCtor(this.storage);
            this._copyTo(dirty);
        }
        this.storage.addDirty(dirty);
        return dirty;
    }

    makeParentsDirty() {
        if (!this._dirty) throw new Error("Invalid operation");
        let up = this;
        while (up.parent) {
            if (up.parent._dirty) break;
            const upParent = up.parent = up.parent.getDirty();
            upParent!.children[up.posInParent!] = up.addr;
            up = upParent;
        }
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
        const posBefore = buf.pos;
        const keyCount = buf.readU16();
        for (let i = 0; i < keyCount; i++) {
            this.keys.push(this.readValue(buf));
        }
        const childrenCount = keyCount ? keyCount + 1 : 0;
        for (let i = 0; i < childrenCount; i++) {
            this.children.push(buf.readU32());
        }
        this.freeBytes -= buf.pos - posBefore;
    }
    protected _copyTo(page: this) {
        super._copyTo(page);
        page.parent = this.parent;
        page.keys = [...this.keys];
        page.children = [...this.children];
        page.freeBytes = this.freeBytes;
    }

    protected abstract readValue(buf: Buffer): T;
    protected abstract classCtor: PageClass<this>;
}

function calcSizeOfKeys<T>(keys: Iterable<IValue<T>>) {
    let sum = 0;
    for (const it of keys) {
        sum += it.byteLength;
    }
    return sum;
}

export class SetPage extends Page {
    rev: number = 1;
    count: number = 0;
}

export class RootPage extends Page {
    rev: number = 1;
}
