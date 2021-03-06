export const PAGESIZE = 4096;

import { Buffer } from "./buffer.ts";
import { PageStorage } from "./storage.ts";
import { IKey, KeyOf, KValue, StringValue, UIntValue } from "./value.ts";

export type PageAddr = number;

export const enum PageType {
    None,
    Super = 1,
    RootTreeNode,
    Set,
}

export interface PageClass<T extends Page> {
    new(storage: PageStorage): T;
}

export abstract class Page {
    storage: PageStorage;
    addr: PageAddr = -1;
    abstract get type(): PageType;

    constructor(storage: PageStorage) {
        this.storage = storage;
        this.init();
    }

    dirty = false;

    /** Should not change pages on disk, we should always copy on write */
    get onDisk() { return this.addr >= 0; }

    /** Should be maintained by the page when changing data */
    freeBytes: number = PAGESIZE - 4;

    init() { }

    /** Create a dirty copy of this page or return this page if it's already dirty. */
    getDirty(addDirty: boolean): this {
        if (this.dirty) return this;
        let dirty = this;
        if (this.onDisk) {
            dirty = new this._thisCtor(this.storage);
            this._copyTo(dirty);
        }
        if (addDirty) this.storage.addDirty(dirty);
        return dirty;
    }

    writeTo(buf: Buffer) {
        const beginPos = buf.pos;
        buf.writeU8(this.type);
        buf.writeU8(0);
        buf.writeU16(0);
        this._writeContent(buf);
        if (buf.pos - beginPos != PAGESIZE - this.freeBytes) {
            throw new Error(`buffer written (${buf.pos - beginPos}) != space used (${PAGESIZE - this.freeBytes})`)
        }
    }
    readFrom(buf: Buffer) {
        const beginPos = buf.pos;
        const type = buf.readU8();
        if (type != this.type) throw new Error(`Wrong type in disk, should be ${this.type}, got ${type}`);
        if (buf.readU8() != 0) throw new Error('Non-zero reserved field');
        if (buf.readU16() != 0) throw new Error('Non-zero reserved field');
        this._readContent(buf);
        if (buf.pos - beginPos != PAGESIZE - this.freeBytes) {
            throw new Error(`buffer read (${buf.pos - beginPos}) != space used (${PAGESIZE - this.freeBytes})`)
        }
    }

    _debugView() {
        return {
            type: this.type,
            addr: this.addr,
            dirty: this.dirty
        };
    }

    protected _copyTo(page: this) { }
    protected _writeContent(buf: Buffer) { }
    protected _readContent(buf: Buffer) { }
    protected get _thisCtor(): PageClass<this> {
        return Object.getPrototypeOf(this).constructor as PageClass<this>;
    }
}

export abstract class NodePage<T extends IKey<unknown>> extends Page {
    parent?: NodePage<T> = undefined;
    posInParent?: number = undefined;
    keys: T[] = [];
    children: PageAddr[] = [];

    init() {
        super.init();
        this.freeBytes -= 2; // keysCount
    }

    setKeys(newKeys: T[], newChildren: PageAddr[]) {
        console.log([newKeys.length, newChildren.length])
        if (!((newKeys.length == 0 && newChildren.length == 0)
            || (newKeys.length + 1 == newChildren.length)
            || (newChildren.length == newKeys.length)
        ))
            throw new Error("Invalid args");
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
            deletedChildren = this.children.splice(pos, delCount);
            if (delCount && this.keys.length == 0) {
                this.freeBytes += 4;
                if (this.children.pop()! != 0) throw new Error("Not implemented");
            }
        }
        this.freeBytes += calcSizeOfKeys(deleted) + delCount * 4;
        return [deleted, deletedChildren];
    }

    setChild(pos: number, child: PageAddr) {
        if (this.children.length <= pos) throw new Error('pos out of range');
        this.children[pos] = child;
    }

    findIndex(key: KeyOf<T>): { found: boolean, pos: number, val: T | undefined } {
        const keys = this.keys;
        let l = 0, r = keys.length - 1;
        while (l <= r) {
            const m = Math.round((l + r) / 2);
            const c = key.compareTo(keys[m].key);
            console.log("compare", key, c == 0 ? '==' : c > 0 ? '>' : '<', keys[m])
            if (c == 0) return { found: true, pos: m, val: keys[m] };
            else if (c > 0) l = m + 1;
            else r = m - 1;
        }
        return { found: false, pos: l, val: undefined };
    }

    async findIndexRecursive(key: KeyOf<T>): Promise<{
        found: boolean, node: NodePage<T>, pos: number, val: T | undefined
    }> {
        let node = this as NodePage<T>;
        while (true) {
            const { found, pos, val } = node.findIndex(key);
            if (found) return { found, node, pos, val };
            const childAddr = node.children[pos];
            if (!childAddr) return { found: false, node, pos, val };
            const childNode = await this.storage.readPage(childAddr, this._childCtor);
            childNode.parent = node;
            childNode.posInParent = pos;
            node = childNode;
        };
    }

    async insert(val: T) {
        const { found, node, pos } = await this.findIndexRecursive(val.key as KeyOf<T>);
        node.insertAt(pos, val);
    }

    insertAt(pos: number, key: T, leftChild: PageAddr = 0) {
        const node = this.getDirty(false);
        node.spliceKeys(pos, 0, key, leftChild);

        if (node.freeBytes < 0) {
            if (node.keys.length <= 2) {
                throw new Error("Not implemented");
            }
            console.log('spliting node with key count:', node.keys.length)
            console.log(node.keys.length, node.children.length);

            // split node
            const leftSib = new this._childCtor(this.storage).getDirty(false);
            const leftCount = node.keys.length / 2;
            const leftKeys = node.spliceKeys(0, leftCount);
            leftSib.setKeys(leftKeys[0], leftKeys[1]);
            const [[middleKey], [middleLeftChild]] = node.spliceKeys(0, 1);
            leftSib.setChild(leftCount, middleLeftChild);

            if (node.parent) {
                // insert the middle key with the left sibling to parent
                leftSib.getDirty(true);
                node.getParentDirty();
                node.parent.insertAt(node.posInParent!, middleKey, leftSib.addr);
                //          ^^^^^^^^ makeDirtyToRoot() inside
            } else {
                // make `node` a parent of two nodes...
                const rightChild = new this._childCtor(this.storage).getDirty(true);
                rightChild.setKeys(node.keys, node.children);
                node.setKeys([middleKey], [leftSib.addr, rightChild.addr]);
                node.getDirty(true);
            }
        } else {
            node.getDirty(true);
            if (node.parent)
                node.makeDirtyToRoot();
        }
    }

    getParentDirty(): NodePage<T> {
        return this.parent = this.parent!.getDirty(true);
    }

    makeDirtyToRoot() {
        if (!this.dirty) throw new Error("Invalid operation");
        let up = this as NodePage<T>;
        while (up.parent) {
            if (up.parent.dirty) break;
            const upParent = up.parent = up.parent.getDirty(true);
            upParent!.children[up.posInParent!] = up.addr;
            up = upParent;
        }
    }

    _debugView() {
        return {
            ...super._debugView(),
            parentAddr: this.parent?.addr,
            posInParent: this.posInParent,
            keys: this.keys
        };
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
    protected _copyTo(page: this) {
        super._copyTo(page);
        page.parent = this.parent;
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

export class SetPage extends Page {
    get type(): PageType { return PageType.Set; }
    rev: number = 1;
    count: number = 0;

    _debugView() {
        return {
            ...super._debugView(),
            rev: this.rev,
            count: this.count,
        }
    }
}

export type SetPageAddr = UIntValue;

export class RootTreeNode extends NodePage<KValue<StringValue, SetPageAddr>> {
    get type(): PageType { return PageType.RootTreeNode; }
    protected _readValue(buf: Buffer): KValue<StringValue, UIntValue> {
        return KValue.readFrom(buf, StringValue.readFrom, UIntValue.readFrom);
    }
    protected get _childCtor() { return RootTreeNode; }
}

export class SuperPage extends RootTreeNode {
    get type(): PageType { return PageType.Super; }
    version: number = 1;
    rev: number = 1;
    setCount: number = 0;
    init() {
        super.init();
        this.freeBytes -= 3 * 4;
    }
    _writeContent(buf: Buffer) {
        super._writeContent(buf);
        buf.writeU32(this.version);
        buf.writeU32(this.rev);
        buf.writeU32(this.setCount);
    }
    _readContent(buf: Buffer) {
        super._readContent(buf);
        this.version = buf.readU32();
        this.rev = buf.readU32();
        this.setCount = buf.readU32();
    }
    protected _copyTo(other: this) {
        super._copyTo(other);
        other.rev = this.rev + 1;
        other.version = this.version;
        other.setCount = this.setCount;
    }
    getDirty(addDirty: boolean) {
        var dirty = this.storage.superPage = super.getDirty(false);
        return dirty;
    }
    _debugView() {
        return {
            ...super._debugView(),
            rev: this.rev,
            version: this.version,
            setCount: this.setCount,
        }
    }
}
