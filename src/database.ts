import { SetPage, SuperPage } from "./page.ts";
import { InFileStorage, PageStorage } from "./storage.ts";
import { KValue, StringValue, UIntValue } from "./value.ts";

export interface EngineContext {
    storage: PageStorage;
}

export class DbSet {
    constructor(
        private _page: SetPage,
        public readonly name: string
    ) { }

    private get page() {
        return this._page = this._page.getLatestCopy();
    }

    get count() {
        return this._page.count;
    }

    async get(key: string): Promise<string | null> {
        const { found, val } = await this.page.findIndexRecursive(new StringValue(key));
        if (!found) return null;
        return val!.value.str;
    }

    async set(key: string, val: string | null) {
        const { found, node, pos } = await this.page.findIndexRecursive(new StringValue(key));

        if (val != null) {
            const newVal = new KValue(new StringValue(key), new StringValue(val));
            const dirtyNode = node.getDirty(false);
            if (found) {
                dirtyNode.setKey(pos, newVal);
            } else {
                this.page.count += 1;
                dirtyNode.insertAt(pos, newVal);
            }
            dirtyNode.postChange();
        } else {
            const dirtyNode = node.getDirty(false);
            if (found) {
                this.page.count -= 1;
                dirtyNode.spliceKeys(pos, 1);
                dirtyNode.postChange();
            } // else noop
        }
    }

    delete(key: string) {
        return this.set(key, null);
    }
}

export class DatabaseEngine implements EngineContext {
    storage: PageStorage = undefined as any;

    get superPage() { return this.storage.superPage; }

    async openFile(path: string) {
        const stor = new InFileStorage();
        await stor.openPath(path);
        await stor.init();
        this.storage = stor;
        console.log('openFile():', this.superPage);
    }

    async createSet(name: string) {
        let set = await this.getSet(name);
        if (set) return set;
        const setPage = new SetPage(this.storage).getDirty(true);
        setPage.name = name;
        await this.superPage!.insert(new KValue(new StringValue(name), new UIntValue(setPage.addr)));
        this.superPage!.setCount++;
        return new DbSet(setPage, name);
    }

    async getSet(name: string) {
        const superPage = this.superPage!;
        const r = await superPage.findIndexRecursive(new StringValue(name));
        if (!r.found) return null;
        const setPage = await this.storage.readPage(r.val!.value.val, SetPage);
        setPage.name = name;
        return new DbSet(setPage, name);
    }

    async getSetCount() {
        return this.superPage!.setCount;
    }

    async commit() {
        await this.storage.commit();
    }

    close() {
        this.storage.close();
    }
}

export interface Database {
    openFile(path: string): Promise<void>;
    createSet(name: string): Promise<DbSet>;
    getSet(name: string): Promise<DbSet | null>;
    getSetCount(): Promise<number>;
    commit(): Promise<void>;
    close(): void;
}

export const Database: { new(): Database; } = DatabaseEngine as any;
