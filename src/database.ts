import { RecordsPage, SetPage, SuperPage } from "./page.ts";
import { InFileStorage, PageStorage } from "./storage.ts";
import { KValue, StringValue, UIntValue } from "./value.ts";

export interface EngineContext {
    storage: PageStorage;
}

export class DbSet {
    constructor(
        private _page: SetPage,
        public readonly name: string,
        private isSnapshot: boolean
    ) { }

    private get page() {
        if (this.isSnapshot) return this._page;
        return this._page = this._page.getLatestCopy();
    }

    get count() {
        return this.page.count;
    }

    async get(key: string): Promise<string | null> {
        const { found, val } = await this.page.findIndexRecursive(new StringValue(key));
        if (!found) return null;
        return val!.value.str;
    }

    async getAll(): Promise<{ key: string, value: string; }[]> {
        return (await this.page.getAllValues()).map(x => ({ key: x.key.str, value: x.value.str }));
    }

    async getKeys(): Promise<string[]> {
        return (await this.page.getAllValues()).map(x => x.key.str);
    }

    async set(key: string, val: string | null) {
        if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
        const keyv = new StringValue(key);
        const valv = !val ? null : new KValue(new StringValue(key), new StringValue(val));
        const done = await this.page.set(keyv, valv);
        if (done == 'added') {
            this.page.count += 1;
        } else if (done == 'removed') {
            this.page.count -= 1;
        }
    }

    delete(key: string) {
        return this.set(key, null);
    }
}

export class DatabaseEngine implements EngineContext {
    storage: PageStorage = undefined as any;
    private snapshot: SuperPage | null = null;

    get superPage() { return this.snapshot || this.storage.superPage; }

    async openFile(path: string) {
        const stor = new InFileStorage();
        await stor.openPath(path);
        await stor.init();
        this.storage = stor;
        // console.log('openFile():', this.superPage);
    }

    async createSet(name: string) {
        let set = await this.getSet(name);
        if (set) return set;
        const setPage = new SetPage(this.storage).getDirty(true);
        setPage.name = name;
        await this.superPage!.insert(new KValue(new StringValue(name), new UIntValue(setPage.addr)));
        this.superPage!.setCount++;
        return new DbSet(setPage, name, !!this.snapshot);
    }

    async getSet(name: string) {
        const superPage = this.superPage!;
        const r = await superPage.findIndexRecursive(new StringValue(name));
        if (!r.found) return null;
        const setPage = await this.storage.readPage(r.val!.value.val, SetPage);
        setPage.name = name;
        return new DbSet(setPage, name, !!this.snapshot);
    }

    async getSetCount() {
        return this.superPage!.setCount;
    }

    async commit() {
        return await this.storage.commit();
    }

    async getPrevSnapshot() {
        if (!this.superPage?.prevSuperPageAddr) return null;
        var prev = new DatabaseEngine();
        prev.storage = this.storage;
        prev.snapshot = await this.storage.readPage(this.superPage.prevSuperPageAddr, SuperPage);
        return prev;
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
    getPrevSnapshot(): Promise<Database | null>;
    close(): void;
}

export const Database: { new(): Database; } = DatabaseEngine as any;
