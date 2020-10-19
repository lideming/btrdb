import { SetPage, SuperPage } from "./page.ts";
import { InFileStorage, PageStorage } from "./storage.ts";
import { KValue, StringValue, UIntValue } from "./value.ts";

export interface EngineContext {
    storage: PageStorage;
}

export interface IDbSet {

}

export class DbSet implements IDbSet {
    constructor(
        private page: SetPage,
        public readonly name: string
    ) { }
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
        await this.superPage!.insert(new KValue(new StringValue(name), new UIntValue(setPage.addr)));
    }

    async commit() {
        await this.storage.commit();
    }

    async getSet(name: string) {
        const superPage = this.superPage!;
        const r = await superPage.findIndexRecursive(new StringValue(name));
        if (!r.found) return null;
        const setPage = await this.storage.readPage(r.val!.value.val, SetPage);
        return new DbSet(setPage, name);
    }
}

export interface Database {
    openFile(path: string): Promise<void>;
    createSet(name: string): Promise<DbSet>;
    commit(): Promise<void>;
}

export const Database: { new(): Database } = DatabaseEngine as any;
