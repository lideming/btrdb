import { SetPage, SuperPage } from "./page.ts";
import { InFileStorage, PageStorage } from "./storage.ts";

export interface EngineContext {
    storage: PageStorage;
}

export interface IDbSet {

}

export class DbSet implements IDbSet {
    constructor(
        private page: SetPage
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

    }

    async getSet() {

    }
}

export interface Database {
    openFile(path: string): Promise<void>;
    createSet(name: string): Promise<DbSet>;
}

export const Database: { new(): Database } = DatabaseEngine as any;
