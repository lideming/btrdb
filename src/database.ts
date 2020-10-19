import { RootPage, SetPage } from "./page.ts";
import { InFileStorage, PageStorage } from "./storage.ts";

export interface DatabaseContext {
    storage: PageStorage;
}

export class DbSet {
    constructor(
        public page: SetPage
    ) { }
}

export class Database implements DatabaseContext {
    storage: PageStorage = undefined as any;

    rootPage: RootPage = undefined as any;

    async openFile(path: string) {
        const stor = new InFileStorage();
        await stor.openPath(path);
        this.storage = stor;
        this.rootPage = new RootPage(this.storage);
    }

    async createSet() {

    }

    async getSet() {

    }
}