import { DbSetType } from "../btrdb.d.ts";
import { DatabaseEngine } from "./database.ts";
import { DocNodeType, DocSetPage, SetPage } from "../pages/page.ts";
import { Node } from "../pages/tree.ts";

export class DbSetPageHelper<PageType extends (SetPage | DocSetPage)> {
  constructor(
    readonly page: PageType,
    readonly writer: boolean,
    private db: DatabaseEngine,
  ) {}

  _node: Node<DocNodeType> | null = null;
  get node() {
    return this._node || (this._node = new Node(this.page));
  }

  exitLock() {
    if (this.writer) {
      this.page!.lock.exitWriter();
      this.db.commitLock.exitWriter();
    } else {
      this.page!.lock.exitReader();
    }
  }
}

export class DbSetBase<PageType extends (SetPage | DocSetPage)> {
  constructor(
    protected _db: DatabaseEngine,
    public readonly name: string,
    public readonly type: DbSetType,
    protected isSnapshot: boolean,
  ) {}

  protected async getPageEnterLock(writer = false) {
    if (writer) {
      await this._db.commitLock.enterWriter();
    } else {
      await this._db.commitLock.enterReader();
    }
    try {
      var page = await this.getPage();
      if (!page) {
        throw new Error("Set not found");
      }
    } catch (e) {
      this._db.commitLock.exitReader();
      throw e;
    }
    if (writer) {
      await page.lock.enterWriter();
    } else {
      await page.lock.enterReader();
      this._db.commitLock.exitReader();
    }
    return new DbSetPageHelper(page, writer, this._db);
  }

  protected getPage() {
    return this._db._getSetPage(this.name, this.type) as Promise<PageType>;
  }

  async getCount() {
    const lock = await this.getPageEnterLock();
    try {
      return lock.page.count;
    } finally {
      lock.exitLock();
    }
  }

  exists() {
    return this._db._setExists(this.name, this.type);
  }
}
