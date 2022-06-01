import { AlreadyExistError, BugError, NotExistError } from "./errors.ts";
import { NodePage, PageAddr } from "./page.ts";
import { Runtime } from "./runtime.ts";
import { IComparable, IKey, KeyComparator } from "./value.ts";

export class Node<T extends IKey<unknown>> {
  constructor(
    public page: NodePage<T>,
    public parent?: Node<T> | undefined,
    public posInParent?: number | undefined,
  ) {
  }

  get addr() {
    return this.page.addr;
  }
  get keys() {
    return this.page.keys;
  }
  get children() {
    return this.page.children;
  }

  findKey(key: IComparable<T>):
    | { found: true; pos: number; val: T }
    | { found: false; pos: number; val: undefined } {
    const keys = this.page.keys;
    let l = 0, r = keys.length - 1;
    while (l <= r) {
      const m = Math.round((l + r) / 2);
      const c = key.compareTo(keys[m]);
      // console.log("compare", key, c == 0 ? '==' : c > 0 ? '>' : '<', keys[m]);
      if (c == 0) return { found: true, pos: m, val: keys[m] };
      else if (c > 0) l = m + 1;
      else r = m - 1;
    }
    return { found: false, pos: l, val: undefined };
  }

  async getAllValues(array?: T[]): Promise<T[]> {
    if (!array) array = [];
    await this.traverseKeys((key) => {
      array!.push(key as any);
    });
    return array;
  }

  async readChildPage(pos: number): Promise<Node<T>> {
    const childPage = await this.page.readChildPage(pos);
    return new Node(childPage, this, pos);
  }

  async _dumpTree() {
    const result: any[] = [
      `(addr ${this.page.addr}${this.page.dirty ? " (dirty)" : ""})`,
    ];
    for (let pos = 0; pos < this.children.length; pos++) {
      const leftAddr = this.children[pos];
      if (leftAddr) {
        const leftPage = await this.readChildPage(pos);
        result.push(await leftPage._dumpTree());
      }
      if (pos < this.keys.length) {
        result.push(this.keys[pos]);
      }
    }
    return result;
  }

  async traverseKeys(
    func: (key: T, page: this, pos: number) => Promise<void> | void,
  ) {
    for (let pos = 0; pos < this.children.length; pos++) {
      const leftAddr = this.children[pos];
      if (leftAddr) {
        const leftPage = await this.readChildPage(pos);
        await leftPage.traverseKeys(func as any);
      }
      if (pos < this.keys.length) {
        await func(this.keys[pos], this, pos);
      }
    }
  }

  async *iterateKeys(): AsyncIterable<T> {
    for (let pos = 0; pos < this.children.length; pos++) {
      const leftAddr = this.children[pos];
      if (leftAddr) {
        const leftPage = await this.readChildPage(pos);
        yield* await leftPage.iterateKeys();
      }
      if (pos < this.keys.length) {
        yield this.keys[pos];
      }
    }
    return;
  }

  async findKeyRecursive(key: IComparable<T>): Promise<
    | { found: true; node: Node<T>; pos: number; val: T }
    | { found: false; node: Node<T>; pos: number; val: undefined }
  > {
    let node: Node<T> = this;
    while (true) {
      const { found, pos, val } = node.findKey(key);
      if (found) return { found: true, node, pos, val: val as T };
      if (!node.children[pos]) {
        return { found: false, node, pos, val: val as undefined };
      }
      node = await node.readChildPage(pos);
    }
  }

  async set(
    key: KeyComparator<T> | T,
    val: T | null,
    policy: "can-change" | "no-change" | "change-only" | "can-append",
  ) {
    const { found, node, pos, val: oldValue } = await this.findKeyRecursive(
      key as IComparable<T>,
    );
    let action: "added" | "removed" | "changed" | "noop" = "noop";

    if (node.page.hasNewerCopy()) {
      console.info({
        cur: node.page._debugView(),
        new: node.page._newerCopy!._debugView(),
      });
      throw new BugError("BUG: set() -> findIndex() returns old copy.");
    }

    if (val != null) {
      const dirtyNode = node.getDirty(false);
      if (found) {
        if (policy === "no-change") {
          throw new AlreadyExistError("key already exists");
        } else if (policy === "can-append") {
          // TODO: omit key on appended value
          dirtyNode.insertAt(pos, val, dirtyNode.children[pos]);
          dirtyNode.setChild(pos + 1, 0);
          dirtyNode.postChange(pos == dirtyNode.keys.length - 1);
          action = "added";
        } else {
          dirtyNode.setKey(pos, val);
          dirtyNode.postChange();
          action = "changed";
        }
      } else {
        if (policy === "change-only") {
          throw new NotExistError("key doesn't exists");
        }
        dirtyNode.insertAt(pos, val);
        dirtyNode.postChange(pos == dirtyNode.keys.length - 1);
        action = "added";
      }
    } else {
      if (found) {
        await node.deleteAt(pos);
        action = "removed";
      } // else noop
    }
    return { action, oldValue: oldValue ?? null };
  }

  async deleteAt(pos: number) {
    const dirtyNode = this.getDirty(false);
    const oldLeftAddr = dirtyNode.children[pos];
    if (oldLeftAddr) {
      let leftSubNode = await dirtyNode.readChildPage(pos);
      const leftNode = leftSubNode;
      while (leftSubNode.children[leftSubNode.children.length - 1]) {
        leftSubNode = await leftSubNode.readChildPage(
          leftSubNode.children.length - 1,
        );
      }
      const leftKey = leftSubNode.keys[leftSubNode.keys.length - 1];
      dirtyNode.page.spliceKeys(pos, 1, leftKey, leftNode.addr);
      await leftSubNode.deleteAt(leftSubNode.keys.length - 1);
      dirtyNode.postChange();
    } else {
      dirtyNode.page.spliceKeys(pos, 1);
      if (dirtyNode.keys.length == 0) {
        if (dirtyNode.parent) {
          const dirtyParent = dirtyNode.parent.getDirty(false);
          dirtyParent.setChild(
            dirtyNode.posInParent!,
            dirtyNode.children[0] ?? 0,
          );
          dirtyParent!.postChange();
          dirtyNode.parent = undefined;
          dirtyNode.page.removeDirty();
        } else if (dirtyNode.children[0]) {
          const child = await dirtyNode.readChildPage(0);
          dirtyNode.page.setKeys(child.keys, child.children);
          dirtyNode.postChange();
        } else {
          dirtyNode.postChange();
        }
      } else {
        dirtyNode.postChange();
      }
    }
  }

  insertAt(pos: number, key: T, leftChild: PageAddr = 0) {
    this.page.spliceKeys(pos, 0, key, leftChild);
  }

  setChild(pos: number, child: number) {
    this.page.setChild(pos, child);
  }

  setKey(pos: number, key: T) {
    this.page.setKey(pos, key);
  }

  /**
   * Finish copy-on-write on this node and parent nodes.
   * Also split this node if the node is overflow.
   */
  postChange(appending = false) {
    if (this.page.hasNewerCopy()) {
      throw new BugError("BUG: postChange() on old copy.");
    }
    if (!this.page.dirty) {
      throw new BugError("BUG: postChange() on non-dirty page.");
    }
    if (this.page.freeBytes < 0) {
      if (this.keys.length <= 2) {
        throw new Error(
          "Not implemented. freeBytes=" + this.page.freeBytes +
          " keys=" + Runtime.inspect(this.keys),
        );
      }
      // console.log('spliting node with key count:', this.keys.length);
      // console.log(this.keys.length, this.children.length);

      // split this node
      const leftSib = this.page.createChildPage();
      // when appending, make the left sibling larger for space efficiency
      const leftCount = appending
        ? Math.floor(this.keys.length * 0.9)
        : Math.floor(this.keys.length / 2);
      const leftKeys = this.page.spliceKeys(0, leftCount);
      leftKeys[1].push(0);
      leftSib.setKeys(leftKeys[0], leftKeys[1]);
      const [[middleKey], [middleLeftChild]] = this.page.spliceKeys(0, 1);
      leftSib.setChild(leftCount, middleLeftChild);

      if (this.parent) {
        // insert the middle key with the left sibling to parent
        this.getDirty(true);
        this.getParentDirty();
        this.parent.setChild(this.posInParent!, this.addr);
        this.parent.insertAt(this.posInParent!, middleKey, leftSib.addr);
        this.parent.postChange(
          this.posInParent! == this.parent.keys.length - 1,
        );
        //          ^^^^^^^^^^ makeDirtyToRoot() inside
      } else {
        // make this node a parent of two nodes...
        const rightChild = this.page.createChildPage();
        rightChild.setKeys(this.keys, this.children);
        this.page.setKeys([middleKey], [leftSib.addr, rightChild.addr]);
        this.getDirty(true);
        this.makeDirtyToRoot();
      }
    } else {
      this.getDirty(true);
      if (this.parent) {
        this.makeDirtyToRoot();
      }
    }
  }

  getDirty(addDirty: boolean): Node<T> {
    this.page = this.page.getDirty(addDirty);
    return this;
  }

  getParentDirty(): Node<T> {
    return this.parent = this.parent!.getDirty(true);
  }

  makeDirtyToRoot() {
    if (!this.page.dirty) {
      throw new BugError("BUG: makeDirtyToRoot() on non-dirty page");
    }
    let node: Node<T> = this;
    while (node.parent) {
      const parent = node.parent;
      const parentWasDirty = parent.page.dirty;
      const dirtyParent = node.parent = parent.getDirty(true);
      dirtyParent.setChild(node.posInParent!, node.addr);
      node = dirtyParent;
      if (parentWasDirty) break;
    }
  }
}

export class NoRefcountNode<T extends IKey<unknown>> extends Node<T> {
  constructor(
    page: NodePage<T>,
    parent?: Node<T> | undefined,
    posInParent?: number | undefined,
  ) {
    super(page, parent, posInParent);
  }

  async readChildPage(pos: number): Promise<NoRefcountNode<T>> {
    const childPage = await this.page.readChildPage(pos);
    return new NoRefcountNode(childPage, this, pos);
  }

  getDirty(addDirty: boolean): Node<T> {
    const oldpage = this.page;
    const wasOnDisk = this.page.hasAddr;
    this.page = this.page.getDirty(addDirty);
    if (oldpage !== this.page) {
      this.page.storage.changeRefCount(oldpage.addr, -1);
      this.page.storage.changeRefCount(this.page.addr, 1);
    }
    if (!wasOnDisk && this.page.hasAddr) {
      this.page.storage.changeRefCount(this.page.addr, 1);
    }
    return this;
  }
}
