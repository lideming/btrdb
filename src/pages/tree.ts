import { debug_node, debugLog } from "../utils/debug.ts";
import { AlreadyExistError, BugError, NotExistError } from "../utils/errors.ts";
import { NodePage, PageAddr } from "../pages/page.ts";
import { Runtime } from "../utils/runtime.ts";
import { IComparable, IKey, KeyComparator } from "../utils/value.ts";

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
    for (let pos = 0; pos < this.keys.length + 1; pos++) {
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
    for (let pos = 0; pos < this.keys.length + 1; pos++) {
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
    for (let pos = 0; pos < this.keys.length + 1; pos++) {
      const leftAddr = this.children[pos];
      if (leftAddr) {
        const leftPage = await this.readChildPage(pos);
        yield* leftPage.iterateKeys();
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
      const dirtyNode = node.getDirty();
      if (found) {
        if (policy === "no-change") {
          throw new AlreadyExistError("key already exists");
        } else if (policy === "can-append") {
          // TODO: omit key on appended value
          dirtyNode.insertAt(pos, val, dirtyNode.children[pos]);
          dirtyNode.setChild(pos + 1, 0);
          await dirtyNode.postChange(pos == dirtyNode.keys.length - 1);
          action = "added";
        } else {
          dirtyNode.setKey(pos, val);
          await dirtyNode.postChange();
          action = "changed";
        }
      } else {
        if (policy === "change-only") {
          throw new NotExistError("key doesn't exists");
        }
        dirtyNode.insertAt(pos, val);
        await dirtyNode.postChange(pos == dirtyNode.keys.length - 1);
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
    const dirtyNode = this.getDirty();
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
      
      // We must propagate changes (handling underflow if needed)
      // Since child recursion handles itself, we only need to check THIS node.
      await dirtyNode.postChange();
    } else {
      dirtyNode.page.spliceKeys(pos, 1);

      // Root height reduction check
      if (!dirtyNode.parent && dirtyNode.keys.length == 0 && dirtyNode.children[0]) {
        const child = await dirtyNode.readChildPage(0);
        dirtyNode.page.setKeys(child.keys, child.children);
        child.discard();
        debug_node && debugLog(
          "page",
          dirtyNode.addr,
          "no keys after delete, no parent, replace content with only child",
        );
        await dirtyNode.postChange();
      } else {
        await dirtyNode.postChange();
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
  async postChange(appending = false) {
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
      // console.log(this.keys.length, this.children.length);
      
      // split this node
      const leftSib = this.createChildNode();
      await leftSib.getDirtyWithAddr();

      let leftCount = Math.floor(this.keys.length / 2);
      // when appending, make the left sibling larger for space efficiency
      if (appending) {
        leftCount = this.keys.length;
        let leftFree = this.page.freeBytes;
        while (
          leftFree < this.page.storage.pageSize * 0.05 ||
          leftCount > this.keys.length - 2
        ) {
          leftCount--;
          leftFree += this.keys[leftCount].byteLength;
          if (this.children.length) leftFree += 4;
        }
      }

      const [leftKeys, leftChildren] = this.page.spliceKeys(0, leftCount);
      if (leftChildren.length) leftChildren.push(0);
      leftSib.page.setKeys(leftKeys, leftChildren);
      const [[middleKey], [middleLeftChild]] = this.page.spliceKeys(0, 1);
      if (middleLeftChild) {
        leftSib.setChild(leftCount, middleLeftChild);
      }

      if (this.parent) {
        // insert the middle key with the left sibling to parent
        await this.getDirtyWithAddr();
        this.parent = await this.parent!.getDirtyWithAddr();
        this.parent.setChild(this.posInParent!, this.addr);
        this.parent.insertAt(this.posInParent!, middleKey, leftSib.addr);
        await this.parent.postChange(
          this.posInParent! == this.parent.keys.length - 1,
        );
        //          ^^^^^^^^^^ makeDirtyToRoot() inside
        debug_node &&
          debugLog("page", this.page.addr, "splited", leftSib.addr);
      } else {
        // make this node a parent of two nodes...
        const rightChild = this.createChildNode();
        await rightChild.getDirtyWithAddr();
        rightChild.page.setKeys(this.keys, this.children);
        this.page.setKeys([middleKey], [leftSib.addr, rightChild.addr]);
        await this.getDirtyWithAddr();
        debug_node && debugLog(
          "page",
          this.page.addr,
          "splited as root",
          leftSib.addr,
          rightChild.addr,
        );
      }
    } 
    // Underflow check: > 75% free space
    else if (this.parent && this.page.freeBytes > this.page.storage.pageSize * 0.75) {
      await this.handleUnderflow();
    } 
    else {
      await this.getDirtyWithAddr();
      if (this.parent) {
        await this.makeDirtyToRoot();
      }
    }
  }

  async handleUnderflow() {
    const parent = await this.parent!.getDirtyWithAddr();
    const pos = this.posInParent!;

    // 1. Try merge with Left Sibling
    if (pos > 0 && parent.children[pos - 1]) {
      const leftSib = await parent.readChildPage(pos - 1);
      const separatorKey = parent.keys[pos - 1];
      if (this.canMerge(leftSib, this, separatorKey)) {
        await this.mergeNodes(leftSib, this, separatorKey, pos - 1);
        return;
      }
    }

    // 2. Try merge with Right Sibling
    if (pos < parent.children.length && parent.children[pos + 1]) {
      const rightSib = await parent.readChildPage(pos + 1);
      const separatorKey = parent.keys[pos];
      if (this.canMerge(this, rightSib, separatorKey)) {
        await this.mergeNodes(this, rightSib, separatorKey, pos);
        return;
      }
    }

    // 3. No merge possible, just propagate dirty
    await this.getDirtyWithAddr();
    await this.makeDirtyToRoot();
  }

  canMerge(left: Node<T>, right: Node<T>, separator: T): boolean {
    const pageSize = this.page.storage.pageSize;
    const leftUsed = pageSize - left.page.freeBytes;
    const rightUsed = pageSize - right.page.freeBytes;
    // Heuristic check: used space + separator + buffer
    const needed = leftUsed + rightUsed + separator.byteLength + 16;
    return needed <= pageSize;
  }

  /**
   * Merges `right` into `left`.
   * `separatorPos` is the index of the separator key in `parent`.
   */
  async mergeNodes(left: Node<T>, right: Node<T>, separator: T, separatorPos: number) {
    const parent = this.parent!; // Parent is already dirty

    // 1. Prepare new content for Left Node
    const newKeys = [...left.keys, separator, ...right.keys];
    const newChildren = [...left.children, ...right.children];

    // 2. Update Left Node
    const dirtyLeft = await left.getDirtyWithAddr();
    dirtyLeft.page.setKeys(newKeys, newChildren);

    // 3. Update Parent (remove separator and Right pointer)
    // We use setKeys to ensure freeBytes is perfectly recalculated.
    const parentKeys = [...parent.keys];
    parentKeys.splice(separatorPos, 1);
    
    const parentChildren = [...parent.children];
    // Remove pointer to Left (pos) and Right (pos+1), insert merged (pos)
    parentChildren.splice(separatorPos, 2, dirtyLeft.addr);
    
    parent.page.setKeys(parentKeys, parentChildren);

    // 4. Discard Right Node
    right.discard();

    debug_node && debugLog("page", this.page.addr, "merged", left.addr, right.addr);

    // 5. Propagate changes up
    await parent.postChange();
  }

  createChildNode(): this {
    return new Node(this.page.createChildPage(), undefined, undefined) as this;
  }

  getDirty(): Node<T> {
    this.page = this.page.getDirty();
    return this;
  }

  async getDirtyWithAddr(): Promise<Node<T>> {
    this.page = await this.page.getDirtyWithAddr();
    return this;
  }

  discard() {
    if (this.page.dirty) {
      this.page.removeDirty();
    }
  }

  async makeDirtyToRoot() {
    if (!this.page.dirty) {
      throw new BugError("BUG: makeDirtyToRoot() on non-dirty page");
    }
    let node: Node<T> = this;
    while (node.parent) {
      const parent = node.parent;
      const parentWasDirty = parent.page.dirty;
      const dirtyParent = node.parent = await parent.getDirtyWithAddr();
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

  override async readChildPage(pos: number): Promise<NoRefcountNode<T>> {
    const childPage = await this.page.readChildPage(pos);
    return new NoRefcountNode(childPage, this, pos);
  }

  _cleanPage: NodePage<T> | null = null;

  override getDirty(): this {
    const oldpage = this.page;
    this.page = this.page.getDirty();
    if (oldpage !== this.page) {
      this._cleanPage = oldpage;
    }
    return this;
  }

  override async getDirtyWithAddr(): Promise<this> {
    let oldpage = this.page;
    const wasOnDisk = this.page.hasAddr;
    this.page = await this.page.getDirtyWithAddr();
    if (oldpage !== this.page) {
      this.page.storage.changeRefCount(this.page.addr, 1);
      this.page.storage.changeRefCount(oldpage.addr, -1);
    }
    if (!wasOnDisk && this.page.hasAddr) {
      this.page.storage.changeRefCount(this.page.addr, 1);
      if (this._cleanPage) {
        this.page.storage.changeRefCount(this._cleanPage.addr, -1);
        this._cleanPage = null;
      }
    }
    return this;
  }

  override createChildNode(): this {
    return new NoRefcountNode(
      this.page.createChildPage(),
      undefined,
      undefined,
    ) as this;
  }

  override discard() {
    super.discard();
    if (this.page.hasAddr) {
      this.page.storage.changeRefCount(this.page.addr, -1);
    }
  }
}
