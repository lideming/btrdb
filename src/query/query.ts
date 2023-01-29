import {
  DocNodeType,
  DocSetPage,
  IndexNodeType,
  IndexTopPage,
} from "../pages/page.ts";
import {
  compareJSValue,
  JSValue,
  KeyLeftmostComparator,
  KeyRightmostComparator,
  PageOffsetValue,
} from "../utils/value.ts";
import { Node } from "../pages/tree.ts";

export interface Query {
  run(page: Node<DocNodeType>): AsyncIterable<PageOffsetValue>;
  [additional: string]: any;
}

export function EQ(index: string, val: any): Query {
  return new QueryEq(index, val);
}

export function NE(index: string, val: any): Query {
  return NOT(EQ(index, val));
}

export function GT(index: string, val: any) {
  return BETWEEN(index, val, null, false, false);
}

export function GE(index: string, val: any) {
  return BETWEEN(index, val, null, true, false);
}

export function LT(index: string, val: any) {
  return BETWEEN(index, null, val, false, false);
}

export function LE(index: string, val: any) {
  return BETWEEN(index, null, val, false, true);
}

export function BETWEEN(
  index: string,
  min: any,
  max: any,
  minInclusive: boolean,
  maxInclusive: boolean,
): Query {
  return new QueryBetween(index, min, max, minInclusive, maxInclusive);
}

export function AND(...queries: Query[]): Query {
  return new QueryAnd(queries);
}

export function OR(...queries: Query[]): Query {
  return new QueryOr(queries);
}

export function NOT(query: Query): Query {
  return new QueryNot(query);
}

export function SLICE(query: Query, skip: number, limit: number) {
  return new QuerySlice(query, skip, limit);
}

export function SKIP(query: Query, skip: number) {
  return SLICE(query, skip, -1);
}

export function LIMIT(query: Query, limit: number) {
  return SLICE(query, 0, limit);
}

export class QueryEq implements Query {
  constructor(
    readonly index: string,
    readonly val: any,
  ) {}

  async *run(page: Node<DocNodeType>) {
    const keyv = new JSValue(this.val);
    const result = await findIndexKey(page, this.index, keyv, false);
    for await (const it of iterateNode(result.node, result.pos, false)) {
      if (compareJSValue(keyv, it.key) === 0) {
        yield it.value;
      } else {
        break;
      }
    }
  }
}

export class QueryBetween implements Query {
  constructor(
    readonly index: string,
    readonly min: any,
    readonly max: any,
    readonly minInclusive: boolean,
    readonly maxInclusive: boolean,
  ) {}

  async *run(page: Node<DocNodeType>) {
    const vMin = this.min == null ? null : new JSValue(this.min);
    const vMax = this.max == null ? null : new JSValue(this.max);
    let keyIterator: AsyncIterable<IndexNodeType>;
    if (vMin) {
      const begin = await findIndexKey(
        page,
        this.index,
        vMin,
        !this.minInclusive,
      );
      keyIterator = iterateNode(begin.node, begin.pos, false);
    } else {
      keyIterator = page.iterateKeys() as AsyncIterable<IndexNodeType>;
    }
    for await (const key of keyIterator) {
      let _c;
      if (
        vMax == null || (_c = compareJSValue(vMax, key.key)) > 0 ||
        (_c === 0 && this.maxInclusive)
      ) {
        yield key.value;
      } else {
        break;
      }
    }
  }
}

export class QueryAnd implements Query {
  constructor(readonly queries: Query[]) {
    if (queries.length == 0) throw new Error("No queries");
  }
  async *run(page: Node<DocNodeType>) {
    let set = new Set<number>();
    let nextSet = new Set<number>();
    for await (const val of this.queries[0].run(page)) {
      set.add(val.encode());
    }
    for (let i = 1; i < this.queries.length; i++) {
      const qResult = this.queries[i].run(page);
      for await (const val of qResult) {
        const valEncoded = val.encode();
        if (set.has(valEncoded)) {
          nextSet.add(valEncoded);
        }
      }
      set.clear();
      [set, nextSet] = [nextSet, set];
    }
    for (const val of set) {
      yield PageOffsetValue.fromEncoded(val);
    }
  }
}

export class QueryOr implements Query {
  constructor(readonly queries: Query[]) {
    if (queries.length == 0) throw new Error("No queries");
  }
  async *run(page: Node<DocNodeType>) {
    let set = new Set<number>();
    for (const sub of this.queries) {
      const subResult = sub.run(page);
      for await (const val of subResult) {
        const valEncoded = val.encode();
        if (!set.has(valEncoded)) {
          set.add(valEncoded);
          yield val;
        }
      }
    }
  }
}

export class QueryNot implements Query {
  constructor(readonly query: Query) {}
  async *run(page: Node<DocNodeType>) {
    let set = new Set<number>();
    const subResult = this.query.run(page);
    for await (const val of subResult) {
      const valEncoded = val.encode();
      set.add(valEncoded);
    }
    for await (const key of page.iterateKeys()) {
      if (!set.has((key as DocNodeType).value.encode())) {
        yield (key as DocNodeType).value;
      }
    }
  }
}

export class QuerySlice implements Query {
  constructor(
    readonly query: Query,
    readonly skip: number,
    readonly limit: number,
  ) {
    if (skip < 0) throw new Error("skip must be >= 0");
  }
  async *run(page: Node<DocNodeType>) {
    const subResult = this.query.run(page);
    let skip = this.skip;
    let limit = this.limit;
    if (limit == 0) return;
    for await (const it of subResult) {
      if (skip) {
        skip--;
        continue;
      }
      yield it;
      if (limit > 0) {
        if (--limit == 0) break;
      }
    }
  }
}

export async function findIndexKey(
  node: Node<DocNodeType>,
  index: string,
  vKey: JSValue,
  rightMost: boolean,
) {
  let indexPage: Node<DocNodeType>;
  if (index == "id") {
    indexPage = node;
  } else {
    const info = (await (node.page as DocSetPage).ensureIndexes())[index];
    if (!info) throw new Error("Specified index does not exist.");
    indexPage = new Node(
      await node.page.storage.readPage(
        (node.page as DocSetPage).indexesAddrMap![index],
        IndexTopPage,
      ),
    );
  }
  const indexResult = await indexPage.findKeyRecursive(
    rightMost
      ? new KeyRightmostComparator(vKey)
      : new KeyLeftmostComparator(vKey),
  );
  return indexResult;
}

export async function* iterateNode(
  node: Node<IndexNodeType>,
  pos: number,
  reverse: boolean,
): AsyncIterable<IndexNodeType> {
  while (true) {
    const val = node.keys[pos];
    if (val) {
      // Get one result and go right
      yield val;
    }
    if (reverse) pos--;
    else pos++;
    if (node.children[pos]) {
      // Go left down to child
      do {
        node = await node.readChildPage(pos);
        pos = reverse ? node.keys.length - 1 : 0;
      } while (node.children[pos]);
    }
    if ((reverse ? -1 : node.keys.length + 1) == pos) {
      // The end of this node, try go up
      if (node.parent) {
        pos = node.posInParent!;
        node = node.parent;
      } else {
        break;
      }
    }
  }
}
