import {
  DocNodeType,
  DocSetPage,
  IndexNodeType,
  IndexTopPage,
} from "./page.ts";
import {
  compareJSValue,
  JSValue,
  KeyLeftmostComparator,
  KeyRightmostComparator,
  PageOffsetValue,
} from "./value.ts";
import { Node } from "./tree.ts";

export interface Query {
  run(page: Node<DocNodeType>): AsyncIterable<PageOffsetValue>;
  [additional: string]: any;
}

export function EQ(index: string, val: any): Query {
  return {
    eq: [index, val],
    async *run(page) {
      const keyv = new JSValue(val);
      const result = await findIndexKey(page, index, keyv, false);
      for await (const it of iterateNode(result.node, result.pos, false)) {
        if (compareJSValue(keyv, it.key) === 0) {
          yield it.value;
        } else {
          break;
        }
      }
    },
  };
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
  return {
    between: [index, min, max, minInclusive, maxInclusive],
    async *run(page) {
      const vMin = min == null ? null : new JSValue(min);
      const vMax = max == null ? null : new JSValue(max);
      let keyIterator: AsyncIterable<IndexNodeType>;
      if (vMin) {
        const begin = await findIndexKey(page, index, vMin, !minInclusive);
        keyIterator = iterateNode(begin.node, begin.pos, false);
      } else {
        keyIterator = page.iterateKeys() as AsyncIterable<IndexNodeType>;
      }
      for await (const key of keyIterator) {
        let _c;
        if (
          vMax == null || (_c = compareJSValue(vMax, key.key)) > 0 ||
          (_c === 0 && maxInclusive)
        ) {
          yield key.value;
        } else {
          break;
        }
      }
    },
  };
}

export function AND(...queries: Query[]): Query {
  if (queries.length == 0) throw new Error("No queries");
  return {
    and: queries,
    async *run(page) {
      let set = new Set<number>();
      let nextSet = new Set<number>();
      for await (const val of queries[0].run(page)) {
        set.add(val.encode());
      }
      for (let i = 1; i < queries.length; i++) {
        const qResult = queries[i].run(page);
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
    },
  };
}

export function OR(...queries: Query[]): Query {
  if (queries.length == 0) throw new Error("No queries");
  return {
    or: queries,
    async *run(page) {
      let set = new Set<number>();
      for (const sub of queries) {
        const subResult = sub.run(page);
        for await (const val of subResult) {
          const valEncoded = val.encode();
          if (!set.has(valEncoded)) {
            set.add(valEncoded);
            yield val;
          }
        }
      }
    },
  };
}

export function NOT(query: Query): Query {
  return {
    not: query,
    async *run(page) {
      let set = new Set<number>();
      const subResult = query.run(page);
      for await (const val of subResult) {
        const valEncoded = val.encode();
        set.add(valEncoded);
      }
      for await (const key of page.iterateKeys()) {
        if (!set.has((key as DocNodeType).value.encode())) {
          yield (key as DocNodeType).value;
        }
      }
    },
  };
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
