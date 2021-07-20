import { DocSetPage, IndexNodeType, IndexTopPage, NodePage } from "./page.ts";
import { JSONValue, KeyLeftmostComparator, PageOffsetValue } from "./value.ts";

export interface Query {
  run(page: DocSetPage): AsyncIterable<PageOffsetValue>;
}

export function IndexEQ(index: string, val: any): Query {
  return {
    async *run(page) {
      const keyv = new JSONValue(val);
      const result = await findIndexKey(page, index, keyv);
      for await (const key of iterateIndex(result.node, result.pos)) {
        if (keyv.compareTo(key.key) === 0) {
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

export async function findIndexKey(
  page: DocSetPage,
  index: string,
  vKey: JSONValue,
) {
  const info = (await page.ensureIndexes())[index];
  if (!info) throw new Error("Specified index does not exist.");
  const indexPage = await page.storage.readPage(
    page.indexesAddrMap![index],
    IndexTopPage,
  );
  const indexResult = await indexPage.findKeyRecursive(
    new KeyLeftmostComparator(vKey),
  );
  return indexResult;
}

export async function* iterateIndex(
  node: NodePage<IndexNodeType>,
  pos: number,
): AsyncIterable<IndexNodeType> {
  while (true) {
    const val = node.keys[pos];
    if (val) {
      // Get one result and go right
      yield val;
    }
    pos++;
    if (node.children[pos]) {
      // Go left down to child
      do {
        node = await node.readChildPage(pos);
        pos = 0;
      } while (node.children[0]);
    }
    if (node.children.length == pos) {
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
