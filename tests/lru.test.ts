import { LRUMap } from "../src/lru.ts";
import { assertEquals } from "./test.dep.ts";

Deno.test({
  name: "LRUMap add/get",
  fn: () => {
    const map = new LRUMap<number, number>();
    assertEquals([map.count, ...map.valuesFromOldest()], [0]);
    map.add(1, 11);
    assertEquals([map.count, ...map.valuesFromOldest()], [1, 11]);
    map.get(1);
    assertEquals([map.count, ...map.valuesFromOldest()], [1, 11]);
    map.add(2, 22);
    assertEquals([map.count, ...map.valuesFromOldest()], [2, 11, 22]);
    map.get(1);
    assertEquals([map.count, ...map.valuesFromOldest()], [2, 22, 11]);
    map.add(3, 33);
    map.add(4, 44);
    map.add(5, 55);
    assertEquals([map.count, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      44,
      55,
    ]);
    map.get(5);
    assertEquals([map.count, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      44,
      55,
    ]);
    map.get(4);
    assertEquals([map.count, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      55,
      44,
    ]);
    map.get(2);
    assertEquals([map.count, ...map.valuesFromOldest()], [
      5,
      11,
      33,
      55,
      44,
      22,
    ]);
  },
});

Deno.test({
  name: "LRUMap remove",
  fn: () => {
    const map = new LRUMap<number, number>();
    assertEquals([map.count, ...map.valuesFromOldest()], [0]);
    map.add(1, 11);
    assertEquals([map.count, ...map.valuesFromOldest()], [1, 11]);
    map.remove(1);
    assertEquals([map.count, ...map.valuesFromOldest()], [0]);
    map.add(1, 11);
    assertEquals([map.count, ...map.valuesFromOldest()], [1, 11]);
    map.add(2, 22);
    assertEquals([map.count, ...map.valuesFromOldest()], [2, 11, 22]);
    map.get(1);
    assertEquals([map.count, ...map.valuesFromOldest()], [2, 22, 11]);
    map.add(3, 33);
    map.add(4, 44);
    map.add(5, 55);
    map.get(5);
    map.get(4);
    map.get(2);
    assertEquals([map.count, ...map.valuesFromOldest()], [
      5,
      11,
      33,
      55,
      44,
      22,
    ]);
    map.remove(2);
    assertEquals([map.count, ...map.valuesFromOldest()], [4, 11, 33, 55, 44]);
    map.remove(5);
    assertEquals([map.count, ...map.valuesFromOldest()], [3, 11, 33, 44]);
    map.remove(1);
    assertEquals([map.count, ...map.valuesFromOldest()], [2, 33, 44]);
    map.remove(4);
    assertEquals([map.count, ...map.valuesFromOldest()], [1, 33]);
    map.remove(3);
    assertEquals([map.count, ...map.valuesFromOldest()], [0]);
  },
});
