import { LRUMap } from "../src/utils/lru.ts";
import { assertEquals } from "./test.dep.ts";

Deno.test({
  name: "LRUMap add/get",
  fn: () => {
    const map = new LRUMap<number, number>();
    assertEquals([map.size, ...map.valuesFromOldest()], [0]);
    map.add(1, 11);
    assertEquals([map.size, ...map.valuesFromOldest()], [1, 11]);
    map.get(1);
    assertEquals([map.size, ...map.valuesFromOldest()], [1, 11]);
    map.add(2, 22);
    assertEquals([map.size, ...map.valuesFromOldest()], [2, 11, 22]);
    map.get(1);
    assertEquals([map.size, ...map.valuesFromOldest()], [2, 22, 11]);
    map.add(3, 33);
    map.add(4, 44);
    map.add(5, 55);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      44,
      55,
    ]);
    map.get(5);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      44,
      55,
    ]);
    map.get(4);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      55,
      44,
    ]);
    map.get(2);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      11,
      33,
      55,
      44,
      22,
    ]);
    assertEquals([map.size, ...map], [
      5,
      [1, 11],
      [3, 33],
      [5, 55],
      [4, 44],
      [2, 22],
    ]);
  },
});

Deno.test({
  name: "LRUMap delete",
  fn: () => {
    const map = new LRUMap<number, number>();
    assertEquals([map.size, ...map.valuesFromOldest()], [0]);
    map.add(1, 11);
    assertEquals([map.size, ...map.valuesFromOldest()], [1, 11]);
    map.delete(1);
    assertEquals([map.size, ...map.valuesFromOldest()], [0]);
    map.add(1, 11);
    assertEquals([map.size, ...map.valuesFromOldest()], [1, 11]);
    map.add(2, 22);
    assertEquals([map.size, ...map.valuesFromOldest()], [2, 11, 22]);
    map.get(1);
    assertEquals([map.size, ...map.valuesFromOldest()], [2, 22, 11]);
    map.add(3, 33);
    map.add(4, 44);
    map.add(5, 55);
    map.get(5);
    map.get(4);
    map.get(2);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      11,
      33,
      55,
      44,
      22,
    ]);
    map.delete(2);
    assertEquals([map.size, ...map.valuesFromOldest()], [4, 11, 33, 55, 44]);
    map.delete(5);
    assertEquals([map.size, ...map.valuesFromOldest()], [3, 11, 33, 44]);
    map.delete(1);
    assertEquals([map.size, ...map.valuesFromOldest()], [2, 33, 44]);
    map.delete(4);
    assertEquals([map.size, ...map.valuesFromOldest()], [1, 33]);
    map.delete(3);
    assertEquals([map.size, ...map.valuesFromOldest()], [0]);
  },
});

Deno.test({
  name: "LRUMap set",
  fn: () => {
    const map = new LRUMap<number, number>();
    assertEquals([map.size, ...map.valuesFromOldest()], [0]);
    map.set(1, 11);
    assertEquals([map.size, ...map.valuesFromOldest()], [1, 11]);
    map.set(1, 11);
    assertEquals([map.size, ...map.valuesFromOldest()], [1, 11]);
    map.set(2, 22);
    assertEquals([map.size, ...map.valuesFromOldest()], [2, 11, 22]);
    map.set(1, 11);
    assertEquals([map.size, ...map.valuesFromOldest()], [2, 22, 11]);
    map.set(3, 33);
    map.set(4, 44);
    map.set(5, 55);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      44,
      55,
    ]);
    map.set(5, 55);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      44,
      55,
    ]);
    map.set(4, 44);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      22,
      11,
      33,
      55,
      44,
    ]);
    map.set(2, 22);
    assertEquals([map.size, ...map.valuesFromOldest()], [
      5,
      11,
      33,
      55,
      44,
      22,
    ]);
  },
});
