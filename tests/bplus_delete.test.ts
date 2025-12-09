import { Runtime } from "../src/utils/runtime.ts";
import { Database } from "../src/database/database.ts";
import { assertEquals } from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { GE } from "../src/query/query.ts";

const filename = "testdata/bplus_delete.db";

Deno.test("B+ Tree Deletion & Merge", async (t) => {
  try {
    await Runtime.remove(filename);
  } catch {}
  const db = new Database();
  await db.openFile(filename);
  const set = await db.createSet("test", "doc");
  await set.useIndexes({ valStr: (d) => d.id });

  // 1. Insert enough keys to create a 2-level tree
  // Page size is small in tests (usually via env or default), but we want to ensure splits.
  // Assuming default ~8KB. Keys are small. We need many keys.
  const count = 500;
  for (let i = 0; i < count; i++) {
    const id = i.toString().padStart(4, "0");
    await set.upsert({ id, val: i } as any);
  }
  await db.commit();

  let all = await set.getAll();
  assertEquals(all.length, count, "Initial count match");

  // 2. Delete some keys from the beginning to trigger leaf merges
  // Deleting 0000, 0001, ...
  const deleteCount = 100;
  for (let i = 0; i < deleteCount; i++) {
     const id = i.toString().padStart(4, "0");
     await set.delete(id);
  }
  await db.commit();

  // Verify count
  all = await set.getAll();
  assertEquals(all.length, count - deleteCount, "After delete count match");
  assertEquals(all[0].id, "0100", "First key correct");

  // Verify Iteration via Query (uses nextPage)
  const queryCount = await set.queryCount(GE('id', '0000'));
  assertEquals(queryCount, count - deleteCount, "Query count match");

  // 3. Delete almost all to trigger Root Collapse
  for (let i = deleteCount; i < count - 1; i++) { // Leave one
     const id = i.toString().padStart(4, "0");
     await set.delete(id);
  }
  await db.commit();
  
  all = await set.getAll();
  assertEquals(all.length, 1, "Should have 1 item left");
  assertEquals(all[0].id, (count - 1).toString().padStart(4, "0"));

  const lastQueryCount = await set.queryCount(GE('id', '0000'));
  assertEquals(lastQueryCount, 1, "Last query count match");

  db.close();
});
