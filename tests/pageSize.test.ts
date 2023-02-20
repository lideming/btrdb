import { assertEquals } from "./test.dep.ts";
import { Database } from "../mod.ts";
import { Runtime } from "../src/utils/runtime.ts";

Deno.test("pageSize", async function () {
  async function testPageSize(pageSize: number) {
    const path = `testdata/pagesize_test_${pageSize}.db`;
    try {
      await Runtime.remove(path);
    } catch (error) {}

    const db = await Database.openFile(path, { pageSize });
    const doc = await db.createSet<any>("testset", "doc");
    await doc.insert({ foo: "bar" });
    const kv = await db.createSet("testset", "kv");
    await kv.set("foo", "bar");
    await db.commit(true);
    db.close();

    assertEquals((await Runtime.stat(path)).size, pageSize * 10);
  }

  await testPageSize(256);
  await testPageSize(4096);
  await testPageSize(16 * 1024);
});
