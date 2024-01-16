import { assertEquals } from "./test.dep.ts";
import * as old from "https://deno.land/x/btrdb@v0.7.2/mod.ts";
import { Database } from "../mod.ts";
import { Runtime } from "../src/utils/runtime.ts";

Deno.test("test upgrade from v0", async () => {
  const path = "testdata/v0_upgrade_test.db";
  try {
    await Runtime.remove(path);
  } catch (error) {}

  // Create old version db
  const olddb = await old.Database.openFile(path);

  const a = await olddb.createSet("a", "kv");
  await a.set("foo", "bar");

  type Doc = { id: number; username: string };
  const b = await olddb.createSet<Doc>("b", "doc");
  await b.insert({ username: "admin_foo" });
  await b.insert({ username: "bar" });
  await b.useIndexes({
    isAdmin: ({ username }) => username.startsWith("admin_"),
  });

  await olddb.commit();
  olddb.close();

  // Upgrade and check
  const db = await Database.openFile(path);
  const [newA, newB] = [db.getSet("a", "kv"), db.getSet<Doc>("b", "doc")];
  assertEquals(await newA.get("foo"), "bar");
  assertEquals(await newB.get(1), { id: 1, username: "admin_foo" });
  assertEquals(await newB.get(2), { id: 2, username: "bar" });
  assertEquals(await newB.findIndex("isAdmin", true), [{
    id: 1,
    username: "admin_foo",
  }]);
  assertEquals(await newB.findIndex("isAdmin", false), [{
    id: 2,
    username: "bar",
  }]);

  assertEquals(await db.commit(), false);
  db.close();
});
