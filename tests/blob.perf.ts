import { Database } from "../mod.ts";

try {
  await Deno.remove("testdata/blob.db");
} catch {}

const db = new Database();
await db.openFile("testdata/blob.db", { fsync: false });
const set = await db.createSet<any>("blob", "doc");

console.time("insert");
for (let i = 0; i < 10000; i++) {
  await set.insert({
    data: new Uint8Array(16 * 1024),
  });
}
console.timeEnd("insert");

console.time("commit");
await db.commit();
console.timeEnd("commit");
