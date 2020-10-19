import { Database } from "./src/database.ts";

await Deno.mkdir("testdata", { recursive: true });

var db = new Database()
await db.openFile('testdata/testdb.db');

for (let i = 0; i < 10; i++)
    console.log(await db.createSet('test'));
await db.commit();
