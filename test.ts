import { Database } from "./src/database.ts";

await Deno.mkdir("testdata", { recursive: true });

var db = new Database()
await db.openFile('testdata/testdb.db');

// for (let i = 0; i < 10; i++)
//     console.log(await db.createSet('test'));
await db.createSet('test1');
await db.createSet('test2');
await db.createSet('test1');
await db.createSet('test3');
await db.commit();
