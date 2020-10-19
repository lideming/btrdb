import { Database } from "./src/database.ts";

await Deno.mkdir("testdata", { recursive: true });

var db = new Database()
await db.openFile('testdata/testdb.db');
