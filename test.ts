import { Database } from "./src/database.ts";
import { assert, assertEquals } from "https://deno.land/std@0.74.0/testing/asserts.ts";

const testFile = 'testdata/testdb.db';

await Deno.mkdir("testdata", { recursive: true });

try { await Deno.remove(testFile) } catch { }

async function runWithDatabase(func: (db: Database) => Promise<void>) {
    var db = new Database()
    await db.openFile(testFile);
    await func(db);
    db.close();
}

await runWithDatabase(async (db) => {
    assertEquals(await db.getSetCount(), 0);
    assert(await db.createSet('test1'));
    assertEquals(await db.getSetCount(), 1);
    assert(await db.createSet('test2'));
    assertEquals(await db.getSetCount(), 2);
    assert(await db.createSet('test1'));
    assertEquals(await db.getSetCount(), 2);
    assert(await db.createSet('test3'));
    assertEquals(await db.getSetCount(), 3);
    await db.commit();
});
