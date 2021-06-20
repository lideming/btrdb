import { Database } from "./src/database.ts";
import { assert, assertEquals } from "https://deno.land/std@0.74.0/testing/asserts.ts";

await new Promise(r => setTimeout(r, 300));

const testFile = 'testdata/testdb.db';

await Deno.mkdir("testdata", { recursive: true });

try { await Deno.remove(testFile); } catch { }

async function runWithDatabase(func: (db: Database) => Promise<void>) {
    console.info('=============================');
    console.info('===== run ' + func.name);
    console.info('=============================');
    var db = new Database();
    await db.openFile(testFile);
    await func(db);
    db.close();
}

await runWithDatabase(async function createSet (db) {
    var set = await db.createSet("test");
    await set.set("testkey", "testval");
    console.info(await set.get("testkey"));
    await db.commit();
});

await runWithDatabase(async function getSet (db) {
    var set = await db.getSet("test");
    console.info(await set!.get("testkey"));
    await db.commit();
});

await runWithDatabase(async function set10k (db) {
    var set = (await db.getSet("test"))!;
    for (let i = 0; i < 1000000; i++) {
        await set.set('key' + i, 'val' + i);
    }
    console.info('set.count', set.count);
    await db.commit();
});

await runWithDatabase(async function get10k (db) {
    var set = (await db.getSet("test"))!;
    console.info('set.count', set.count);
    for (let i = 0; i < 1000000; i++) {
        const val = await set!.get('key' + i);
        if (val != 'val' + i) {
            console.error('expect', i, 'got', val);
        }
    }
    console.info('read done');
    await db.commit();
});

// await runWithDatabase(async db => {
//     console.info(await db.getSetCount());
// });

// await runWithDatabase(async db => {
//     for (let i = 3000; i < 3100; i++) {
//         await db.createSet('test ' + i);
//     }
//     await db.commit();
// });

// await runWithDatabase(async (db) => {
//     assertEquals(await db.getSetCount(), 0);
//     assert(await db.createSet('test1'));
//     assertEquals(await db.getSetCount(), 1);
//     assert(await db.createSet('test2'));
//     assertEquals(await db.getSetCount(), 2);
//     assert(await db.createSet('test1'));
//     assertEquals(await db.getSetCount(), 2);
//     assert(await db.createSet('test3'));
//     assertEquals(await db.getSetCount(), 3);
//     await db.commit();
// });
