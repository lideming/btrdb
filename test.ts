import { Database } from "./src/database.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.74.0/testing/asserts.ts";

const testFile = "testdata/testdb.db";

await new Promise((r) => setTimeout(r, 300));

await recreateDatabase();

// create/get() sets

await runWithDatabase(async function createSet(db) {
  var set = await db.createSet("test");
  await set.set("testkey", "testval");
  console.info(await set.get("testkey"));
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function getSet(db) {
  var set = await db.getSet("test");
  console.info(await set!.get("testkey"));
  assertEquals(await db.commit(), false);
});

// create/get() document sets

await runWithDatabase(async function createDocSet(db) {
  var set = await db.createSet("testdoc", "doc");
  await set.set("testkey", { "id": 1, "username": "btrdb" });
  console.info(await set.get("testkey"));
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function getDocSet(db) {
  var set = await db.getSet("testdoc", "doc");
  console.info(await set!.get("testkey"));
  assertEquals(await db.commit(), false);
});

// get snapshots

await runWithDatabase(async function createSetSnap(db) {
  var set = await db.createSet("snap1");
  await set.set("somekey", "somevalue");
  assertEquals(await db.commit(), true); // commit "a"
});

await runWithDatabase(async function checkSnap(db) {
  var set = await db.getSet("snap1");
  assertEquals(await set!.get("somekey"), "somevalue");
  var snap = await db.getPrevSnapshot(); // before commit "a"
  assertEquals(await snap!.getSet("snap1"), null);
  assert(!!await snap!.getSet("test"));
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function changeSnap(db) {
  var set = await db.getSet("snap1");
  await set!.set("somekey", "someothervalue");
  await set!.set("newkey", "newvalue");
  assertEquals(await db.commit(), true); // commit "b"
});

await runWithDatabase(async function checkSnap2(db) {
  var set = await db.getSet("snap1"); // commit "b"
  assertEquals(await set!.count, 2);
  assertEquals(await set!.get("somekey"), "someothervalue");
  assertEquals(await set!.get("newkey"), "newvalue");
  var snap = await db.getPrevSnapshot(); // commit "a"
  var snapset = await snap!.getSet("snap1");
  assertEquals(await snapset!.count, 1);
  assertEquals(await snapset!.get("somekey"), "somevalue");
  var snap2 = await snap!.getPrevSnapshot(); // before commit "a"
  assertEquals(await snap2!.getSet("snap1"), null);
  assertEquals(await db.commit(), false);
});

// set/get() lots of records (concurrently)

var concurrentKeys = new Array(50).fill(0).map((x) =>
  Math.floor(Math.random() * 100000000000).toString()
);

await runWithDatabase(async function setGetCommitConcurrent(db) {
  var set = (await db.createSet("testConcurrent"))!;
  var tasks: Promise<void>[] = [];
  for (const k of concurrentKeys) {
    tasks.push((async () => {
      await set.set("key" + k, "val" + k);
      // console.info('<<< ' + k);
      const val = await set!.get("key" + k);
      if (val == "val" + k) {
        // console.info('>>> ' + val);
      } else {
        console.info(">>> expect " + k + " got " + val);
      }
      await db.commit();
    })());
  }
  await Promise.all(tasks);
  console.info("set.count", set.count);
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function getAfterConcurrent(db) {
  var set = (await db.getSet("testConcurrent"))!;
  console.info("set.count", set.count);
  let errors = [];
  for (const k of concurrentKeys) {
    const val = await set!.get("key" + k);
    if (val != "val" + k) {
      errors.push("expect " + k + " got " + val);
    }
  }
  console.info("read done, total", concurrentKeys.length);
  if (errors) {
    console.info("errors", errors.length, errors);
  }
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function createSetGetCommitConcurrent(db) {
  var tasks: Promise<void>[] = [];
  for (const k of concurrentKeys) {
    tasks.push((async () => {
      const set = await db.createSet("k" + k[0]);
      await set.set("key" + k, "val" + k);
      // console.info('<<< ' + k);
      const val = await set!.get("key" + k);
      if (val == "val" + k) {
        // console.info('>>> ' + val);
      } else {
        console.info(">>> expect " + k + " got " + val);
      }
      await db.commit();
    })());
  }
  await Promise.all(tasks);
  assertEquals(await db.commit(), false);
  assertEquals(
    (await db.getSetNames()).filter((x) => x[0] == "k"),
    [...new Set(concurrentKeys.map((k) => "k" + k[0]))].sort(),
  );
});

// set/get() lots of records

var keys = new Array(10000).fill(0).map((x) =>
  Math.floor(Math.random() * 100000000000).toString()
);

await runWithDatabase(async function set10k(db) {
  var set = (await db.getSet("test"))!;
  for (const k of keys) {
    await set.set("key" + k, "val" + k);
  }
  console.info("set.count", set.count);
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function get10k(db) {
  var set = (await db.getSet("test"))!;
  console.info("set.count", set.count);
  for (const k of keys) {
    const val = await set!.get("key" + k);
    if (val != "val" + k) {
      console.error("expect", k, "got", val);
    }
  }
  console.info("read done");
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function getKeys(db) {
  var set = await db.getSet("test");
  var r = await set!.getKeys();
  var uniqueKeys = [...new Set(keys)].map((x) => "key" + x).sort();
  for (let i = 0; i < uniqueKeys.length; i++) {
    if (uniqueKeys[i] != r[i]) {
      throw new Error(`${uniqueKeys[i]} != ${r[i]}, i = ${i}}`);
    }
  }
  // await db.commit();
});

// await runWithDatabase(async function lotsOfCommits (db) {
//     var set = await db.getSet("test"); // commit "b"
//     for (let i = 0; i < 10000000; i++) {
//         await set!.set('somevar', 'val' + i);
//         await db.commit();
//     }
// });

console.info("Tests finished.");

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

async function recreateDatabase() {
  await Deno.mkdir("testdata", { recursive: true });
  try {
    await Deno.remove(testFile);
  } catch {}
}

async function runWithDatabase(func: (db: Database) => Promise<void>) {
  console.info("");
  console.info("=============================");
  console.info("==> run " + func.name);
  console.info("=============================");
  var db = new Database();
  await db.openFile(testFile);
  await func(db);
  db.close();
}
