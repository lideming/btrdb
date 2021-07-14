import { Database, IDbDocSet } from "./mod.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.100.0/testing/asserts.ts";
import { PAGESIZE } from "./src/page.ts";

const testFile = "testdata/testdb.db";

await new Promise((r) => setTimeout(r, 300));

await recreateDatabase();

// create/get() sets

await runWithDatabase(async function createSet(db) {
  var set = await db.createSet("test");
  await set.set("testkey", "testval");
  await set.set("testkey2", "testval2");
  assertEquals(await set.get("testkey"), "testval");
  assertEquals(await set.get("testkey2"), "testval2");
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function Set_get(db) {
  var set = await db.getSet("test");
  assertEquals(await set!.get("testkey"), "testval");
  assertEquals(await set!.get("testkey2"), "testval2");
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function Set_delete(db) {
  var set = await db.getSet("test");
  await set!.delete("testkey");
  assertEquals(await set!.get("testkey"), null);
  assertEquals(await set!.get("testkey2"), "testval2");
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function getSetCount(db) {
  assertEquals(await db.getSetCount(), 1);
  assert(await db.createSet("testCount1"));
  assertEquals(await db.getSetCount(), 2);
  assert(await db.createSet("testCount2"));
  assertEquals(await db.getSetCount(), 3);
  assert(await db.createSet("testCount1"));
  assertEquals(await db.getSetCount(), 3);
  assert(await db.createSet("testCount3"));
  assertEquals(await db.getSetCount(), 4);
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function deleteSet(db) {
  assertEquals(await db.deleteSet("testCount3"), true);
  assertEquals(await db.getSet("testCount3"), null);
  assertEquals(await db.getSetCount(), 3);
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function deleteSet_afterModify(db) {
  const set = await db.getSet("testCount1");
  assert(set);
  await set.set("somechange", "somevalue");
  assertEquals(await db.deleteSet("testCount1"), true);
  assertEquals(await db.getSet("testCount1"), null);
  assertEquals(await db.getSetCount(), 2);
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function deleteSet_check(db) {
  assertEquals(await db.getSet("testCount1"), null);
  assertEquals(await db.getSet("testCount3"), null);
  assertEquals(await db.deleteSet("testCount1"), false);
  assertEquals(await db.deleteSet("testCount3"), false);
  assertEquals(await db.getSetCount(), 2);
  assertEquals(await db.commit(), false);
});

// create/get() document sets

interface TestUser {
  id: number;
  username: string;
  gender?: "m" | "f";
}

await runWithDatabase(async function DocSet_insert(db) {
  var set = await db.createSet<TestUser>("testdoc", "doc");
  await set.insert({ "username": "btrdb" });
  await set.insert({ "username": "test" });
  assertEquals(await set.get(1), { "id": 1, "username": "btrdb" });
  assertEquals(await set.get(2), { "id": 2, "username": "test" });
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function DocSet_upsert(db) {
  var set = await db.createSet<TestUser>("testdoc", "doc");
  await set.upsert({ "id": 1, "username": "whatdb" });
  await set.upsert({ "id": 2, "username": "nobody" });
  assertEquals(await set.get(1), { "id": 1, "username": "whatdb" });
  assertEquals(await set.get(2), { "id": 2, "username": "nobody" });
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function DocSet_get(db) {
  var set = await db.getSet("testdoc", "doc");
  assertEquals(await set!.get(1), { "id": 1, "username": "whatdb" });
  assertEquals(await set!.get(2), { "id": 2, "username": "nobody" });
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function DocSet_getAll(db) {
  var set = await db.getSet("testdoc", "doc");
  assertEquals(await set!.getAll(), [
    { "id": 1, "username": "whatdb" },
    { "id": 2, "username": "nobody" },
  ]);
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function DocSet_getIds(db) {
  var set = await db.getSet("testdoc", "doc");
  assertEquals(await set!.getIds(), [1, 2]);
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function DocSet_delete(db) {
  var set = await db.getSet("testdoc", "doc");
  await set!.delete(1);
  assertEquals(await set!.getAll(), [{ "id": 2, "username": "nobody" }]);
  assertEquals(await db.commit(), true);
});

// use indexes

await runWithDatabase(async function DocSet_indexes_before_insert(db) {
  var set = await db.createSet<TestUser>("testindexes", "doc");
  await set.useIndexes({
    username: { unique: true, key: (u) => u.username },
    gender: (u) => u.gender,
  });
  await set.insert({ "username": "btrdb", gender: "m" });
  await set.insert({ "username": "test", gender: "m" });
  await set.insert({ "username": "the3rd", gender: "f" });
  assertEquals(await set.getFromIndex("username", "btrdb"), {
    "id": 1,
    "username": "btrdb",
    "gender": "m",
  });
  assertEquals(await set.getFromIndex("username", "test"), {
    "id": 2,
    "username": "test",
    "gender": "m",
  });
  assertEquals(await set.getFromIndex("username", "the3rd"), {
    "id": 3,
    "username": "the3rd",
    "gender": "f",
  });
  assertEquals(await set.findIndex("gender", "m"), [
    { "id": 1, "username": "btrdb", "gender": "m" },
    { "id": 2, "username": "test", "gender": "m" },
  ]);
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function DocSet_indexes_after_insert(db) {
  var set = await db.createSet<TestUser>("testindexes2", "doc");
  await set.insert({ "username": "btrdb", "gender": "m" });
  await set.insert({ "username": "test", "gender": "m" });
  await set.insert({ "username": "the3rd", "gender": "f" });
  await set.useIndexes({
    username: { unique: true, key: (u) => u.username },
    gender: (u) => u.gender,
  });
  assertEquals(await set.getFromIndex("username", "btrdb"), {
    "id": 1,
    "username": "btrdb",
    "gender": "m",
  });
  assertEquals(await set.getFromIndex("username", "test"), {
    "id": 2,
    "username": "test",
    "gender": "m",
  });
  assertEquals(await set.getFromIndex("username", "the3rd"), {
    "id": 3,
    "username": "the3rd",
    "gender": "f",
  });
  assertEquals(await set.findIndex("gender", "m"), [
    { "id": 1, "username": "btrdb", "gender": "m" },
    { "id": 2, "username": "test", "gender": "m" },
  ]);
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function DocSet_indexes_after_upsert(db) {
  var set = await db.getSet<TestUser>("testindexes2", "doc");
  assert(set);
  await set.upsert({ "id": 2, "username": "nobody", "gender": "f" });
  assertEquals(await set.getAll(), [
    { "id": 1, "username": "btrdb", "gender": "m" },
    { "id": 2, "username": "nobody", "gender": "f" },
    { "id": 3, "username": "the3rd", "gender": "f" },
  ]);
  assertEquals(await set.getFromIndex("username", "btrdb"), {
    "id": 1,
    "username": "btrdb",
    "gender": "m",
  });
  assertEquals(await set.getFromIndex("username", "nobody"), {
    "id": 2,
    "username": "nobody",
    "gender": "f",
  });
  assertEquals(await set.findIndex("gender", "m"), [
    { "id": 1, "username": "btrdb", "gender": "m" },
  ]);
  assertEquals(await set.findIndex("gender", "f"), [
    { "id": 2, "username": "nobody", "gender": "f" },
    { "id": 3, "username": "the3rd", "gender": "f" },
  ]);
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function DocSet_indexes_after_delete(db) {
  var set = await db.getSet("testindexes2", "doc");
  assert(set);
  await set.delete(1);
  assertEquals(await set.getAll(), [
    { "id": 2, "username": "nobody", "gender": "f" },
    { "id": 3, "username": "the3rd", "gender": "f" },
  ]);
  assertEquals(await set.getFromIndex("username", "btrdb"), null);
  assertEquals(await set.getFromIndex("username", "nobody"), {
    "id": 2,
    "username": "nobody",
    "gender": "f",
  });
  assertEquals(await set.findIndex("gender", "m"), []);
  assertEquals(await set.findIndex("gender", "f"), [
    { "id": 2, "username": "nobody", "gender": "f" },
    { "id": 3, "username": "the3rd", "gender": "f" },
  ]);
  assertEquals(await db.commit(), true);
});

// TODO remove this condition once we have finished DataPage
if (PAGESIZE != 256) {
  await runWithDatabase(async function DocSet_indexes_demo(db) {
    interface User {
      id: number;
      username: string;
      status: "online" | "offline";
      role: "admin" | "user";
    }

    const userSet = await db.createSet<User>("users", "doc");

    // Define indexes on the set and update indexes if needed.
    userSet.useIndexes({
      status: (user) => user.status,
      // define "status" index, which indexing the value of user.status for each user in the set

      username: { unique: true, key: (user) => user.username },
      // define "username" unique index, which does not allow duplicated username.

      onlineAdmin: (user) => user.status == "online" && user.role == "admin",
      // define "onlineAdmin" index, the value is a computed boolean.
    });

    await userSet.insert({ username: "yuuza", status: "online", role: "user" });
    await userSet.insert({ username: "foo", status: "offline", role: "admin" });
    await userSet.insert({ username: "bar", status: "online", role: "admin" });

    // Get all online users
    console.info(await userSet.findIndex("status", "online"), [
      { username: "yuuza", status: "online", role: "user", id: 1 },
      { username: "bar", status: "online", role: "admin", id: 3 },
    ]);

    // Get all users named 'yuuza'
    console.info(await userSet.findIndex("username", "yuuza"), [{
      username: "yuuza",
      status: "online",
      role: "user",
      id: 1,
    }]);

    // Get all online admins
    console.info(await userSet.findIndex("onlineAdmin", true), [{
      username: "bar",
      status: "online",
      role: "admin",
      id: 3,
    }]);
  });
}

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

const concurrentKeys = new Array(200).fill(0).map((x, i) =>
  Math.floor(Math.abs(Math.sin(i)) * 100000000000).toString()
);
const expectedConcurrentKeys = [...new Set(concurrentKeys)].sort();
const expectedConcurrentSetNames = [
  ...new Set(concurrentKeys.map((k) => "k" + k[0])),
].sort();

await runWithDatabase(async function setGetCommitConcurrent(db) {
  var set = (await db.createSet("testConcurrent"))!;
  var tasks: Promise<void>[] = [];
  for (const k of concurrentKeys) {
    tasks.push((async () => {
      await set.set("key" + k, "val" + k);
      const val = await set!.get("key" + k);
      assertEquals(val, "val" + k);
      await db.commit();
    })());
  }
  await Promise.all(tasks);
  assertEquals(set.count, expectedConcurrentKeys.length);
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function getAfterConcurrent(db) {
  var set = (await db.getSet("testConcurrent"))!;
  assertEquals(set.count, expectedConcurrentKeys.length);
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
  assertEquals(errors, []);
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
    expectedConcurrentSetNames,
  );
});

// set/get() lots of key-value records

const keys = new Array(100000).fill(0).map((x, i) =>
  Math.floor(Math.abs(Math.sin(i + 1)) * 100000000000).toString()
);
const expectedKeys = [...new Set(keys)].sort();

await runWithDatabase(async function setMassive(db) {
  var set = (await db.createSet("testMassive"))!;
  for (const k of keys) {
    await set.set("key" + k, "val" + k);
  }
  assertEquals(set.count, expectedKeys.length);
  assertEquals(await db.commit(), true);
});

await runWithDatabase(async function getMassive(db) {
  var set = (await db.getSet("testMassive"))!;
  const errors = [];
  assertEquals(set.count, expectedKeys.length);
  for (const k of keys) {
    const val = await set!.get("key" + k);
    if (val != "val" + k) {
      errors.push("expect " + k + " got " + val);
    }
  }
  assertEquals(errors, []);
  assertEquals(await db.commit(), false);
});

await runWithDatabase(async function getKeys(db) {
  var set = await db.getSet("testMassive");
  var r = await set!.getKeys();
  var uniqueKeys = [...new Set(keys)].map((x) => "key" + x).sort();
  for (let i = 0; i < uniqueKeys.length; i++) {
    if (uniqueKeys[i] != r[i]) {
      throw new Error(`${uniqueKeys[i]} != ${r[i]}, i = ${i}}`);
    }
  }
  // await db.commit();
});

interface TestDoc {
  id: string;
}

const lastThreeSet = [...new Set(keys.map((x) => x.substr(x.length - 3)))]
  .sort();
const lastThreeMap = lastThreeSet.map((
  three,
) => [three, keys.filter((x) => x.endsWith(three)).sort()]);

await runWithDatabase(async function DocSet_upsertMassive(db) {
  var set = await db.createSet<TestDoc>("docMassive", "doc");
  await set.useIndexes({
    lastThree: (d) => d.id.substr(d.id.length - 3),
  });
  for (const k of keys) {
    await set.upsert({ id: k });
  }
  assertEquals(await db.commit(), true);
  const actualIndexResults = (await Promise.all(
    lastThreeSet.map((three) => set.findIndex("lastThree", three)),
  )).map((x) => x.map((x) => x.id).sort());
  const expectedIndexResults = lastThreeMap.map((x) => x[1]);
  try {
    assertEquals(actualIndexResults, expectedIndexResults);
  } catch (error) {
    await dumpObjectToFile("testdata/tree.txt", await (set as any)._dump());
    await dumpObjectToFile("testdata/actual.txt", actualIndexResults);
    await dumpObjectToFile("testdata/expected.txt", expectedIndexResults);
    throw new Error(
      "test failed, dump is created under 'testdata' folder: " + error,
    );
  }
  assertEquals(set.count, expectedKeys.length);
});

const fives = keys.map((x) => x.substring(0, 5));
const fivesSet = [...new Set(fives)].sort();
const fiveLastThreeSet = [
  ...new Set(fivesSet.map((x) => x.substring(x.length - 3))),
];
const fiveLastThreeMap = fiveLastThreeSet.map(
  (three) => [three, fivesSet.filter((x) => x.endsWith(three)).sort()] as const,
);

await runWithDatabase(async function DocSet_upsertOverrideMassive(db) {
  var set = await db.createSet<TestDoc>("docMassive2", "doc");
  await set.useIndexes({
    lastThree: (d) => d.id.substr(d.id.length - 3),
  });
  const expectedIndexResults = fiveLastThreeMap.map((x) => x[1]);
  let actualIndexResults = null;
  try {
    for (const k of fives) {
      await set.upsert({ id: k });
    }
    assertEquals(await db.commit(), true);
    actualIndexResults = (await Promise.all(
      fiveLastThreeSet.map((three) => set.findIndex("lastThree", three)),
    )).map((x) => x.map((x) => x.id).sort());
    assertEquals(actualIndexResults, expectedIndexResults);
  } catch (error) {
    await dumpObjectToFile(
      "testdata/five_tree.txt",
      await (set as any)._dump(),
    );
    await dumpObjectToFile("testdata/five_actual.txt", actualIndexResults);
    await dumpObjectToFile("testdata/five_expected.txt", expectedIndexResults);
    throw new Error(
      "test failed, dump is created under 'testdata' folder: " + error,
    );
  }
  assertEquals(set.count, fivesSet.length);
});

await runWithDatabase(async function DocSet_deleteMassive(db) {
  var set = await db.createSet<TestDoc>("docMassive2", "doc");
  // await dumpObjectToFile(
  //   "testdata/five_before_delete_tree.txt",
  //   await (set as any)._dump(),
  // );
  const toDelete = fivesSet.filter((x) => x[1] == "0");
  let actualIndexResults = null;
  const expectedIndexResults = fiveLastThreeMap.map((x) =>
    x[1].filter((x) => x[1] != "0")
  );
  try {
    for (const k of toDelete) {
      // console.info('delete', k);
      await set.delete(k);
    }
    assertEquals(await db.commit(), true);
    actualIndexResults = (await Promise.all(
      fiveLastThreeSet.map((three) => set.findIndex("lastThree", three)),
    )).map((x) => x.map((x) => x.id).sort());
    assertEquals(actualIndexResults, expectedIndexResults);
    assertEquals(set.count, fivesSet.length - toDelete.length);
  } catch (error) {
    console.info(error);
    console.info("generating dump...");
    await dumpObjectToFile(
      "testdata/five_delete_tree.txt",
      await (set as any)._dump(),
    );
    await dumpObjectToFile(
      "testdata/five_delete_actual.txt",
      actualIndexResults,
    );
    await dumpObjectToFile(
      "testdata/five_delete_expected.txt",
      expectedIndexResults,
    );
    throw new Error(
      "test failed, dump is created under 'testdata' folder: " + error,
    );
  }
});

// await runWithDatabase(async function lotsOfCommits (db) {
//     var set = await db.getSet("test"); // commit "b"
//     for (let i = 0; i < 10000000; i++) {
//         await set!.set('somevar', 'val' + i);
//         await db.commit();
//     }
// });

// await runWithDatabase(async db => {
//     console.info(await db.getSetCount());
// });

// await runWithDatabase(async db => {
//     for (let i = 3000; i < 3100; i++) {
//         await db.createSet('test ' + i);
//     }
//     await db.commit();
// });

async function recreateDatabase() {
  await Deno.mkdir("testdata", { recursive: true });
  try {
    await Deno.remove(testFile);
  } catch {}
}

function dumpObjectToFile(file: string, obj: any) {
  const inspectOptions: Deno.InspectOptions = {
    colors: false,
    iterableLimit: 100000,
    depth: 10,
    compact: false,
    trailingComma: true,
  };
  return Deno.writeTextFile(file, Deno.inspect(obj, inspectOptions));
}

async function runWithDatabase(
  func: (db: Database) => Promise<void>,
  only?: boolean,
) {
  // console.info("");
  // console.info("=============================");
  // console.info("==> test " + func.name);
  // console.info("=============================");

  Deno.test({
    name: func.name,
    fn: async () => {
      console.info("\n=============================");
      console.time("open");
      const db = new Database();
      await db.openFile(testFile, { fsync: false });
      console.timeEnd("open");

      console.time("run");
      await func(db);
      console.timeEnd("run");
      db.close();

      const file = await Deno.open(testFile);
      const size = (await Deno.fstat(file.rid)).size;
      console.info("file size:", size, `(${size / PAGESIZE} pages)`);
      const storage = (db as any).storage;
      if (storage.written) {
        console.info(
          "space efficient:",
          (1 - (storage.writtenFreebytes / storage.written)).toFixed(3),
        );
      }
      console.info("");
      file.close();
    },
    only,
  });
}
