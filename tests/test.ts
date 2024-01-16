import {
  AND,
  BETWEEN,
  EQ,
  GE,
  GT,
  IDbDocSet,
  LE,
  LT,
  NOT,
  OptionalId,
  OR,
  query,
  SLICE,
} from "../mod.ts";
import { encoder } from "../src/utils/buffer.ts";
import { AlreadyExistError } from "../src/utils/errors.ts";
import { Runtime } from "../src/utils/runtime.ts";
import { assert, assertEquals, assertThrowsAsync } from "./test.dep.ts";
import {
  assertQueryEquals,
  dumpObjectToFile,
  ignoreMassiveTests,
  run,
  runWithDatabase,
} from "./test_util.ts";

export { run };

// await new Promise((r) => setTimeout(r, 300));

console.info("preparing test data...");

// create/get() sets

let expectedSetCount = 0;

runWithDatabase(async function createSet(db) {
  var set = await db.createKvSet("test");
  expectedSetCount++;
  await set.set("testkey", "testval");
  await set.set("testkey2", "testval2");
  assertEquals(await set.get("testkey"), "testval");
  assertEquals(await set.get("testkey2"), "testval2");
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function createSet_oldStyle(db) {
  var set = await db.createSet("test_old");
  expectedSetCount++;
  await set.set("testkey", "testval");
  await set.set("testkey2", "testval2");
  assertEquals(await set.get("testkey"), "testval");
  assertEquals(await set.get("testkey2"), "testval2");
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function Set_get(db) {
  var set = db.getSet("test");
  assertEquals(await set!.get("testkey"), "testval");
  assertEquals(await set!.get("testkey2"), "testval2");
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function Set_getAll(db) {
  var set = db.getSet("test");
  assertEquals(await set!.getAll(), [
    { key: "testkey", value: "testval" },
    { key: "testkey2", value: "testval2" },
  ]);
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function Set_forEach(db) {
  var set = db.getSet("test");
  const all: any[] = [];
  await set!.forEach((key, value) => {
    all.push({ key, value });
  });
  assertEquals(all, [
    { key: "testkey", value: "testval" },
    { key: "testkey2", value: "testval2" },
  ]);
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function Set_delete(db) {
  var set = db.getSet("test");
  await set!.delete("testkey");
  assertEquals(await set!.get("testkey"), null);
  assertEquals(await set!.get("testkey2"), "testval2");
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function getSetCount(db) {
  assertEquals(await db.getSetCount(), expectedSetCount);
  assert(await db.createSet("testCount1"));
  expectedSetCount++;
  assertEquals(await db.getSetCount(), expectedSetCount);
  assert(await db.createSet("testCount2"));
  expectedSetCount++;
  assertEquals(await db.getSetCount(), expectedSetCount);
  assert(await db.createSet("testCount1"));
  assertEquals(await db.getSetCount(), expectedSetCount);
  assert(await db.createSet("testCount3"));
  expectedSetCount++;
  assertEquals(await db.getSetCount(), expectedSetCount);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function deleteSet(db) {
  assertEquals(await db.deleteSet("testCount3", "kv"), true);
  expectedSetCount--;
  assertEquals(await db.getSet("testCount3").exists(), false);
  assertEquals(await db.getSetCount(), expectedSetCount);
  assertEquals(await db.commit(), true);

  assertEquals(await db.deleteKvSet("testCount2"), true);
  expectedSetCount--;
  assertEquals(await db.getSet("testCount2").exists(), false);
  assertEquals(await db.getSetCount(), expectedSetCount);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function deleteSet_afterModify(db) {
  const set = db.getSet("testCount1");
  assert(set);
  await set.set("somechange", "somevalue");
  assertEquals(await db.deleteSet("testCount1", "kv"), true);
  expectedSetCount--;
  assertEquals(await db.getSet("testCount1").exists(), false);
  assertEquals(await db.getSetCount(), expectedSetCount);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function deleteSet_check(db) {
  assertEquals(await db.getSet("testCount1").exists(), false);
  assertEquals(await db.getSet("testCount3").exists(), false);
  assertEquals(await db.deleteSet("testCount1", "kv"), false);
  assertEquals(await db.deleteSet("testCount3", "kv"), false);
  assertEquals(await db.getSetCount(), expectedSetCount);
  assertEquals(await db.commit(), false);
});

// create/get() document sets

interface TestUser {
  id: number;
  username: string;
  gender?: "m" | "f";
}

runWithDatabase(async function DocSet_insert(db) {
  var set = await db.createDocSet<TestUser>("testdoc");
  await set.insert({ "username": "btrdb" });
  await set.insert({ "username": "test" });
  assertEquals(await set.get(1), { "id": 1, "username": "btrdb" });
  assertEquals(await set.get(2), { "id": 2, "username": "test" });
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_insert_oldStyle(db) {
  var set = await db.createSet<TestUser>("testdoc_old", "doc");
  await set.insert({ "username": "btrdb" });
  await set.insert({ "username": "test" });
  assertEquals(await set.get(1), { "id": 1, "username": "btrdb" });
  assertEquals(await set.get(2), { "id": 2, "username": "test" });
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_upsert(db) {
  var set = await db.createDocSet<TestUser>("testdoc");
  await set.upsert({ "id": 1, "username": "whatdb" });
  await set.upsert({ "id": 2, "username": "nobody" });
  assertEquals(await set.get(1), { "id": 1, "username": "whatdb" });
  assertEquals(await set.get(2), { "id": 2, "username": "nobody" });
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_get(db) {
  var set = db.getDocSet<TestUser>("testdoc");
  assertEquals(await set!.get(1), { "id": 1, "username": "whatdb" });
  assertEquals(await set!.get(2), { "id": 2, "username": "nobody" });
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function DocSet_get_oldStyle(db) {
  var set = db.getSet("testdoc", "doc");
  assertEquals(await set!.get(1), { "id": 1, "username": "whatdb" });
  assertEquals(await set!.get(2), { "id": 2, "username": "nobody" });
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function DocSet_getAll(db) {
  var set = db.getDocSet<TestUser>("testdoc");
  assertEquals(await set!.getAll(), [
    { "id": 1, "username": "whatdb" },
    { "id": 2, "username": "nobody" },
  ]);
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function DocSet_forEach(db) {
  var set = db.getDocSet<TestUser>("testdoc");
  const all: any[] = [];
  await set!.forEach((doc) => {
    all.push(doc);
  });
  assertEquals(all, [
    { "id": 1, "username": "whatdb" },
    { "id": 2, "username": "nobody" },
  ]);
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function DocSet_getIds(db) {
  var set = db.getDocSet<TestUser>("testdoc");
  assertEquals(await set!.getIds(), [1, 2]);
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function DocSet_delete(db) {
  var set = db.getDocSet<TestUser>("testdoc");
  await set!.delete(1);
  assertEquals(await set!.getAll(), [{ "id": 2, "username": "nobody" }]);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_deleteSet(db) {
  var set = await db.createDocSet<TestUser>("testdoc_delete_test");
  await set.insert({ "username": "btrdb" });
  await set.insert({ "username": "test" });
  assertEquals(await set.get(1), { "id": 1, "username": "btrdb" });
  assertEquals(await set.get(2), { "id": 2, "username": "test" });
  assertEquals(await db.commit(), true);

  assertEquals(await set.exists(), true);
  await db.deleteDocSet("testdoc_delete_test");
  assertEquals(await set.exists(), false);
  assertEquals(await db.commit(), true);
});

let longString = "";
for (let i = 0; i < 10000; i++) {
  longString += Math.floor(Math.abs(Math.sin(i + 1)) * 100000000000).toString();
}

runWithDatabase(async function DocSet_largeDocument(db) {
  var set = await db.getSet<TestUser>("testdoc", "doc");
  await set!.insert({ "username": longString });
  assertEquals(await set!.getAll(), [{ "id": 2, "username": "nobody" }, {
    "id": 3,
    "username": longString,
  }]);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_blob(db) {
  var set = await db.createSet<any>("testblob", "doc");
  const buffer = encoder.encode(longString);
  await set!.insert({ data: buffer });
  await set!.insert({ data: new Uint8Array(0) });
  assertEquals(await set!.getAll(), [
    { id: 1, data: buffer },
    { id: 2, data: new Uint8Array(0) },
  ]);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_largeDocument_after_index(db) {
  var set = await db.getSet<TestUser>("testdoc", "doc");
  assertEquals(await set!.getAll(), [{ "id": 2, "username": "nobody" }, {
    "id": 3,
    "username": longString,
  }]);
  await set!.useIndexes({
    username10: (u) => u.username.substr(0, Math.min(10, u.username.length)),
  });
  assertEquals(await set!.findIndex("username10", longString.substr(0, 10)), [{
    "id": 3,
    "username": longString,
  }]);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_largeDocument_before_index(db) {
  var set = await db.getSet<TestUser>("testdoc", "doc");
  await set!.useIndexes({
    username10: (u) => u.username.substr(0, Math.min(10, u.username.length)),
    username8: (u) => u.username.substr(0, Math.min(8, u.username.length)),
  });
  assertEquals(await set!.findIndex("username8", longString.substr(0, 8)), [{
    "id": 3,
    "username": longString,
  }]);
  assertEquals(await db.commit(), true);
});

// use indexes

runWithDatabase(async function DocSet_indexes_before_insert(db) {
  var set = await db.createSet<TestUser>("testindexes", "doc");
  await set.useIndexes({
    username: { unique: true, key: (u) => u.username },
    gender: (u) => u.gender,
  });
  await set.insert({ "username": "btrdb", gender: "m" });
  await set.insert({ "username": "test", gender: "m" });
  await set.insert({ "username": "the3rd", gender: "f" });
  assertEquals(await set.findIndex("username", "btrdb"), [{
    "id": 1,
    "username": "btrdb",
    "gender": "m",
  }]);
  assertEquals(await set.findIndex("username", "test"), [{
    "id": 2,
    "username": "test",
    "gender": "m",
  }]);
  assertEquals(await set.findIndex("username", "the3rd"), [{
    "id": 3,
    "username": "the3rd",
    "gender": "f",
  }]);
  assertEquals(await set.findIndex("gender", "m"), [
    { "id": 1, "username": "btrdb", "gender": "m" },
    { "id": 2, "username": "test", "gender": "m" },
  ]);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_indexes_after_insert(db) {
  var set = await db.createSet<TestUser>("testindexes2", "doc");
  await set.insert({ "username": "btrdb", "gender": "m" });
  await set.insert({ "username": "test", "gender": "m" });
  await set.insert({ "username": "the3rd", "gender": "f" });
  await set.useIndexes({
    gender: (u) => u.gender,
    id_username: (u) => u.id + "_" + u.username,
    username: { unique: true, key: (u) => u.username },
  });
  assertEquals(await set.findIndex("username", "btrdb"), [{
    "id": 1,
    "username": "btrdb",
    "gender": "m",
  }]);
  assertEquals(await set.findIndex("username", "test"), [{
    "id": 2,
    "username": "test",
    "gender": "m",
  }]);
  assertEquals(await set.findIndex("username", "the3rd"), [{
    "id": 3,
    "username": "the3rd",
    "gender": "f",
  }]);
  assertEquals(await set.findIndex("id_username", "1_btrdb"), [{
    "id": 1,
    "username": "btrdb",
    "gender": "m",
  }]);
  assertEquals(await set.findIndex("id_username", "2_test"), [{
    "id": 2,
    "username": "test",
    "gender": "m",
  }]);
  assertEquals(await set.findIndex("id_username", "3_the3rd"), [{
    "id": 3,
    "username": "the3rd",
    "gender": "f",
  }]);
  assertEquals(await set.findIndex("gender", "m"), [
    { "id": 1, "username": "btrdb", "gender": "m" },
    { "id": 2, "username": "test", "gender": "m" },
  ]);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_indexes_unique_insert_failed(db) {
  var set = db.getSet<TestUser>("testindexes2", "doc");
  const query = async () => [
    await set.getAll(),
    await set.findIndex("gender", "m"),
    await set.findIndex("gender", "f"),
    await set.findIndex("id_username", "1_btrdb"),
    await set.findIndex("id_username", "2_test"),
    await set.findIndex("id_username", "3_the3rd"),
    await set.findIndex("id_username", "2_btrdb"),
    await set.findIndex("id_username", "4_btrdb"),
  ];
  const oldQueryResults = await query();
  await assertThrowsAsync(async () => {
    await set.insert({ "username": "btrdb", "gender": "f" });
  }, AlreadyExistError);
  await assertThrowsAsync(async () => {
    await set.update({ id: 2, "username": "btrdb", "gender": "f" });
  }, AlreadyExistError);
  assertEquals(oldQueryResults, await query());
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_indexes_after_upsert(db) {
  var set = await db.getSet<TestUser>("testindexes2", "doc");
  assert(set);
  await set.upsert({ "id": 2, "username": "nobody", "gender": "f" });
  assertEquals(await set.getAll(), [
    { "id": 1, "username": "btrdb", "gender": "m" },
    { "id": 2, "username": "nobody", "gender": "f" },
    { "id": 3, "username": "the3rd", "gender": "f" },
  ]);
  assertEquals(await set.findIndex("username", "btrdb"), [{
    "id": 1,
    "username": "btrdb",
    "gender": "m",
  }]);
  assertEquals(await set.findIndex("username", "nobody"), [{
    "id": 2,
    "username": "nobody",
    "gender": "f",
  }]);
  assertEquals(await set.findIndex("gender", "m"), [
    { "id": 1, "username": "btrdb", "gender": "m" },
  ]);
  assertEquals(await set.findIndex("gender", "f"), [
    { "id": 2, "username": "nobody", "gender": "f" },
    { "id": 3, "username": "the3rd", "gender": "f" },
  ]);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_indexes_after_delete(db) {
  var set = db.getSet("testindexes2", "doc");
  assert(set);
  await set.delete(1);
  assertEquals(await set.getAll(), [
    { "id": 2, "username": "nobody", "gender": "f" },
    { "id": 3, "username": "the3rd", "gender": "f" },
  ]);
  assertEquals(await set.findIndex("username", "btrdb"), []);
  assertEquals(await set.findIndex("username", "nobody"), [{
    "id": 2,
    "username": "nobody",
    "gender": "f",
  }]);
  assertEquals(await set.findIndex("gender", "m"), []);
  assertEquals(await set.findIndex("gender", "f"), [
    { "id": 2, "username": "nobody", "gender": "f" },
    { "id": 3, "username": "the3rd", "gender": "f" },
  ]);
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function DocSet_indexes_demo(db) {
  interface User {
    id: number;
    username: string;
    status: "online" | "offline";
    role: "admin" | "user";
  }

  const userSet = await db.createDocSet<User>("users", {
    indexes: {
      status: (user) => user.status,
      // define "status" index, which indexing the value of user.status for each user in the set

      role: (user) => user.role,

      username: { unique: true, key: (user) => user.username },
      // define "username" unique index, which does not allow duplicated username.

      onlineAdmin: (user) => user.status == "online" && user.role == "admin",
      // define "onlineAdmin" index, the value is a computed boolean.
    },
  });

  await userSet.insert({ username: "yuuza", status: "online", role: "user" });
  await userSet.insert({ username: "foo", status: "offline", role: "admin" });
  await userSet.insert({ username: "bar", status: "online", role: "admin" });
  assertEquals(await db.commit(), true);

  // Get all online users
  assertEquals(await userSet.findIndex("status", "online"), [
    { username: "yuuza", status: "online", role: "user", id: 1 },
    { username: "bar", status: "online", role: "admin", id: 3 },
  ]);

  // Get all users named 'yuuza'
  assertEquals(await userSet.findIndex("username", "yuuza"), [{
    username: "yuuza",
    status: "online",
    role: "user",
    id: 1,
  }]);

  // Get all online admins
  assertEquals(await userSet.findIndex("onlineAdmin", true), [{
    username: "bar",
    status: "online",
    role: "admin",
    id: 3,
  }]);

  // Get all offline admins
  assertEquals(
    await userSet.query(
      AND(
        EQ("status", "offline"),
        EQ("role", "admin"),
      ),
    ),
    [{ username: "foo", status: "offline", role: "admin", id: 2 }],
  );

  // Get all online users, but exclude id 1.
  assertEquals(
    await userSet.query(
      AND(
        EQ("status", "online"),
        NOT(EQ("id", 1)), // "id" is a special "index" name
      ),
    ),
    [{ username: "bar", status: "online", role: "admin", id: 3 }],
  );
});

interface User {
  id: number;
  username: string;
  status: "online" | "offline";
  role: "admin" | "user";
}

const usersNoId: OptionalId<User>[] = [
  { username: "yuuza0", status: "online", role: "admin" },
  { username: "yuuza3", status: "online", role: "user" },
  { username: "foo", status: "offline", role: "admin" },
  { username: "foo2", status: "online", role: "user" },
  { username: "foo3", status: "offline", role: "user" },
  { username: "bar", status: "offline", role: "admin" },
  { username: "bar2", status: "online", role: "admin" },
] as any;

const users = usersNoId.map((x, i) => ({ id: i + 1, ...x })) as User[];

runWithDatabase(async function DocSet_query(db) {
  const userSet = await db.createSet<User>("users2", "doc");

  // Define indexes on the set and update indexes if needed.
  await userSet.useIndexes({
    status: (user) => user.status,
    // define "status" index, which indexing the value of user.status for each user in the set

    role: (user) => user.role,

    username: { unique: true, key: (user) => user.username },
    // define "username" unique index, which does not allow duplicated username.

    onlineAdmin: (user) => user.status == "online" && user.role == "admin",
    // define "onlineAdmin" index, the value is a computed boolean.

    status_role: (user) => [user.status, user.role],
  });

  for (const doc of usersNoId) {
    await userSet.insert(doc as OptionalId<User>);
  }
  assertEquals(await db.commit(), true);

  checkQueryString();

  await checkQuery(userSet);
});

function checkQueryString() {
  assertQueryEquals(
    query`
      status == ${"online"}
      AND role == ${"admin"}
    `,
    AND(
      EQ("status", "online"),
      EQ("role", "admin"),
    ),
  );
  assertQueryEquals(
    query`
      NOT(
        status == ${"offline"}
        OR role == ${"user"}
      )
    `,
    NOT(OR(
      EQ("status", "offline"),
      EQ("role", "user"),
    )),
  );
  assertQueryEquals(
    query`name == ${"foo"} AND age == ${123}`,
    AND(
      EQ("name", "foo"),
      EQ("age", 123),
    ),
  );
  assertQueryEquals(
    query`(name >= ${"foo"}) AND (age <= ${123})`,
    AND(
      GE("name", "foo"),
      LE("age", 123),
    ),
  );
  assertQueryEquals(
    query`(name > ${"foo"}) AND (age < ${123})`,
    AND(
      GT("name", "foo"),
      LT("age", 123),
    ),
  );
  assertQueryEquals(
    query`NOT((name > ${"foo"}) AND (age < ${123}))`,
    NOT(AND(
      GT("name", "foo"),
      LT("age", 123),
    )),
  );
  assertQueryEquals(
    query`name == ${"foo"} AND age == ${123} AND c == ${1111}`,
    AND(
      EQ("name", "foo"),
      EQ("age", 123),
      EQ("c", 1111),
    ),
  );
  assertQueryEquals(
    query`name == ${"foo"} OR age == ${123} OR c == ${3} OR d == ${4}`,
    OR(
      EQ("name", "foo"),
      EQ("age", 123),
      EQ("c", 3),
      EQ("d", 4),
    ),
  );
}

async function checkQuery(userSet: IDbDocSet<User>) {
  // query functions
  assertEquals(
    await userSet.query(AND(
      EQ("status", "online"),
      EQ("role", "admin"),
    )),
    users.filter((x) => x.status == "online" && x.role == "admin"),
  );

  // query expressions
  assertEquals(
    await userSet.query`
      status == ${"online"}
      AND role == ${"admin"}
    `,
    users.filter((x) => x.status == "online" && x.role == "admin"),
  );

  // query expressions (inverted name and value)
  assertEquals(
    await userSet.query`
      ${"online"} == status
      AND ${"admin"} == role
    `,
    users.filter((x) => x.status == "online" && x.role == "admin"),
  );

  // old style query expressions
  assertEquals(
    await userSet.query(query`
      status == ${"online"}
      AND role == ${"admin"}
    `),
    users.filter((x) => x.status == "online" && x.role == "admin"),
  );
  assertEquals(
    await userSet.query(query`
      ${"online"} == status
      AND ${"admin"} == role
    `),
    users.filter((x) => x.status == "online" && x.role == "admin"),
  );

  // queryCount
  assertEquals(
    await userSet.queryCount`
      status == ${"online"}
      AND role == ${"admin"}
    `,
    users.filter((x) => x.status == "online" && x.role == "admin").length,
  );
  assertEquals(
    await userSet.queryCount`
      ${"online"} == status
      AND ${"admin"} == role
    `,
    users.filter((x) => x.status == "online" && x.role == "admin").length,
  );

  // composite index
  assertEquals(
    await userSet.query(
      EQ("status_role", ["online", "admin"]),
    ),
    users.filter((x) => x.status == "online" && x.role == "admin"),
  );

  assertEquals(
    await userSet.query(query`
      status_role == ${["online", "admin"]}
    `),
    users.filter((x) => x.status == "online" && x.role == "admin"),
  );

  assertEquals(
    await userSet.query(OR(
      EQ("status", "offline"),
      EQ("role", "user"),
    )),
    users.filter((x) => x.status == "offline" || x.role == "user"),
  );

  assertEquals(
    await userSet.query(NOT(OR(
      EQ("status", "offline"),
      EQ("role", "user"),
    ))),
    await userSet.query(AND(
      EQ("status", "online"),
      EQ("role", "admin"),
    )),
  );

  assertEquals(
    await userSet.query(query`
      NOT(
        status == ${"offline"}
        OR role == ${"user"}
      )
    `),
    await userSet.query(AND(
      EQ("status", "online"),
      EQ("role", "admin"),
    )),
  );

  assertEquals(
    await userSet.query(
      BETWEEN("id", 2, 5, false, false),
    ),
    users.filter((x) => x.id > 2 && x.id < 5),
  );

  assertEquals(
    await userSet.query(query`
      id > ${2} AND id < ${5}
    `),
    users.filter((x) => x.id > 2 && x.id < 5),
  );

  assertEquals(
    await userSet.query(
      BETWEEN("id", 2, 5, true, true),
    ),
    users.filter((x) => x.id >= 2 && x.id <= 5),
  );

  assertEquals(
    await userSet.query(query`
      id >= ${2} AND id <= ${5}
    `),
    users.filter((x) => x.id >= 2 && x.id <= 5),
  );

  assertEquals(
    await userSet.query(
      AND(
        GE("id", 2),
        LE("id", 5),
      ),
    ),
    users.filter((x) => x.id >= 2 && x.id <= 5),
  );

  assertEquals(
    await userSet.query(
      NOT(AND(
        GE("id", 2),
        LE("id", 5),
      )),
    ),
    users.filter((x) => !(x.id >= 2 && x.id <= 5)),
  );

  assertEquals(
    await userSet.query(
      OR(
        LT("id", 2),
        GT("id", 5),
      ),
    ),
    users.filter((x) => !(x.id >= 2 && x.id <= 5)),
  );

  assertEquals(
    await userSet.query(
      GT("id", 2),
    ),
    users.filter((x) => x.id > 2),
  );

  assertEquals(
    await userSet.query(
      LT("id", 5),
    ),
    users.filter((x) => x.id < 5),
  );

  assertEquals(
    await userSet.query(
      LE("id", 5),
    ),
    users.filter((x) => x.id <= 5),
  );

  // SLICE (SKIP/LIMIT)
  assertEquals(
    await userSet.query(
      SLICE(
        GT("id", 1),
        1,
        2,
      ),
    ),
    users.filter((x) => x.id > 1).slice(1, 1 + 2),
  );

  assertEquals(
    await userSet.query`
      id > ${1} SKIP ${1}
    `,
    users.filter((x) => x.id > 1).slice(1),
  );

  assertEquals(
    await userSet.query`
      id > ${1} LIMIT ${2}
    `,
    users.filter((x) => x.id > 1).slice(0, 0 + 2),
  );

  assertEquals(
    await userSet.query`
      id > ${1} SKIP ${1} LIMIT ${2}
    `,
    users.filter((x) => x.id > 1).slice(1, 1 + 2),
  );
}

// transaction

runWithDatabase(async function transaction(db) {
  // (db as any).transaction.debug = true;

  function getSet() {
    return db.getSet<any>("transaction", "doc");
  }

  function runTransactionDeleteAll() {
    return db.runTransaction(async () => {
      const set = (await getSet())!;
      for (const id of await set.getIds()) {
        await set.delete(id);
      }
    });
  }

  await db.runTransaction(async () => {
    await db.createSet("transaction", "doc");
  });

  await db.runTransaction(async () => {
    const set = (await getSet())!;
    await set.useIndexes({
      val: { key: (x) => x.val, unique: true },
    });
  });

  // some concurrent transactions
  await Promise.all([
    db.runTransaction(async () => {
      const set = (await getSet())!;
      await set.insert({ val: 1 });
    }),
    db.runTransaction(async () => {
      const set = (await getSet())!;
      await set.insert({ val: 2 });
    }),
    db.runTransaction(async () => {
      const set = (await getSet())!;
      await set.insert({ val: 3 });
    }),
  ]);

  assertEquals(
    (await (await getSet())!.getAll()).map((x) => x.val).sort(),
    [1, 2, 3],
  );

  await runTransactionDeleteAll();

  // some concurrent transactions, some will fail
  const testValues = new Array(100).fill(0).map((x, i) => i);
  const failedValues = [13, 36, 45, 46, 49, 90];
  await Promise.all(
    testValues.map(async (x) => {
      try {
        await db.runTransaction(async () => {
          if (failedValues.includes(x)) {
            throw new Error("just failed");
          }
          const set = (await getSet())!;
          await set.insert({ val: x });
        });
      } catch (err) {
        if (err.message != "just failed") {
          throw err;
        }
      }
    }),
  );

  assertEquals(
    (await (await getSet())!.getAll()).map((x) => x.val).sort((a, b) => a - b),
    testValues.filter((x) => !failedValues.includes(x)),
  );
});

// dump/import

runWithDatabase(async function dumpAndImport(db) {
  await Runtime.writeTextFile(
    "testdata/dump.json",
    JSON.stringify(await db.dump()),
  );

  for (const obj of await db.getObjects()) {
    await db.deleteObject(obj.name, obj.type);
  }

  await db.import(JSON.parse(await Runtime.readTextFile("testdata/dump.json")));

  await db.commit();

  const userSet = await db.getSet<User>("users2", "doc");
  await checkQuery(userSet!);

  var testkv = db.getSet("test");
  assertEquals(await testkv!.getAll(), [
    { key: "testkey2", value: "testval2" },
  ]);
});

// create/get named snapshot

runWithDatabase(async function namedSnap1(db) {
  await db.createSnapshot("before_a");
  var set = await db.createSet("namedsnap1");
  await set.set("somekey", "somevalue");
  await db.createSnapshot("a");
  assertEquals(await db.commit(), true); // commit "a"
});

runWithDatabase(async function namedSnap2(db) {
  var set = db.getSet("namedsnap1");
  assertEquals(await set!.get("somekey"), "somevalue");
  var snap = await db.getSnapshot("before_a");
  assert(snap);
  assertEquals(await snap!.getSet("namedsnap1").exists(), false);
  assert(await snap!.getSet("test").exists());
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function namedSnap3(db) {
  var set = db.getSet("namedsnap1");
  await set!.set("somekey", "someothervalue");
  await set!.set("newkey", "newvalue");
  await db.createSnapshot("b");
  assertEquals(await db.commit(), true);
});

runWithDatabase(async function namedSnap4(db) {
  var set = db.getSet("namedsnap1");
  assertEquals(await set!.getCount(), 2);
  assertEquals(await set!.get("somekey"), "someothervalue");
  assertEquals(await set!.get("newkey"), "newvalue");
  var snap = await db.getSnapshot("a");
  var snapset = await snap!.getSet("namedsnap1");
  assertEquals(await snapset!.getCount(), 1);
  assertEquals(await snapset!.get("somekey"), "somevalue");
  var snap2 = await snap!.getSnapshot("before_a");
  assertEquals(await snap2!.getSet("namedsnap1").exists(), false);
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function rebuild(db) {
  await db.rebuild();
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function check_after_rebuild(db) {
  var set = db.getSet("test");
  assertEquals(await set!.get("testkey2"), "testval2");
  await checkQuery((await db.getSet<User>("users2", "doc"))!);
});

// set/get() lots of records (concurrently)

const concurrentKeys = new Array(200).fill(0).map((x, i) =>
  Math.floor(Math.abs(Math.sin(i)) * 100000000000).toString()
);
const expectedConcurrentKeys = [...new Set(concurrentKeys)].sort();
const expectedConcurrentSetNames = [
  ...new Set(concurrentKeys.map((k) => "k" + k[0])),
].sort();

runWithDatabase(async function setGetCommitConcurrent(db) {
  var set = (await db.createSet("testConcurrent"))!;
  var tasks: Promise<void>[] = [];
  for (const k of concurrentKeys) {
    // tasks.push((async () => {
    await ((async () => {
      await set.set("key" + k, "val" + k);
      const val = await set!.get("key" + k);
      assertEquals(val, "val" + k);
      await db.commit();
    })());
  }
  await Promise.all(tasks);
  assertEquals(await set.getCount(), expectedConcurrentKeys.length);
  assertEquals(await db.commit(), false);
});

runWithDatabase(async function getAfterConcurrent(db) {
  var set = (db.getSet("testConcurrent"))!;
  assertEquals(await set.getCount(), expectedConcurrentKeys.length);
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

runWithDatabase(async function createSetGetCommitConcurrent(db) {
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
    (await db.getObjects()).filter((x) => x.name[0] == "k"),
    expectedConcurrentSetNames.map((x) => ({ type: "kv", name: x })),
  );
});

// set/get() lots of key-value records

const keys = (ignoreMassiveTests == "ignore")
  ? []
  : new Array(100000).fill(0).map((x, i) =>
    Math.floor(Math.abs(Math.sin(i + 1)) * 100000000000).toString()
  );
const expectedKeys = [...new Set(keys)].sort();

runWithDatabase(async function setMassive(db) {
  var set = (await db.createSet("testMassive"))!;
  for (const k of keys) {
    await set.set("key" + k, "val" + k);
  }
  assertEquals(await set.getCount(), expectedKeys.length);
  assertEquals(await db.commit(), true);
}, ignoreMassiveTests);

runWithDatabase(async function getMassive(db) {
  var set = (db.getSet("testMassive"))!;
  const errors = [];
  assertEquals(await set.getCount(), expectedKeys.length);
  for (const k of keys) {
    const val = await set!.get("key" + k);
    if (val != "val" + k) {
      errors.push("expect " + k + " got " + val);
      await set!.get("key" + k);
    }
  }
  assertEquals(errors, []);
  assertEquals(await db.commit(), false);
}, ignoreMassiveTests);

runWithDatabase(async function getKeys(db) {
  var set = db.getSet("testMassive");
  var r = await set!.getKeys();
  var uniqueKeys = [...new Set(keys)].map((x) => "key" + x).sort();
  for (let i = 0; i < uniqueKeys.length; i++) {
    if (uniqueKeys[i] != r[i]) {
      throw new Error(`${uniqueKeys[i]} != ${r[i]}, i = ${i}}`);
    }
  }
  // await db.commit();
}, ignoreMassiveTests);

interface TestDoc {
  id: string;
}

const lastThreeSet = (ignoreMassiveTests == "ignore")
  ? []
  : [...new Set(keys.map((x) => x.substr(x.length - 3)))]
    .sort();
const lastThreeMap = lastThreeSet.map((
  three,
) => [three, keys.filter((x) => x.endsWith(three)).sort()]);

runWithDatabase(async function DocSet_upsertMassive(db) {
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
  assertEquals(await set.getCount(), expectedKeys.length);
}, ignoreMassiveTests);

const fives = keys.map((x) => x.substring(0, 5));
const fivesSet = [...new Set(fives)].sort();
const fiveLastThreeSet = [
  ...new Set(fivesSet.map((x) => x.substring(x.length - 3))),
];
const fiveLastThreeMap = fiveLastThreeSet.map(
  (three) => [three, fivesSet.filter((x) => x.endsWith(three)).sort()] as const,
);

runWithDatabase(async function DocSet_upsertOverrideMassive(db) {
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
      "test failed, dump is created under 'testdata' folder: ",
      { cause: error },
    );
  }
  assertEquals(await set.getCount(), fivesSet.length);
}, ignoreMassiveTests);

const AD_toDelete = fivesSet.filter((x) => x[1] == "0");
const AD_expectedIndexResults = fiveLastThreeMap.map((x) =>
  x[1].filter((x) => x[1] != "0")
);

runWithDatabase(async function DocSet_deleteMassive(db) {
  var set = await db.createSet<TestDoc>("docMassive2", "doc");
  // await dumpObjectToFile(
  //   "testdata/five_before_delete_tree.txt",
  //   await (set as any)._dump(),
  // );
  let actualIndexResults = null;
  try {
    for (const k of AD_toDelete) {
      // console.info('delete', k);
      await set.delete(k);
    }
    assertEquals(await db.commit(), true);
    actualIndexResults = (await Promise.all(
      fiveLastThreeSet.map((three) => set.findIndex("lastThree", three)),
    )).map((x) => x.map((x) => x.id).sort());
    assertEquals(actualIndexResults, AD_expectedIndexResults);
    assertEquals(await set.getCount(), fivesSet.length - AD_toDelete.length);
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
      AD_expectedIndexResults,
    );
    throw new Error(
      "test failed, dump is created under 'testdata' folder: " + error,
    );
  }
}, ignoreMassiveTests);

runWithDatabase(async function DocSet_deleteMassive_reopen_verify(db) {
  var set = await db.createSet<TestDoc>("docMassive2", "doc");
  // await dumpObjectToFile(
  //   "testdata/five_before_delete_tree.txt",
  //   await (set as any)._dump(),
  // );
  let actualIndexResults = null;
  try {
    actualIndexResults = (await Promise.all(
      fiveLastThreeSet.map((three) => set.findIndex("lastThree", three)),
    )).map((x) => x.map((x) => x.id).sort());
    assertEquals(actualIndexResults, AD_expectedIndexResults);
    assertEquals(await set.getCount(), fivesSet.length - AD_toDelete.length);
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
      AD_expectedIndexResults,
    );
    throw new Error(
      "test failed, dump is created under 'testdata' folder: " + error,
    );
  }
}, ignoreMassiveTests);

runWithDatabase(async function rebuild_after_massive(db) {
  await db.rebuild();
  assertEquals(await db.commit(), false);
}, ignoreMassiveTests);

runWithDatabase(async function check_after_rebuild(db) {
  var kv = db.getSet("test");
  assertEquals(await kv!.get("testkey2"), "testval2");
  await checkQuery((await db.getSet<User>("users2", "doc"))!);

  let actualIndexResults = null;
  const expectedIndexResults = fiveLastThreeMap.map((x) =>
    x[1].filter((x) => x[1] != "0")
  );
  var set = (await db.getSet<TestDoc>("docMassive2", "doc"))!;
  try {
    actualIndexResults = (await Promise.all(
      fiveLastThreeSet.map((three) => set.findIndex("lastThree", three)),
    )).map((x) => x.map((x) => x.id).sort());
    assertEquals(actualIndexResults, expectedIndexResults);
    assertEquals(await set.getCount(), fivesSet.length - AD_toDelete.length);
  } catch (error) {
    console.info(error);
    console.info("generating dump...");
    await dumpObjectToFile(
      "testdata/after_rebuild_tree.txt",
      await (set as any)._dump(),
    );
    await dumpObjectToFile(
      "testdata/after_rebuild_actual.txt",
      actualIndexResults,
    );
    await dumpObjectToFile(
      "testdata/after_rebuild_expected.txt",
      expectedIndexResults,
    );
    throw new Error(
      "test failed, dump is created under 'testdata' folder: " + error,
    );
  }
}, ignoreMassiveTests);

runWithDatabase(async function delete_massive_then_rebuild(db) {
  await db.deleteDocSet("docMassive");
  await db.rebuild();
}, ignoreMassiveTests);

// runWithDatabase(async function lotsOfCommits (db) {
//     var set = db.getSet("test"); // commit "b"
//     for (let i = 0; i < 10000000; i++) {
//         await set!.set('somevar', 'val' + i);
//         await db.commit();
//     }
// });

// runWithDatabase(async db => {
//     console.info(await db.getSetCount());
// });

// runWithDatabase(async db => {
//     for (let i = 3000; i < 3100; i++) {
//         await db.createSet('test ' + i);
//     }
//     await db.commit();
// });

if (globalThis.Deno) {
  if (globalThis.Deno.args[0] == "run") {
    (async () => {
      const result = await run();
      globalThis.Deno.exit(result.total == result.passed ? 0 : 1);
    })();
  }
}
