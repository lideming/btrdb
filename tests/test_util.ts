import { Database } from "../mod.ts";
import { PAGESIZE } from "../src/page.ts";
import { Runtime, RuntimeInspectOptions } from "../src/runtime.ts";
import { assertEquals } from "./test.dep.ts";

const testFile = "testdata/testdb.db";

export const ignoreMassiveTests: boolean | "ignore" = false as any;
const recreate: boolean = true;

const databaseTests: {
  func: (db: Database) => Promise<void>;
  only?: boolean | "ignore";
}[] = [];

if (recreate) {
  Runtime.test({ fn: recreateDatabase, name: "recreate database" });
}

export async function recreateDatabase() {
  await Runtime.mkdir("testdata", { recursive: true });
  try {
    await Runtime.remove(testFile);
  } catch {}
}

export function dumpObjectToFile(file: string, obj: any) {
  const inspectOptions: RuntimeInspectOptions = {
    colors: false,
    iterableLimit: 100000,
    depth: 10,
    compact: false,
    trailingComma: true,
  };
  return Runtime.writeTextFile(file, Runtime.inspect(obj, inspectOptions));
}

export function runWithDatabase(
  func: (db: Database) => Promise<void>,
  only?: boolean | "ignore",
) {
  Runtime.test({
    name: func.name,
    fn: () => runDbTest(func),
    only: only === true,
    ignore: only === "ignore",
  });

  databaseTests.push({ func, only });
}

export async function runDbTest(func: (db: Database) => Promise<void>) {
  console.time("open");
  const db = new Database();
  await db.openFile(testFile, { fsync: false });
  console.timeEnd("open");

  console.time("run");
  await func(db);
  console.timeEnd("run");
  db.close();

  const file = await Runtime.open(testFile);
  const size = (await file.stat()).size;
  console.info("file size:", size, `(${size / PAGESIZE} pages)`);
  const storage = (db as any).storage;
  if (storage.written) {
    console.info(
      "space efficient:",
      (1 - (storage.writtenFreebytes / storage.written)).toFixed(3),
    );
  }
  file.close();
}

export async function run() {
  if (recreate) {
    await recreateDatabase();
  }
  const useOnly = databaseTests.filter((x) => x.only === true).length > 0;
  let total = databaseTests.length, passed = 0, failed = 0, ignored = 0;
  for (const { func, only } of databaseTests) {
    if (only != "ignore" && (!useOnly || only)) {
      console.info("");
      console.info("=============================");
      console.info("==> test " + func.name);
      console.info("=============================");
      try {
        await runDbTest(func);
        passed++;
      } catch (error) {
        console.error("error in test", error);
        failed++;
      }
    } else {
      ignored++;
    }
  }
  const stat = { total, passed, failed, ignored };
  console.info("Tests completed", stat);
  return stat;
}

export function assertQueryEquals(a: any, b: any) {
  assertEquals(queryValueFilter(a), queryValueFilter(b));
}

export function queryValueFilter(obj: any): any {
  if (typeof obj == "object") {
    if (obj instanceof Array) {
      return obj.map(queryValueFilter);
    } else {
      const newobj = { ...obj };
      delete newobj["run"];
      for (const key in newobj) {
        if (Object.prototype.hasOwnProperty.call(newobj, key)) {
          newobj[key] = queryValueFilter(newobj[key]);
        }
      }
      return newobj;
    }
  }
  return obj;
}
