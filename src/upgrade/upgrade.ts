import { Database } from "../database/database.ts";
import { InFileStorage, PageStorage } from "../pages/storage.ts";
import * as oldBtrdb from "./oldVersion/btrdb_v0_7_2.js";
import { Runtime } from "../utils/runtime.ts";

export async function checkUpgrade(storage: PageStorage) {
  if (!(storage instanceof InFileStorage)) return;
  const zeroPageBuffer = new Uint8Array(storage.pageSize);
  await storage._readPageBuffer(0, zeroPageBuffer);
  if (zeroPageBuffer[0] == 1) {
    // older version's SuperPage
    const path = storage.filePath!;
    const tmpPath = path + ".upgrade.tmp";
    storage.close();

    const db = new Database();
    await db.openFile(tmpPath);

    oldBtrdb.setRuntimeImplementaion(Runtime);
    const old = new oldBtrdb.Database();
    await old.openFile(path);

    const dumped = await old.dump();
    old.close();

    await db.import(dumped);
    await db.commit();
    db.close();

    await Runtime.rename(path, path + ".pre-upgrade.backup");
    await Runtime.rename(tmpPath, path);

    await storage.openPath(path);
    return true;
  }
  return false;
}
