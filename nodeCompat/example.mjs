import { Database } from "../dist/btrdb.mjs";

const db = new Database();
await db.openFile("data.db");

const configSet = await db.createSet("config");
await configSet.set("username", "yuuza");

console.info(await configSet.get("username")); // "yuuza"

await db.commit();
