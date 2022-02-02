import { Database, HttpApiServer } from "../mod.ts";

export async function startTestServer() {
  try {
    await Deno.remove("testdata/httptest.db");
  } catch {}
  const db = await Database.openFile("testdata/httptest.db");
  // const db = await Database.openMemory();
  (self as any).postMessage?.({ msg: "ready" });
  db.autoCommit = true;
  new HttpApiServer(db).serve(
    Deno.listen({ hostname: "127.0.0.1", port: 1234 }),
  );
}

if ((self as any).postMessage) {
  startTestServer();
}
