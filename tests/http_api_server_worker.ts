import { Database, HttpApiServer } from "../mod.ts";

export async function startTestServer() {
  try {
    await Deno.remove("testdata/httptest.db");
  } catch {}
  const db = await Database.openFile("testdata/httptest.db");
  (self as any).postMessage?.({ msg: "ready" });
  new HttpApiServer(db).serve(
    Deno.listen({ hostname: "127.0.0.1", port: 1234 }),
  );
}

if ((self as any).postMessage) {
  startTestServer();
}
