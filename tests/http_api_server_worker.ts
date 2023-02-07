import { Database, HttpApiServer } from "../mod.ts";

export async function startTestServer(port = 1234) {
  // const db = await Database.openFile("testdata/httptest.db");
  const db = await Database.openMemory();
  (self as any).postMessage?.({ msg: "ready" });
  db.autoCommit = true;
  const listener = Deno.listen({ hostname: "127.0.0.1", port });
  const serveTask = new HttpApiServer(db).serve(listener);
  return async () => {
    db.close();
    listener.close();
    await serveTask;
  };
}

if ((self as any).postMessage) {
  const close = await startTestServer();
  (self as any).addEventListener("message", (e: MessageEvent) => {
    if (e.data.msg === "close") {
      close().then(() => {
        (self as any).postMessage({ msg: "closed" });
      });
    }
  });
}
