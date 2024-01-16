import { assert, assertEquals } from "./test.dep.ts";
import { startTestServer } from "./http_api_server_worker.ts";

for (const RUN_IN_WORKER of [false, true]) {
  const prefix = RUN_IN_WORKER ? "(worker) " : "";
  const port = RUN_IN_WORKER ? 1234 : 1235;

  let stopServer: () => Promise<void>;
  Deno.test(prefix + "start server", {
    sanitizeOps: false,
    sanitizeResources: false,
  }, async () => {
    if (RUN_IN_WORKER) {
      console.info("Starting HTTP API worker...");
      const worker = new Worker(
        new URL("./http_api_server_worker.ts", import.meta.url).href,
        {
          type: "module",
          deno: {
            namespace: true,
          },
        } as any,
      );
      await new Promise<void>((resolve) => {
        worker.onmessage = (e) => {
          if (e.data.msg == "ready") {
            resolve();
          }
        };
      });
      stopServer = async () => {
        worker.postMessage({ msg: "close" });
        await new Promise<void>((resolve) => {
          worker.onmessage = (e) => {
            if (e.data.msg == "closed") {
              resolve();
            }
          };
        });
      };
      console.info("Worker ready.");
    } else {
      stopServer = await startTestServer(port);
    }
  });

  const testOptions = RUN_IN_WORKER
    ? { sanitizeOps: false }
    : { sanitizeOps: false, sanitizeResources: false };

  function testApi(
    method: "GET" | "DELETE",
    path: string,
    expected?: any,
  ): Promise<void>;
  function testApi(
    method: "POST" | "PUT",
    path: string,
    body?: any,
    expected?: any,
  ): Promise<void>;
  async function testApi(
    method: string,
    path: string,
    body?: any,
    expected?: any,
  ) {
    if (method == "GET" || method == "DELETE") {
      expected = body;
      body = undefined;
    }
    console.info(method, path);
    const resp = await fetch(`http://127.0.0.1:${port}${path}`, {
      method,
      body: body ? JSON.stringify(body) : undefined,
    });
    if (expected?.error) {
      assertEquals([(await resp.json()).error, resp.status], [
        expected.error,
        expected.status,
      ]);
      return;
    }
    assert(resp.ok, `HTTP status ${resp.status} ${resp.statusText}`);
    if (resp.headers.get("content-type")?.includes("application/json")) {
      assertEquals(await resp.json(), expected);
    } else {
      // reads the (empty) response to release resources
      assertEquals([await resp.text(), expected], ["", undefined]);
    }
  }

  function jsonUri(obj: any) {
    return encodeURIComponent(JSON.stringify(obj));
  }

  Deno.test(
    prefix + "http api (db objects)",
    testOptions,
    async () => {
      await testApi("GET", "/objects", []);
      await testApi("DELETE", "/sets/kv:test", false);
      await testApi("POST", "/sets/kv:test");
      await testApi("POST", "/sets/doc:users");
      await testApi("GET", "/objects", [
        { type: "doc", name: "users" },
        { type: "kv", name: "test" },
      ]);
      await testApi("DELETE", "/sets/doc:users", true);
      await testApi("DELETE", "/sets/doc:users", false);
      await testApi("GET", "/objects", [
        { type: "kv", name: "test" },
      ]);
      await testApi("DELETE", "/sets/kv:test", true);
      await testApi("DELETE", "/sets/kv:test", false);
      await testApi("GET", "/objects", []);
    },
  );

  Deno.test(
    prefix + "http api (kv sets)",
    testOptions,
    async () => {
      await testApi("POST", "/sets/kv:test");
      await testApi("PUT", `/sets/kv:test/${jsonUri("key1")}`, "value1", true);
      await testApi("GET", `/sets/kv:test/${jsonUri("key1")}`, "value1");
      await testApi("GET", `/sets/kv:test/?count`, 1);
      await testApi("PUT", `/sets/kv:test/${jsonUri(123)}`, 456, true);
      await testApi("GET", `/sets/kv:test/${jsonUri(123)}`, 456);
      await testApi("GET", `/sets/kv:test/?count`, 2);
      await testApi(
        "PUT",
        `/sets/kv:test/${jsonUri("with/ & %")}`,
        "okay?",
        true,
      );
      await testApi("GET", `/sets/kv:test/${jsonUri("with/ & %")}`, "okay?");
      await testApi("GET", `/sets/kv:test/?count`, 3);
      await testApi("DELETE", `/sets/kv:test/${jsonUri("with/ & %")}`);
      await testApi("GET", `/sets/kv:test/${jsonUri("with/ & %")}`, null);
      await testApi("GET", `/sets/kv:test/?count`, 2);
      await testApi("GET", `/sets/kv:test/?keys`, [
        123,
        "key1",
      ]);
      await testApi("GET", `/sets/kv:test/`, [
        { key: 123, value: 456 },
        { key: "key1", value: "value1" },
      ]);
      await testApi("DELETE", `/sets/kv:test/${jsonUri("noSuchKey")}`, {
        error: "key not found",
        status: 404,
      });
      await testApi("GET", `/sets/kv:noSuchSet/${jsonUri("lol")}`, {
        error: "set not found",
        status: 404,
      });
    },
  );

  Deno.test(
    prefix + "http api (doc sets)",
    testOptions,
    async () => {
      await testApi("POST", "/sets/doc:testdoc");
      await testApi("POST", `/sets/doc:testdoc/?insert`, {
        username: "btrdb",
        role: "admin",
      }, 1);
      await testApi("POST", `/sets/doc:testdoc/?insert`, {
        username: "foo",
        role: "user",
      }, 2);
      await testApi("GET", `/sets/doc:testdoc/1`, {
        id: 1,
        username: "btrdb",
        role: "admin",
      });
      await testApi("GET", `/sets/doc:testdoc/2`, {
        id: 2,
        username: "foo",
        role: "user",
      });
      await testApi("GET", `/sets/doc:testdoc/3`, null);
      await testApi("GET", `/sets/doc:testdoc/?ids`, [1, 2]);
      await testApi("GET", `/sets/doc:testdoc/?count`, 2);
      await testApi("GET", `/sets/doc:testdoc/?query=id>{}&value=1`, [
        {
          id: 2,
          username: "foo",
          role: "user",
        },
      ]);
      await testApi("POST", `/sets/doc:testdoc/?query`, {
        query: "id < {}",
        values: [2],
      }, [
        {
          id: 1,
          username: "btrdb",
          role: "admin",
        },
      ]);
      await testApi("POST", `/sets/doc:testdoc/?indexes`, {
        username: { unique: true, key: "username" },
        role: "role",
      });
      await testApi(
        "GET",
        `/sets/doc:testdoc/?query=username=={}&value=${jsonUri("foo")}`,
        [
          {
            id: 2,
            username: "foo",
            role: "user",
          },
        ],
      );
      await testApi("PUT", `/sets/doc:testdoc/2`, {
        id: 2,
        username: "foobar",
        role: "user",
      });
      await testApi("GET", `/sets/doc:testdoc/2`, {
        id: 2,
        username: "foobar",
        role: "user",
      });
      await testApi("DELETE", `/sets/doc:testdoc/2`);
      await testApi("GET", `/sets/doc:testdoc/2`, null);
      await testApi("DELETE", `/sets/doc:testdoc/2`, {
        error: "key not found",
        status: 404,
      });
      await testApi("GET", `/sets/doc:noSuchSet/${jsonUri("lol")}`, {
        error: "set not found",
        status: 404,
      });
    },
  );

  Deno.test(prefix + "stop server", {
    sanitizeOps: false,
    sanitizeResources: false,
  }, async () => {
    await stopServer();
  });
}
