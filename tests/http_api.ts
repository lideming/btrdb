import { assert, assertEquals } from "./test.dep.ts";
import { startTestServer } from "./http_api_server_worker.ts";

const RUN_IN_WORKER = true;

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
  console.info("Worker ready.");
} else {
  await startTestServer();
}

const testOptions = RUN_IN_WORKER
  ? {}
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
  const resp = await fetch("http://127.0.0.1:1234" + path, {
    method,
    body: body ? JSON.stringify(body) : undefined,
  });
  assert(resp.ok, `HTTP status ${resp.status} ${resp.statusText}`);
  if (resp.headers.get("content-type")?.includes("application/json")) {
    return assertEquals(await resp.json(), expected);
  } else {
    // reads the (empty) response to release resources
    assertEquals([await resp.text(), expected], ["", undefined]);
  }
}

function jsonUri(obj: any) {
  return encodeURIComponent(JSON.stringify(obj));
}

Deno.test(
  "http api (db objects)",
  testOptions,
  async () => {
    await testApi("GET", "/objects", []);
    await testApi("POST", "/sets/kv:test");
    await testApi("POST", "/sets/doc:users");
    await testApi("GET", "/objects", [
      { type: "doc", name: "users" },
      { type: "kv", name: "test" },
    ]);
    await testApi("DELETE", "/sets/doc:users");
    await testApi("GET", "/objects", [
      { type: "kv", name: "test" },
    ]);
    await testApi("DELETE", "/sets/kv:test");
    await testApi("GET", "/objects", []);
  },
);

Deno.test(
  "http api (kv sets)",
  testOptions,
  async () => {
    await testApi("POST", "/sets/kv:test");
    await testApi("PUT", `/sets/kv:test/${jsonUri("key1")}`, "value1");
    await testApi("GET", `/sets/kv:test/${jsonUri("key1")}`, "value1");
    await testApi("GET", `/sets/kv:test/?count`, 1);
    await testApi("PUT", `/sets/kv:test/${jsonUri(123)}`, 456);
    await testApi("GET", `/sets/kv:test/${jsonUri(123)}`, 456);
    await testApi("GET", `/sets/kv:test/?count`, 2);
    await testApi("PUT", `/sets/kv:test/${jsonUri("with/ & %")}`, "okay?");
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
  },
);

Deno.test(
  "http api (doc sets)",
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
    await testApi("GET", `/sets/doc:testdoc/?ids`, [1, 2]);
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
      username: "username",
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
  },
);
