import { Database } from "./database.ts";
import { query } from "./queryString.ts";
import { IDbDocSet, IDbSet, IndexDef, KeySelector } from "./btrdb.d.ts";

export class HttpApiServer {
  constructor(
    public db: Database,
  ) {
  }
  async serve(listener: Deno.Listener) {
    for await (const conn of listener) {
      this.serveConn(conn);
    }
  }

  async serveConn(conn: Deno.Conn) {
    const httpConn = Deno.serveHttp(conn);
    for await (const requestEvent of httpConn) {
      await this.serveRequest(requestEvent);
    }
  }

  async serveRequest(event: Deno.RequestEvent) {
    try {
      const ret = await this.handler(event);
      if (ret === undefined) {
        event.respondWith(new Response(null, { status: 200 }));
      } else {
        event.respondWith(
          new Response(JSON.stringify(ret), {
            headers: {
              "content-type": "application/json",
            },
          }),
        );
      }
    } catch (error) {
      if (error instanceof ApiError) {
        event.respondWith(
          new Response(
            JSON.stringify({
              error: error.message,
            }),
            {
              status: error.statusCode,
              headers: {
                "content-type": "application/json",
              },
            },
          ),
        );
      } else {
        console.error(error);
        event.respondWith(new Response(null, { status: 500 }));
      }
    }
  }

  private async handler(event: Deno.RequestEvent) {
    const req = event.request;
    const url = new URL(req.url);
    const path = url.pathname.split("/").slice(
      1,
      url.pathname.endsWith("/") ? -1 : undefined,
    );
    // console.debug(req.method, path);
    if (path.length >= 2) {
      const [settype, setname] = decodeSetId(path[1]);
      if (path[0] == "sets") {
        if (path.length === 2 && url.search == "") {
          if (req.method == "POST") {
            // Create a set
            await this.db.createSet(setname, settype as any);
            return;
          } else if (req.method == "DELETE") {
            // Delete a set
            await this.db.deleteSet(setname, settype);
            return;
          }
        }
        if (settype == "kv") {
          // Key-value Sets
          if (path[2]) {
            const key = JSON.parse(decodeURIComponent(path[2]));
            if (req.method == "GET") {
              // Get a value by key
              const set = await this.getSet(setname, settype);
              return await set.get(key);
            } else if (req.method == "PUT") {
              // Set a key-value pair
              const set = await this.getSet(setname, settype);
              await set.set(key, await req.json());
              return;
            } else if (req.method == "DELETE") {
              // Delete a key
              const set = await this.getSet(setname, settype);
              if (!await set.delete(key)) {
                throw new ApiError(404, `key not found`);
              }
              return;
            }
          } else if (url.search == "?keys") {
            // List keys
            const set = await this.getSet(setname, settype);
            return await set.getKeys();
          } else if (url.search == "?count") {
            // Get count of pairs
            const set = await this.getSet(setname, settype);
            return await set.getCount();
          } else if (url.search == "") {
            // List key-value pairs
            const set = await this.getSet(setname, settype);
            return await set.getAll();
          }
        } else if (settype == "doc") {
          if (path[2]) {
            const id = JSON.parse(decodeURIComponent(path[2]));
            if (req.method == "GET") {
              // Get a document by id
              const set = await this.getSet(setname, settype);
              return await set.get(id);
            } else if (req.method == "PUT") {
              // Upsert a document
              const set = await this.getSet(setname, settype);
              const doc = await req.json();
              await set.upsert(doc);
              return;
            } else if (req.method == "DELETE") {
              // Delete a document by id
              const set = await this.getSet(setname, settype);
              if (!await set.delete(id)) {
                throw new ApiError(404, `key not found`);
              }
              return;
            }
          } else {
            if (req.method == "GET") {
              if (url.searchParams.get("query")) {
                // Query documents (GET)
                const set = await this.getSet(setname, settype);
                const querystr = url.searchParams.get("query")!;
                const values = url.searchParams.getAll("value").map((x) =>
                  JSON.parse(x)
                );
                const q = query(querystr.split("{}") as any, ...values);
                return await set.query(q);
              } else if (url.search == "?count") {
                // Get documents count
                const set = await this.getSet(setname, settype);
                return await set.getCount();
              } else if (url.search == "?ids") {
                // Get document ids
                const set = await this.getSet(setname, settype);
                return await set.getIds();
              }
            } else if (req.method == "POST") {
              if (url.search == "?query") {
                // Query documents (POST)
                const set = await this.getSet(setname, settype);
                const { query: querystr, values } = await req.json() as {
                  query: string;
                  values: any[];
                };
                const q = query(querystr.split("{}") as any, ...values);
                return await set.query(q);
              } else if (url.search == "?insert") {
                // Insert a document
                const set = await this.getSet(setname, settype);
                const doc = await req.json();
                await set.insert(doc);
                return doc["id"];
              } else if (url.search == "?indexes") {
                // Set indexes
                const set = await this.getSet(setname, settype);
                const indexes = Object.fromEntries(
                  Object.entries(await req.json() as IndexDef<any>)
                    .map(([name, def]) => {
                      if (typeof def == "string") {
                        def = propNameToKeySelector(def);
                      } else {
                        def = {
                          key: propNameToKeySelector((def as any).key),
                          unique: (def as any).unique || false,
                        };
                      }
                      return [name, def];
                    }),
                );
                await set.useIndexes(indexes);
                return;
              }
            }
          }
        }
      }
    } else if (path.length == 1 && path[0] == "objects") {
      // List objects
      return await this.db.getObjects();
    }

    throw new ApiError(400, "Unknown URL");
  }

  private async getSet(name: string, type: "kv"): Promise<IDbSet>;
  private async getSet(name: string, type: "doc"): Promise<IDbDocSet>;
  private async getSet(name: string, type: string) {
    const set = await this.db.getSet(name, type as any) as any;
    if (!set) throw new ApiError(404, `set not found`);
    return set;
  }
}

class ApiError extends Error {
  constructor(readonly statusCode: number, msg: string) {
    super(msg);
  }
}

function decodeSetId(setid: string) {
  return setid.split(":", 2) as ["kv" | "doc", string];
}

function propNameToKeySelector(name: string): KeySelector<any> {
  return (0, eval)(`x => x[${JSON.stringify(name)}]`) as any;
}
