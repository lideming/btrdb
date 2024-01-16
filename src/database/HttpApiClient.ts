export interface HttpClientOptions {
  fetch?: typeof globalThis.fetch;
  baseUrl?: string;
  token?: string;
}

export class ClientDatabase {
  constructor(readonly httpClient: HttpClient) {}

  async createSet(name: string, type?: "kv"): Promise<ClientKvSet>;
  async createSet(name: string, type: "doc"): Promise<ClientDocSet>;
  async createSet(name: string, type: "kv" | "doc" = "kv") {
    await this.httpClient.request("POST", url`/sets/${type}:${name}`);
    return this.getSet(name, type as any) as any;
  }

  async createKvSet(name: string): Promise<ClientKvSet> {
    return await this.createSet(name, "kv");
  }

  async createDocSet(name: string): Promise<ClientDocSet> {
    return await this.createSet(name, "doc");
  }

  getSet(name: string, type?: "kv"): ClientKvSet;
  getSet(name: string, type: "doc"): ClientDocSet;
  getSet(name: string, type: "kv" | "doc" = "kv") {
    if (type === "kv" || type === undefined) {
      return this.getKvSet(name);
    } else if (type === "doc") {
      return this.getDocSet(name);
    } else {
      throw new Error("wrong type " + type);
    }
  }

  getKvSet(name: string) {
    return new ClientKvSet(this.httpClient, name);
  }

  getDocSet(name: string) {
    return new ClientDocSet(this.httpClient, name);
  }

  async deleteSet(name: string, type: "kv" | "doc"): Promise<boolean> {
    return await this.httpClient.request(
      "DELETE",
      url`/sets/${type}:${name}`,
    );
  }

  async deleteKvSet(name: string): Promise<boolean> {
    return await this.deleteSet(name, "kv");
  }

  async deleteDocSet(name: string): Promise<boolean> {
    return await this.deleteSet(name, "doc");
  }

  async getObjects(): Promise<
    {
      name: string;
      type: "kv" | "doc";
    }[]
  > {
    return await this.httpClient.request("GET", "/objects");
  }
  async deleteObject(name: string, type: "kv" | "doc"): Promise<boolean> {
    return await this.httpClient.request(
      "DELETE",
      url`/sets/${type}:${name}`,
    );
  }
}

export class ClientKvSet {
  constructor(readonly httpClient: HttpClient, readonly name: string) {}

  async exists(): Promise<boolean> {
    const { status } = await this.httpClient.requestReturnStatus(
      "HEAD",
      url`/sets/kv:${this.name}`,
    );
    return status == 200;
  }
  async get(key: SetKeyType): Promise<SetValueType | null> {
    return await this.httpClient.request(
      "GET",
      url`/sets/kv:${this.name}/${JSON.stringify(key)}`,
    );
  }
  async set(key: SetKeyType, value: SetValueType): Promise<boolean> {
    return await this.httpClient.request(
      "PUT",
      url`/sets/kv:${this.name}/${JSON.stringify(key)}`,
      value,
    );
  }
  async delete(key: SetKeyType): Promise<boolean> {
    const { status } = await this.httpClient.requestReturnStatus(
      "DELETE",
      url`/sets/kv:${this.name}/${JSON.stringify(key)}`,
    );
    return status == 200;
  }
  async getAll(): Promise<
    {
      key: SetKeyType;
      value: SetValueType;
    }[]
  > {
    return await this.httpClient.request("GET", url`/sets/kv:${this.name}`);
  }
  async getKeys(): Promise<SetKeyType[]> {
    return await this.httpClient.request(
      "GET",
      url`/sets/kv:${this.name}?keys`,
    );
  }
  async getCount(): Promise<number> {
    return await this.httpClient.request(
      "GET",
      url`/sets/kv:${this.name}?count`,
    );
  }
  async forEach(
    fn: (key: SetKeyType, value: SetValueType) => void | Promise<void>,
  ): Promise<void> {
    for (const { key, value } of await this.getAll()) {
      await fn(key, value);
    }
  }
}

export class ClientDocSet {
  constructor(readonly httpClient: HttpClient, readonly name: string) {}

  async exists(): Promise<boolean> {
    const { status } = await this.httpClient.requestReturnStatus(
      "HEAD",
      url`/sets/doc:${this.name}`,
    );
    return status == 200;
  }
  async getCount(): Promise<number> {
    return await this.httpClient.request(
      "GET",
      url`/sets/doc:${this.name}?count`,
    );
  }
  async get(id: any): Promise<any> {
    return await this.httpClient.request(
      "GET",
      url`/sets/doc:${this.name}/${JSON.stringify(id)}`,
    );
  }
  async insert(doc: any): Promise<any> {
    return await this.httpClient.request(
      "POST",
      url`/sets/doc:${this.name}/?insert`,
      doc,
    );
  }
  async upsert(doc: any): Promise<void> {
    return await this.httpClient.request(
      "PUT",
      url`/sets/doc:${this.name}/${JSON.stringify(doc.id)}`,
      doc,
    );
  }
  async delete(id: unknown): Promise<boolean> {
    const { status } = await this.httpClient.requestReturnStatus(
      "DELETE",
      url`/sets/doc:${this.name}/${JSON.stringify(id)}`,
    );
    return status == 200;
  }
  async getIds(): Promise<any[]> {
    return await this.httpClient.request(
      "GET",
      url`/sets/doc:${this.name}/?ids`,
    );
  }
  async useIndexes(indexDefs: IndexDef): Promise<void> {
    return await this.httpClient.request(
      "POST",
      url`/sets/doc:${this.name}/?indexes`,
      indexDefs,
    );
  }
  async findIndex(index: string, key: any): Promise<any[]> {
    return await this.httpClient.request(
      "POST",
      url`/sets/doc:${this.name}/?query`,
      {
        query: `${index} == {}`,
        values: [key],
      },
    );
  }
}

const defaultFetch = globalThis.fetch;

export class HttpClient {
  constructor(readonly options: HttpClientOptions) {}

  async request(
    method: "GET" | "POST" | "PUT" | "DELETE",
    url: string,
    body?: any,
  ) {
    const { status, body: resBody } = await this.requestReturnStatus(
      method,
      url,
      body,
    );
    if (status < 200 || status >= 300) {
      throw new HTTPError(status, resBody);
    }
    return resBody;
  }

  async requestReturnStatus(
    method: "GET" | "POST" | "PUT" | "DELETE" | "HEAD",
    url: string,
    body?: any,
  ) {
    const { fetch = defaultFetch, baseUrl = "", token } = this.options;
    const headers: any = {};
    if (body !== undefined) {
      body = JSON.stringify(body);
      headers["content-type"] = "application/json";
    }
    if (token) {
      headers["authorization"] = "Bearer " + token;
    }
    const resp = await fetch(baseUrl + url, { method, headers, body });
    let json = undefined;
    if (resp.headers.get("content-type")?.startsWith("application/json")) {
      json = await resp.json();
    }
    return {
      status: resp.status,
      body: json !== undefined ? json : await resp.text(),
    };
  }
}

class HTTPError extends Error {
  constructor(readonly httpStatus: number, readonly httpBody: any) {
    const bodyString = typeof httpBody == "object"
      ? JSON.stringify(httpBody)
      : httpBody;
    super(`HTTP status (${httpStatus}): ${bodyString}`);
  }
}

function url(strs: TemplateStringsArray, ...args: any[]) {
  const result = [];
  for (let i = 0; i < strs.length; i++) {
    result.push(strs[i]);
    if (i < args.length) {
      result.push(encodeURIComponent(args[i]));
    }
  }
  return result.join("");
}

type SetKeyType = string | number | SetKeyType[];
type SetValueType = string | number | any[] | object;
type IndexDef = Record<
  string,
  | string
  | {
    key: string;
    unique?: boolean;
  }
>;