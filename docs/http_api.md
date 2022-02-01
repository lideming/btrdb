# HTTP API for btrdb

## Database objects (sets)

### `setid`

The `setid` is a string as `` `${type}:${name}` ``

### Create a set

`POST /sets/:setid`

### Delete a set

`DELETE /sets/:setid`

### List objects

`GET /objects`

Response body: (json)

```ts
type ListObjectsResponse = { name: string; type: DbObjectType }[];
```

## Key-value Sets

### Set a key-value pair

`PUT /sets/:setid/:key`

Note: the `key` is string or number in JSON format (e.g. `/sets/kv:test/"foo"`
or `/sets/kv:test/123`).

Request body: (json) the value

### Get a value by key

`GET /sets/:setid/:key`

Response body: (json) the value

### Delete a key

`DELETE /sets/:setid/:key`

Returns status code 404 if the key does not exist.

### List keys

`GET /sets/:setid/?keys`

Response body: (json)

```ts
type ListKeysResponse = string[];
```

### List key-value pairs

`GET /sets/:setid`

Response body: (json)

```ts
type ListKeysResponse = { key: SetKeyType; value: SetValueType }[];
```

### Get count of pairs

`GET /sets/:setid?count`

Response body: (json) the count

## Document sets

### Insert a document

`POST /sets/:setid/?insert`

Request body: (json) the document

Response body: (json) the id of the inserted document

### Upsert a document

`PUT /sets/:setid/:id`

Note: the `id` is string or number in JSON format (e.g. `/sets/doc:test/"foo"`
or `/sets/doc:test/123`).

Request body: (json) the document

### Get a document by id

`GET /sets/:setid/:id`

### Delete a document by id

`DELETE /sets/:setid/:id`

Returns status code 404 if the id is not found.

### Get documents count

`GET /sets/:setid/?count`

Response body: (json) the count

### List document ids

`GET /sets/:setid/?ids`

Response body: (json) array of document ids

### Set indexes

`POST /sets/:setid/?indexes`

Request body: (json) the index definitions (`IndexDef<T>`, but the `key` is a
name of property, functions are not supported)

### Query documents

`POST /sets/:setid/?query`

Request body: (json)

```ts
type QueryRequest = {
  /** The literal parts of the query, with value placeholders `{}` */
  query: string;
  /** The values of the query */
  values: any[];
};
const example = {
  query: "age >= {} AND gender == {}",
  values: [18, "male"],
};
```

Response body: (json) array of documents

The same API with `GET`:

`GET /sets/:setid/?query=:query&value=:value_0&value=:value_1`
