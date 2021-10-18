export { Database } from "./src/database.ts";
export { numberIdGenerator } from "./src/DbDocSet.ts";
export { AND, BETWEEN, EQ, GE, GT, LE, LT, NE, NOT, OR } from "./src/query.ts";
export { query } from "./src/queryString.ts";
export type {
  DbObjectType,
  DbSetType,
  IDbDocSet,
  IDbSet,
  IDocument,
  IdType,
  IndexDef,
  KeySelector,
  NoId,
  OptionalId,
  Query,
  SetKeyType,
  SetValueType,
} from "./src/btrdb.d.ts";
