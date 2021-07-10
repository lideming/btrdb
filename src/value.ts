import { Buffer, encoder } from "./buffer.ts";

export interface ISerializable {
  writeTo(buf: Buffer): void;
  readonly byteLength: number;
}

export interface IComparable<T> {
  compareTo(other: T): -1 | 0 | 1;
}

export interface IValue extends ISerializable {}

export interface KeyType<T extends IKey<any>> {
  readFrom(buf: Buffer): T;
}

export interface Key<T> extends IValue, IComparable<T>, IKey<T> {
  readonly hash: any;
}

export interface IKey<T> extends IValue {
  readonly key: Key<T>;
}

export type KeyOf<T> = T extends IKey<infer K> ? Key<K> : never;

export type ValueReader<T> = (buf: Buffer) => T;

export class StringValue implements Key<StringValue> {
  constructor(
    public readonly str: string,
  ) {
  }
  private buf: Uint8Array | undefined = undefined;
  get hash() {
    return this.str;
  }
  get key(): Key<any> {
    return this;
  }
  get byteLength(): number {
    this.ensureBuf();
    return Buffer.calcLenEncodedBufferSize(this.buf!);
  }
  writeTo(buf: Buffer): void {
    this.ensureBuf();
    buf.writeLenEncodedBuffer(this.buf!);
  }
  compareTo(str: StringValue) {
    return (this.str < str.str) ? -1 : (this.str === str.str) ? 0 : 1;
  }
  ensureBuf() {
    if (this.buf === undefined) {
      this.buf = encoder.encode(this.str);
    }
  }

  static readFrom(buf: Buffer) {
    return new StringValue(buf.readString());
  }

  [Symbol.for("Deno.customInspect")]() {
    return "Str(" + this.str + ")";
  }
}

export class UIntValue implements IKey<UIntValue> {
  constructor(
    public readonly val: number,
  ) {
  }
  get hash() {
    return this.val;
  }
  get key() {
    return this;
  }
  get byteLength() {
    return 4;
  }
  writeTo(buf: Buffer): void {
    buf.writeU32(this.val);
  }
  compareTo(other: UIntValue): 0 | 1 | -1 {
    return (this.val < other.val) ? -1 : (this.val === other.val) ? 0 : 1;
  }

  static readFrom(buf: Buffer) {
    return new UIntValue(buf.readU32());
  }

  [Symbol.for("Deno.customInspect")]() {
    return "UInt(" + this.val + ")";
  }
}

export class JSONValue extends StringValue {
  constructor(public readonly val: any, stringified?: string) {
    super(stringified ?? JSON.stringify(val));
  }

  static readFrom(buf: Buffer) {
    const str = buf.readString();
    return new JSONValue(JSON.parse(str), str);
  }

  [Symbol.for("Deno.customInspect")]() {
    return "JSON(" + Deno.inspect(this.val) + ")";
  }
}

export class KValue<K extends Key<K>, V extends IValue>
  implements IKey<K>, IComparable<KValue<K, V>> {
  constructor(
    public readonly key: K,
    public readonly value: V,
  ) {
  }
  writeTo(buf: Buffer): void {
    this.key.writeTo(buf);
    this.value.writeTo(buf);
  }
  get byteLength() {
    return this.key.byteLength + this.value.byteLength;
  }

  static readFrom<K extends Key<K>, V extends IValue>(
    buf: Buffer,
    readKey: ValueReader<K>,
    readValue: ValueReader<V>,
  ) {
    return new KValue<K, V>(readKey(buf), readValue(buf));
  }

  compareTo(other: this) {
    return this.key.compareTo(other.key) ||
      (this.value as any).compareTo(other.value);
  }

  [Deno.customInspect]() {
    return "KV(" + Deno.inspect(this.key) + ", " + Deno.inspect(this.value) +
      ")";
  }
}

export class DocumentValue extends JSONValue implements IKey<any> {
  keyValue: JSONValue;
  constructor(val: any, stringified?: string) {
    super(val, stringified);
    this.keyValue = new JSONValue(this.val.id);
  }

  get key() {
    return this.keyValue;
  }

  static readFrom(buf: Buffer) {
    const str = buf.readString();
    return new DocumentValue(JSON.parse(str), str);
  }

  [Symbol.for("Deno.customInspect")]() {
    return "Doc(" + Deno.inspect(this.val) + ")";
  }
}

export class KeyComparator<T extends IKey<any>> implements IComparable<T> {
  constructor(readonly key: KeyOf<T>) {
  }
  compareTo(other: T): 0 | 1 | -1 {
    // console.info("KeyCompare", this.key, other, other.key);
    return this.key.compareTo(other.key);
  }
}
