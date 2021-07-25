import { encodeValue, readValue } from "./binval.ts";
import { Buffer, encoder } from "./buffer.ts";
import { Runtime } from "./runtime.ts";

export interface ISerializable {
  writeTo(buf: Buffer): void;
  readonly byteLength: number;
}

export interface IComparable<T> {
  compareTo(other: T): -1 | 0 | 1;
}

export interface IValue extends ISerializable {}

export interface ValueType<T extends IValue> {
  readFrom(buf: Buffer): T;
}

export type KeyType<T extends IKey<any>> = ValueType<T>;

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
  compareTo(str: this) {
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

  [Runtime.customInspect]() {
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

  [Runtime.customInspect]() {
    return "UInt(" + this.val + ")";
  }
}

export class JSValue implements Key<JSValue> {
  private buf: Uint8Array | undefined;
  constructor(public readonly val: any, buf?: Uint8Array) {
    this.buf = buf;
  }

  get hash() {
    throw new Error("Not implemented.");
  }
  get key(): Key<any> {
    return this;
  }

  compareTo(other: this) {
    return this.val < other.val ? -1 : this.val > other.val ? 1 : 0;
  }

  get byteLength(): number {
    return this.ensureBuf().byteLength;
  }
  writeTo(buf: Buffer): void {
    this.ensureBuf();
    buf.writeBuffer(this.ensureBuf());
  }
  ensureBuf() {
    if (this.buf === undefined) {
      this.buf = encodeValue(this.val);
    }
    return this.buf;
  }

  static readFrom(buf: Buffer) {
    const begin = buf.pos;
    const val = readValue(buf);
    const end = buf.pos;
    return new JSValue(val, buf.buffer.slice(begin, end));
  }

  [Runtime.customInspect]() {
    return "JSON(" + Runtime.inspect(this.val) + ")";
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

  [Runtime.customInspect]() {
    return "KV(" + Runtime.inspect(this.key) + ", " +
      Runtime.inspect(this.value) +
      ")";
  }
}

export class DocumentValue extends JSValue implements IKey<any> {
  keyValue: JSValue;
  constructor(val: any, buf?: Uint8Array) {
    super(val, buf);
    this.keyValue = new JSValue(this.val.id);
  }

  get key() {
    return this.keyValue;
  }

  static readFrom(buf: Buffer) {
    const begin = buf.pos;
    const val = readValue(buf);
    const end = buf.pos;
    return new DocumentValue(val, buf.buffer.slice(begin, end));
  }

  [Runtime.customInspect]() {
    return "Doc(" + Runtime.inspect(this.val) + ")";
  }
}

export class KeyComparator<T extends IKey<any>> implements IComparable<T> {
  constructor(readonly key: KeyOf<T>) {
  }
  compareTo(other: T): 0 | 1 | -1 {
    return this.key.compareTo(other.key);
  }
}

export class KeyLeftmostComparator<T extends IKey<any>>
  implements IComparable<T> {
  constructor(readonly key: KeyOf<T>) {
  }
  compareTo(other: T): 0 | 1 | -1 {
    return this.key.compareTo(other.key) || -1; // always return -1 if the key equals
  }
}

export class KeyRightmostComparator<T extends IKey<any>>
  implements IComparable<T> {
  constructor(readonly key: KeyOf<T>) {
  }
  compareTo(other: T): 0 | 1 | -1 {
    return this.key.compareTo(other.key) || 1; // always return 1 if the key equals
  }
}

const pow16 = 2 ** 16;

export class PageOffsetValue implements IValue {
  constructor(readonly addr: number, readonly offset: number) {
  }
  writeTo(buf: Buffer): void {
    buf.writeU32(this.addr);
    buf.writeU16(this.offset);
  }
  get byteLength() {
    return 4 + 2;
  }
  compareTo(other: PageOffsetValue) {
    return (this.addr < other.addr)
      ? -1
      : (this.addr > other.addr)
      ? 1
      : (this.offset < other.offset)
      ? -1
      : (this.offset > other.offset)
      ? 1
      : 0;
  }
  static readFrom(buf: Buffer) {
    return new PageOffsetValue(buf.readU32(), buf.readU16());
  }
  encode() {
    return this.addr * pow16 + this.offset;
  }
  static fromEncoded(num: number) {
    const offset = num % pow16;
    return new PageOffsetValue((num - offset) / pow16, offset);
  }
  [Runtime.customInspect]() {
    return `Addr(${this.addr}, ${this.offset})`;
  }
}

export class RawBufferValue implements IValue {
  constructor(readonly buffer: Uint8Array) {}
  writeTo(buf: Buffer): void {
    buf.writeBuffer(this.buffer);
  }
  get byteLength() {
    return this.buffer.byteLength;
  }
}
