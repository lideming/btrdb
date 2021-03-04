import { Buffer, encoder } from "./buffer.ts";


export interface ISerializable {
    writeTo(buf: Buffer): void;
    readonly byteLength: number;
}

export interface IComparable<T> {
    compareTo(other: T): -1 | 0 | 1;
}

export interface IValue extends ISerializable { }

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
        public readonly str: string) {
    }
    private buf: Uint8Array | undefined = undefined;
    get hash() { return this.str; }
    get key() { return this; }
    get byteLength(): number {
        this.ensureBuf();
        return Buffer.calcLenEncodedBufferSize(this.buf!);
    }
    writeTo(buf: Buffer): void {
        this.ensureBuf();
        buf.writeLenEncodedBuffer(this.buf!);
    }
    compareTo(str: StringValue) {
        return (this.str < str.str) ? -1 :
            (this.str === str.str) ? 0 : 1;
    }
    ensureBuf() {
        if (this.buf === undefined) {
            this.buf = encoder.encode(this.str);
        }
    }

    static readFrom(buf: Buffer) {
        return new StringValue(buf.readString());
    }
}

export class UIntValue implements IKey<UIntValue> {
    constructor(
        public readonly val: number
    ) {
    }
    get hash() { return this.val; }
    get key() { return this; }
    get byteLength() { return 4; }
    writeTo(buf: Buffer): void {
        buf.writeU32(this.val);
    }
    compareTo(other: UIntValue): 0 | 1 | -1 {
        return (this.val < other.val) ? -1 :
            (this.val === other.val) ? 0 : 1;
    }

    static readFrom(buf: Buffer) {
        return new UIntValue(buf.readU32());
    }
}

export class KValue<K extends Key<K>, V extends IValue> implements IKey<K> {
    constructor(
        public readonly key: K,
        public readonly value: V
    ) {
    }
    writeTo(buf: Buffer): void {
        this.key.writeTo(buf);
        this.value.writeTo(buf);
    }
    get byteLength() { return this.key.byteLength + this.value.byteLength; }

    static readFrom<K extends Key<K>, V extends IValue>(
        buf: Buffer,
        readKey: ValueReader<K>,
        readValue: ValueReader<V>
    ) {
        return new KValue<K, V>(readKey(buf), readValue(buf));
    }

    [Deno.customInspect]() {
        return Deno.inspect([this.key, this.value]);
    }
}
