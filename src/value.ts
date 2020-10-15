import { Buffer, encoder } from "./buffer.ts";


export interface ISerializable {
    writeTo(buf: Buffer): void;
    readonly byteLength: number;
}

export interface IComparable<T> {
    compareTo(other: T): -1 | 0 | 1;
}

export interface IValue<T> extends ISerializable, IComparable<T> {
    readonly hash: any;
}

export class StringValue implements IValue<StringValue> {
    readonly str: string;
    private buf: Uint8Array | undefined = undefined;
    get hash() { return this.str; };
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
            (str.str === str.str) ? 0 : 1;
    }
    ensureBuf() {
        if (this.buf === undefined) {
            this.buf = encoder.encode(this.str);
        }
    }
    constructor(str: string) {
        this.str = str;
    }
}
