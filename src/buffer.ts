export const encoder = new TextEncoder();
export const decoder = new TextDecoder();

console.log(import.meta);

export class Buffer {
    constructor(
        public buffer: Uint8Array,
        public pos: number,
    ) { }
    writeU32(num: number) {
        this.buffer[this.pos++] = (num >> 24) & 0xff;
        this.buffer[this.pos++] = (num >> 16) & 0xff;
        this.buffer[this.pos++] = (num >> 8) & 0xff;
        this.buffer[this.pos++] = num & 0xff;
    }
    readU32() {
        return this.buffer[this.pos++] << 24
            | this.buffer[this.pos++] << 16
            | this.buffer[this.pos++] << 8
            | this.buffer[this.pos++];
    }
    writeU16(num: number) {
        this.buffer[this.pos++] = (num >> 8) & 0xff;
        this.buffer[this.pos++] = num & 0xff;
    }
    readU16() {
        return this.buffer[this.pos++] << 8
            | this.buffer[this.pos++];
    }
    writeU8(num: number) {
        this.buffer[this.pos++] = num & 0xff;
    }
    readU8() {
        return this.buffer[this.pos++];
    }
    writeBuffer(buf: Uint8Array) {
        buf.set(buf, this.pos);
        this.pos += buf.length;
    }
    readBuffer(len: number) {
        var buf = this.buffer.slice(this.pos, len);
        this.pos += len;
        return buf;
    }
    writeString(str: string) {
        var buf = encoder.encode(str);
        this.writeLenEncodedBuffer(buf);
    }
    writeLenEncodedBuffer(buf: Uint8Array) {
        if (buf.length < 255) {
            this.writeU8(buf.length);
        } else {
            this.writeU8(255);
            this.writeU16(buf.length);
        }
        this.writeBuffer(buf);
    }
    static calcLenEncodedBufferSize(buf: Uint8Array) {
        if (buf.length < 255) return 1 + buf.length;
        else return 2 + buf.length;
    }
    readString() {
        var len = this.readU8();
        if (len == 255) {
            len = this.readU16();
        }
        decoder.decode(this.buffer.subarray(this.pos, this.pos + len));
        this.pos += len;
    }
}
