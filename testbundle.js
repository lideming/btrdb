// deno-fmt-ignore-file
// deno-lint-ignore-file
// This code was bundled using `deno bundle` and it's not recommended to edit it manually

const _Deno = globalThis["Deno"];
let Runtime = !globalThis["Deno"] ? null : {
    mkdir: _Deno.mkdir,
    remove: _Deno.remove,
    rename: _Deno.rename,
    writeTextFile: _Deno.writeTextFile,
    readTextFile: _Deno.readTextFile,
    test: _Deno.test,
    open: _Deno.open,
    inspect: _Deno.inspect,
    fdatasync: _Deno.fdatasync,
    customInspect: Symbol.for("Deno.customInspect"),
    env: _Deno.env,
    SeekMode: _Deno.SeekMode,
    File: _Deno.File
};
if (!Runtime) {
    Runtime = {
        customInspect: Symbol.for("Deno.customInspect")
    };
}
function setRuntimeImplementaion(runtime) {
    Runtime = runtime;
}
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const tmpbuf = new ArrayBuffer(8);
const f64arr = new Float64Array(tmpbuf);
const u8arr = new Uint8Array(tmpbuf);
class Buffer {
    buffer;
    pos;
    constructor(buffer, pos){
        this.buffer = buffer;
        this.pos = pos;
    }
    writeF64(num) {
        this.beforeWriting(8);
        f64arr[0] = num;
        this.writeBuffer(u8arr);
    }
    readF64() {
        for(let i8 = 0; i8 < 8; i8++){
            u8arr[i8] = this.buffer[this.pos + i8];
        }
        this.pos += 8;
        return f64arr[0];
    }
    writeU32(num) {
        this.beforeWriting(4);
        this.buffer[this.pos++] = num >>> 24 & 255;
        this.buffer[this.pos++] = num >>> 16 & 255;
        this.buffer[this.pos++] = num >>> 8 & 255;
        this.buffer[this.pos++] = num & 255;
    }
    readU32() {
        return (this.buffer[this.pos++] << 24 | this.buffer[this.pos++] << 16 | this.buffer[this.pos++] << 8 | this.buffer[this.pos++]) >>> 0;
    }
    writeU16(num) {
        this.beforeWriting(2);
        this.buffer[this.pos++] = num >> 8 & 255;
        this.buffer[this.pos++] = num & 255;
    }
    readU16() {
        return this.buffer[this.pos++] << 8 | this.buffer[this.pos++];
    }
    writeU8(num) {
        this.beforeWriting(1);
        this.buffer[this.pos++] = num & 255;
    }
    readU8() {
        return this.buffer[this.pos++];
    }
    writeBuffer(buf) {
        this.beforeWriting(buf.byteLength);
        try {
            this.buffer.set(buf, this.pos);
        } catch  {
            debugger;
        }
        this.pos += buf.byteLength;
    }
    readBuffer(len) {
        var buf = this.buffer.slice(this.pos, this.pos + len);
        this.pos += len;
        return buf;
    }
    readBufferReadonly(len) {
        var buf = this.buffer.subarray(this.pos, this.pos + len);
        this.pos += len;
        return buf;
    }
    writeString(str) {
        const len = Buffer.calcStringSize(str);
        this.writeEncodedUint(len);
        this.beforeWriting(len);
        const r = encoder.encodeInto(str, this.buffer.subarray(this.pos));
        this.pos += r.written;
    }
    writeLenEncodedBuffer(buf) {
        this.writeEncodedUint(buf.length);
        this.writeBuffer(buf);
    }
    writeEncodedUint(val) {
        if (val < 254) {
            this.writeU8(val);
        } else if (val < 65536) {
            this.writeU8(254);
            this.writeU16(val);
        } else {
            this.writeU8(255);
            this.writeU32(val);
        }
    }
    readEncodedUint() {
        var val = this.readU8();
        if (val < 254) {
            return val;
        } else if (val == 254) {
            return this.readU16();
        } else {
            return this.readU32();
        }
    }
    readString() {
        const len = this.readEncodedUint();
        const str = decoder.decode(this.buffer.subarray(this.pos, this.pos + len));
        this.pos += len;
        return str;
    }
    static calcStringSize(str) {
        if (nativeStringSize !== undefined) return nativeStringSize(str);
        let bytes = 0;
        const len = str.length;
        for(let i9 = 0; i9 < len; i9++){
            const codePoint = str.charCodeAt(i9);
            if (codePoint < 128) {
                bytes += 1;
            } else if (codePoint < 2048) {
                bytes += 2;
            } else if (codePoint >= 55296 && codePoint < 57344) {
                if (codePoint < 56320 && i9 + 1 < len) {
                    const next = str.charCodeAt(i9 + 1);
                    if (next >= 56320 && next < 57344) {
                        bytes += 4;
                        i9++;
                    } else {
                        bytes += 3;
                    }
                } else {
                    bytes += 3;
                }
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }
    static calcLenEncodedStringSize(str) {
        const len = Buffer.calcStringSize(str);
        return Buffer.calcEncodedUintSize(len) + len;
    }
    static calcLenEncodedBufferSize(buf) {
        return Buffer.calcEncodedUintSize(buf.length) + buf.length;
    }
    static calcEncodedUintSize(len) {
        return len < 254 ? 1 : len < 65536 ? 3 : 5;
    }
    beforeWriting(size) {}
}
const _globalThis = globalThis;
const nativeStringSize = _globalThis?.Buffer?.byteLength ?? undefined;
class DynamicBuffer extends Buffer {
    constructor(initSize = 32){
        super(new Uint8Array(initSize), 0);
    }
    beforeWriting(size) {
        const minsize = this.pos + size;
        if (minsize > this.buffer.byteLength) {
            let newsize = this.buffer.byteLength * 4;
            while(minsize > newsize){
                newsize *= 4;
            }
            const newBuffer = new Uint8Array(newsize);
            newBuffer.set(this.buffer);
            this.buffer = newBuffer;
        }
    }
}
class AlreadyExistError extends Error {
}
class NotExistError extends Error {
}
class BugError extends Error {
}
const resolved = Promise.resolve();
function deferred() {
    let resolve, reject;
    var prom = new Promise((r, rej)=>{
        resolve = r, reject = rej;
    });
    prom.resolve = resolve;
    prom.reject = reject;
    return prom;
}
class OneWriterLock {
    readers = 0;
    writers = 0;
    pendingReaders = 0;
    wakeAllReaders = null;
    wakeWriters = [];
    _prefer = false;
    enterReader() {
        if (!this.writers) {
            this.readers++;
            return resolved;
        } else {
            if (!this.wakeAllReaders) this.wakeAllReaders = deferred();
            this.pendingReaders++;
            return this.wakeAllReaders;
        }
    }
    exitReader() {
        if (this.writers != 0 || this.readers <= 0) throw new Error("BUG");
        this.readers--;
        if (this.wakeWriters.length && this.readers == 0 && this.writers == 0) {
            this.wakeWriters.pop().resolve();
            this.writers++;
        }
    }
    enterWriterFromReader() {
        if (this.writers != 0 || this.readers <= 0) throw new Error("BUG");
        this.exitReader();
        return this.enterWriter(true);
    }
    enterWriter(asap = false) {
        if (!this.writers && !this.readers) {
            this.writers++;
            return resolved;
        } else {
            const wait = deferred();
            if (asap) this.wakeWriters.unshift(wait);
            else this.wakeWriters.push(wait);
            return wait;
        }
    }
    exitWriter() {
        if (this.writers != 1 || this.readers != 0) {
            throw new Error("BUG, " + this.writers + ", " + this.readers);
        }
        this.writers--;
        this._prefer = !this._prefer;
        if (this._prefer) {
            if (this.wakeAllReaders) {
                this.wakeAllReaders.resolve();
                this.wakeAllReaders = null;
                this.readers = this.pendingReaders;
                this.pendingReaders = 0;
            } else if (this.wakeWriters.length) {
                this.wakeWriters.pop().resolve();
                this.writers++;
            }
        } else {
            if (this.wakeWriters.length) {
                this.wakeWriters.pop().resolve();
                this.writers++;
            } else if (this.wakeAllReaders) {
                this.wakeAllReaders.resolve();
                this.wakeAllReaders = null;
                this.readers = this.pendingReaders;
                this.pendingReaders = 0;
            }
        }
    }
}
class TaskQueue {
    tasks = [];
    running = null;
    enqueue(task) {
        this.tasks.push(task);
        if (this.tasks.length == 1) {
            this._run();
        }
    }
    waitCurrentLastTask() {
        if (!this.tasks.length) return Promise.resolve();
        const toWait = this.tasks[this.tasks.length - 1];
        return this.waitTask(toWait);
    }
    async waitTask(toWait) {
        do {
            await this.running;
        }while (this.tasks.indexOf(toWait) > 0)
        return await this.running;
    }
    async _run() {
        while(this.tasks.length){
            this.running = this.tasks[0].run();
            await this.running;
            this.tasks.shift();
        }
        this.running = null;
    }
}
function encodeValue(val) {
    const buf = new DynamicBuffer();
    writeValue(val, buf);
    return buf.buffer.subarray(0, buf.pos);
}
function writeValue(obj, buf) {
    if (obj === null) {
        buf.writeU8(Type.Null);
    } else if (obj === undefined) {
        buf.writeU8(Type.Undefined);
    } else if (obj === false) {
        buf.writeU8(Type.False);
    } else if (obj === true) {
        buf.writeU8(Type.True);
    } else if (typeof obj === "number") {
        if (Number.isInteger(obj)) {
            if (obj >= -7 && obj <= 127) {
                if (obj > 0) {
                    buf.writeU8(Type.Number0 + obj);
                } else if (obj < 0) {
                    buf.writeU8(Type.NumberNeg0 + obj);
                } else {
                    if (Object.is(obj, -0)) {
                        buf.writeU8(Type.NumberNeg0);
                    } else {
                        buf.writeU8(Type.Number0);
                    }
                }
            } else {
                let negative = 0;
                if (obj < 0) {
                    obj = -obj;
                    negative = 3;
                }
                if (obj < 256) {
                    buf.writeU8(Type.Uint8 + negative);
                    buf.writeU8(obj);
                } else if (obj < 65536) {
                    buf.writeU8(Type.Uint16 + negative);
                    buf.writeU16(obj);
                } else if (obj < 2 ** 32) {
                    buf.writeU8(Type.Uint32 + negative);
                    buf.writeU32(obj);
                } else {
                    buf.writeU8(Type.Float64);
                    buf.writeF64(negative ? -obj : obj);
                }
            }
        } else {
            buf.writeU8(Type.Float64);
            buf.writeF64(obj);
        }
    } else if (typeof obj === "string") {
        const len = Buffer.calcStringSize(obj);
        if (len <= 32) {
            buf.writeU8(Type.String0 + len);
        } else {
            buf.writeU8(Type.String);
            buf.writeEncodedUint(len);
        }
        buf.beforeWriting(len);
        const r = encoder.encodeInto(obj, buf.buffer.subarray(buf.pos));
        if (r.written !== len) {
            throw new Error("Expect " + r.written + " == " + len);
        }
        buf.pos += len;
    } else if (typeof obj === "object") {
        if (obj instanceof Array) {
            if (obj.length <= 8) {
                buf.writeU8(Type.Array0 + obj.length);
            } else {
                buf.writeU8(Type.Array);
                buf.writeEncodedUint(obj.length);
            }
            for (const val of obj){
                writeValue(val, buf);
            }
        } else if (obj instanceof Uint8Array) {
            if (obj.length <= 32) {
                buf.writeU8(Type.Binary0 + obj.byteLength);
            } else {
                buf.writeU8(Type.Binary);
                buf.writeEncodedUint(obj.byteLength);
            }
            buf.writeBuffer(obj);
        } else {
            const keys1 = Object.keys(obj);
            if (keys1.length <= 8) {
                buf.writeU8(Type.Object0 + keys1.length);
            } else {
                buf.writeU8(Type.Object);
                buf.writeEncodedUint(keys1.length);
            }
            for (const key of keys1){
                buf.writeString(key);
                writeValue(obj[key], buf);
            }
        }
    } else {
        throw new Error("Unsupported value " + obj);
    }
}
function calcEncodedLength(obj) {
    if (obj === null) {
        return 1;
    } else if (obj === undefined) {
        return 1;
    } else if (obj === false) {
        return 1;
    } else if (obj === true) {
        return 1;
    } else if (typeof obj === "number") {
        if (Number.isInteger(obj)) {
            if (obj >= -7 && obj <= 127) {
                return 1;
            } else {
                if (obj < 0) obj = -obj;
                if (obj < 256) {
                    return 2;
                } else if (obj < 65536) {
                    return 3;
                } else if (obj < 2 ** 32) {
                    return 5;
                } else {
                    return 9;
                }
            }
        } else {
            return 9;
        }
    } else if (typeof obj === "string") {
        const len = Buffer.calcStringSize(obj);
        if (len <= 32) {
            return 1 + len;
        } else {
            return 1 + Buffer.calcEncodedUintSize(len) + len;
        }
    } else if (typeof obj === "object") {
        if (obj instanceof Array) {
            let len = 0;
            if (obj.length <= 8) {
                len += 1;
            } else {
                len += 1 + Buffer.calcEncodedUintSize(obj.length);
            }
            for (const val of obj){
                len += calcEncodedLength(val);
            }
            return len;
        } else if (obj instanceof Uint8Array) {
            let len = 0;
            if (obj.length <= 32) {
                len += 1;
            } else {
                len += 1 + Buffer.calcEncodedUintSize(obj.byteLength);
            }
            len += obj.byteLength;
            return len;
        } else {
            let len = 0;
            const keys2 = Object.keys(obj);
            if (keys2.length <= 8) {
                len += 1;
            } else {
                len += 1 + Buffer.calcEncodedUintSize(keys2.length);
            }
            for (const key of keys2){
                len += Buffer.calcLenEncodedStringSize(key);
                len += calcEncodedLength(obj[key]);
            }
            return len;
        }
    } else {
        throw new Error("Unsupported value " + obj);
    }
}
function readValue(buf) {
    const type = buf.readU8();
    return decodeMap[type](buf, type);
}
const decodeMap = [];
decodeMap.push(()=>null
);
decodeMap.push(()=>undefined
);
decodeMap.push(()=>false
);
decodeMap.push(()=>true
);
decodeMap.push((buf)=>buf.readU8()
);
decodeMap.push((buf)=>buf.readU16()
);
decodeMap.push((buf)=>buf.readU32()
);
decodeMap.push((buf)=>-buf.readU8()
);
decodeMap.push((buf)=>-buf.readU16()
);
decodeMap.push((buf)=>-buf.readU32()
);
decodeMap.push((buf)=>buf.readF64()
);
decodeMap.push((buf)=>buf.readString()
);
decodeMap.push((buf)=>{
    const len = buf.readEncodedUint();
    return buf.readBuffer(len);
});
decodeMap.push((buf)=>decodeObject(buf, buf.readEncodedUint())
);
decodeMap.push((buf)=>decodeArray(buf, buf.readEncodedUint())
);
for(let i = 15; i <= 35; i++){
    decodeMap.push(decodeUnused);
}
for(let i1 = 0; i1 <= 8; i1++){
    decodeMap.push(decodeSmallArray);
}
for(let i2 = 0; i2 <= 8; i2++){
    decodeMap.push(decodeSmallObject);
}
for(let i3 = 0; i3 <= 32; i3++){
    decodeMap.push(decodeSmallBlob);
}
decodeMap.push(()=>""
);
for(let i4 = 1; i4 <= 32; i4++){
    decodeMap.push(decodeSmallString);
}
for(let i5 = 120; i5 <= 126; i5++){
    decodeMap.push(decodeSmallNegativeNumber);
}
decodeMap.push(()=>-0
);
for(let i6 = 128; i6 <= 255; i6++){
    decodeMap.push(decodeSmallPositiveNumber);
}
function decodeSmallArray(buf, type) {
    return decodeArray(buf, type - Type.Array0);
}
function decodeSmallObject(buf, type) {
    return decodeObject(buf, type - Type.Object0);
}
function decodeSmallBlob(buf, type) {
    return buf.readBuffer(type - Type.Binary0);
}
function decodeSmallString(buf, type) {
    const buffer = buf.readBufferReadonly(type - Type.String0);
    return decoder.decode(buffer);
}
function decodeSmallNegativeNumber(buf, type) {
    return type - Type.NumberNeg0;
}
function decodeSmallPositiveNumber(buf, type) {
    return type - Type.Number0;
}
function decodeUnused(buf, type) {
    throw new Error("Unsupported value type " + type);
}
function BinvalObject() {}
BinvalObject.prototype = Object.create(null);
function decodeObject(buf, propCount) {
    let obj = new BinvalObject();
    for(let i7 = 0; i7 < propCount; i7++){
        const key = buf.readString();
        obj[key] = readValue(buf);
    }
    return obj;
}
function decodeArray(buf, itemCount) {
    let arr = [];
    for(let i8 = 0; i8 < itemCount; i8++){
        arr.push(readValue(buf));
    }
    return arr;
}
var Type;
(function(Type1) {
    Type1[Type1["Null"] = 0] = "Null";
    Type1[Type1["Undefined"] = 1] = "Undefined";
    Type1[Type1["False"] = 2] = "False";
    Type1[Type1["True"] = 3] = "True";
    Type1[Type1["Uint8"] = 4] = "Uint8";
    Type1[Type1["Uint16"] = 5] = "Uint16";
    Type1[Type1["Uint32"] = 6] = "Uint32";
    Type1[Type1["NegUint8"] = 7] = "NegUint8";
    Type1[Type1["NegUint16"] = 8] = "NegUint16";
    Type1[Type1["NegUint32"] = 9] = "NegUint32";
    Type1[Type1["Float64"] = 10] = "Float64";
    Type1[Type1["String"] = 11] = "String";
    Type1[Type1["Binary"] = 12] = "Binary";
    Type1[Type1["Object"] = 13] = "Object";
    Type1[Type1["Array"] = 14] = "Array";
    Type1[Type1["Array0"] = 36] = "Array0";
    Type1[Type1["Object0"] = 45] = "Object0";
    Type1[Type1["Binary0"] = 54] = "Binary0";
    Type1[Type1["String0"] = 87] = "String0";
    Type1[Type1["NumberNeg0"] = 127] = "NumberNeg0";
    Type1[Type1["Number0"] = 128] = "Number0";
})(Type || (Type = {}));
class StringValue {
    str;
    constructor(str){
        this.str = str;
    }
    _totalLen = undefined;
    _strLen = undefined;
    get hash() {
        return this.str;
    }
    get key() {
        return this;
    }
    get byteLength() {
        if (this._totalLen === undefined) {
            this._strLen = Buffer.calcStringSize(this.str);
            this._totalLen = Buffer.calcEncodedUintSize(this._strLen) + this._strLen;
        }
        return this._totalLen;
    }
    writeTo(buf) {
        this.byteLength;
        buf.writeEncodedUint(this._strLen);
        buf.beforeWriting(this._strLen);
        encoder.encodeInto(this.str, buf.buffer.subarray(buf.pos));
        buf.pos += this._strLen;
    }
    compareTo(str) {
        return this.str < str.str ? -1 : this.str === str.str ? 0 : 1;
    }
    static readFrom(buf) {
        return new StringValue(buf.readString());
    }
    [Runtime.customInspect]() {
        return "Str(" + this.str + ")";
    }
}
class UIntValue {
    val;
    constructor(val){
        this.val = val;
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
    writeTo(buf) {
        buf.writeU32(this.val);
    }
    compareTo(other) {
        return this.val < other.val ? -1 : this.val === other.val ? 0 : 1;
    }
    static readFrom(buf) {
        return new UIntValue(buf.readU32());
    }
    [Runtime.customInspect]() {
        return "UInt(" + this.val + ")";
    }
}
class JSValue {
    val;
    _byteLength;
    constructor(val, buf){
        this.val = val;
        this._byteLength = buf?.byteLength;
    }
    get hash() {
        throw new Error("Not implemented.");
    }
    get key() {
        return this;
    }
    compareTo(other) {
        var left = this.val;
        var right = other.val;
        return left < right ? -1 : left > right ? 1 : left === right ? 0 : compareObject(left, right);
    }
    get byteLength() {
        return this._byteLength ??= calcEncodedLength(this.val);
    }
    writeTo(buf) {
        writeValue(this.val, buf);
    }
    getBuf() {
        return encodeValue(this.val);
    }
    static readFrom(buf) {
        const begin = buf.pos;
        const val = readValue(buf);
        const end = buf.pos;
        return new JSValue(val, buf.buffer.slice(begin, end));
    }
    [Runtime.customInspect]() {
        return "JSVal(" + Runtime.inspect(this.val) + ")";
    }
}
function compareJSValue(a, b) {
    var left = a.val;
    var right = b.val;
    return left < right ? -1 : left > right ? 1 : left === right ? 0 : compareObject(left, right);
}
function compareValueOrObject(left, right) {
    return left < right ? -1 : left > right ? 1 : left === right ? 0 : compareObject(left, right);
}
function compareValue(a, b) {
    return a < b ? -1 : a > b ? 1 : 0;
}
function compareObject(a, b) {
    if (typeof a !== typeof b) {
        return compareValue(typeof a, typeof b);
    }
    if (a instanceof Array) {
        for(let i10 = 0; i10 < a.length; i10++){
            const r = compareValueOrObject(a[i10], b[i10]);
            if (r !== 0) return r;
        }
        if (a.length === b.length) return 0;
        else if (a.length > b.length) return 1;
        else return -1;
    }
    throw new Error(`Cannot compare between ${Runtime.inspect(a)} and ${Runtime.inspect(b)}`);
}
class KValue {
    key;
    value;
    constructor(key, value){
        this.key = key;
        this.value = value;
    }
    writeTo(buf) {
        this.key.writeTo(buf);
        this.value.writeTo(buf);
    }
    get byteLength() {
        return this.key.byteLength + this.value.byteLength;
    }
    static readFrom(buf, readKey, readValue1) {
        return new KValue(readKey(buf), readValue1(buf));
    }
    compareTo(other) {
        return this.key.compareTo(other.key) || this.value.compareTo(other.value);
    }
    [Runtime.customInspect]() {
        return "KV(" + Runtime.inspect(this.key) + ", " + Runtime.inspect(this.value) + ")";
    }
}
class DocumentValue extends JSValue {
    keyValue;
    constructor(val, buf){
        super(val, buf);
        this.keyValue = new JSValue(this.val.id);
    }
    get key() {
        return this.keyValue;
    }
    static readFrom(buf) {
        const begin = buf.pos;
        const val = readValue(buf);
        const end = buf.pos;
        return new DocumentValue(val, buf.buffer.slice(begin, end));
    }
    [Runtime.customInspect]() {
        return "Doc(" + Runtime.inspect(this.val) + ")";
    }
}
class KeyComparator {
    key;
    constructor(key){
        this.key = key;
    }
    compareTo(other) {
        return this.key.compareTo(other.key);
    }
}
class KeyLeftmostComparator {
    key;
    constructor(key){
        this.key = key;
    }
    compareTo(other) {
        return this.key.compareTo(other.key) || -1;
    }
}
class KeyRightmostComparator {
    key;
    constructor(key){
        this.key = key;
    }
    compareTo(other) {
        return this.key.compareTo(other.key) || 1;
    }
}
const pow16 = 2 ** 16;
class PageOffsetValue {
    addr;
    offset;
    constructor(addr, offset){
        this.addr = addr;
        this.offset = offset;
    }
    writeTo(buf) {
        buf.writeU32(this.addr);
        buf.writeU16(this.offset);
    }
    get byteLength() {
        return 4 + 2;
    }
    compareTo(other) {
        return this.addr < other.addr ? -1 : this.addr > other.addr ? 1 : this.offset < other.offset ? -1 : this.offset > other.offset ? 1 : 0;
    }
    static readFrom(buf) {
        return new PageOffsetValue(buf.readU32(), buf.readU16());
    }
    encode() {
        return this.addr * pow16 + this.offset;
    }
    static fromEncoded(num) {
        const offset = num % pow16;
        return new PageOffsetValue((num - offset) / pow16, offset);
    }
    [Runtime.customInspect]() {
        return `Addr(${this.addr}, ${this.offset})`;
    }
}
const PAGESIZE = getPageSize() || 4096;
const KEYSIZE_LIMIT = Math.floor(PAGESIZE / 4);
function getPageSize() {
    try {
        const val = Runtime.env.get("BTRDB_PAGESIZE");
        if (!val) return null;
        const num = parseInt(val);
        if (isNaN(num)) {
            console.error("BTRDB_PAGESIZE: expected an integer");
            return null;
        }
        return num;
    } catch (error) {
        return null;
    }
}
var PageType;
(function(PageType1) {
    PageType1[PageType1["None"] = 0] = "None";
    PageType1[PageType1["Super"] = 1] = "Super";
    PageType1[PageType1["RootTreeNode"] = 2] = "RootTreeNode";
    PageType1[PageType1["Set"] = 3] = "Set";
    PageType1[PageType1["Records"] = 4] = "Records";
    PageType1[PageType1["DocSet"] = 5] = "DocSet";
    PageType1[PageType1["DocRecords"] = 6] = "DocRecords";
    PageType1[PageType1["IndexTop"] = 7] = "IndexTop";
    PageType1[PageType1["Index"] = 8] = "Index";
    PageType1[PageType1["Data"] = 9] = "Data";
})(PageType || (PageType = {}));
class Page {
    storage;
    addr = -1;
    constructor(storage){
        this.storage = storage;
        this.init();
    }
    dirty = true;
    get hasAddr() {
        return this.addr != -1;
    }
    _newerCopy = null;
    _discard = false;
    freeBytes = PAGESIZE - 4;
    init() {}
    getDirty(addDirty) {
        if (this.hasNewerCopy()) {
            throw new BugError("getDirty on out-dated page");
        }
        if (this.dirty) {
            if (addDirty && !this.hasAddr) this.storage.addDirty(this);
            return this;
        } else {
            let dirty = new this._thisCtor(this.storage);
            dirty.dirty = true;
            this._copyTo(dirty);
            this._newerCopy = dirty;
            if (addDirty) this.storage.addDirty(dirty);
            return dirty;
        }
    }
    removeDirty() {
        if (this.hasNewerCopy()) {
            throw new BugError("removeDirty on out-dated page");
        }
        if (!this.dirty) throw new BugError("removeDirty on non-dirty page");
        this._discard = true;
    }
    hasNewerCopy() {
        if (this._newerCopy) {
            if (this._newerCopy._discard) {
                this._newerCopy = null;
                return false;
            } else {
                return true;
            }
        }
        return false;
    }
    getLatestCopy() {
        let p = this;
        while(p._newerCopy && !p._newerCopy._discard)p = p._newerCopy;
        return p;
    }
    writeTo(buf) {
        if (this.freeBytes < 0) {
            console.error(this);
            throw new BugError(`BUG: page content overflow (free ${this.freeBytes})`);
        }
        const beginPos = buf.pos;
        buf.writeU8(this.type);
        buf.writeU8(0);
        buf.writeU16(0);
        this._writeContent(buf);
        if (buf.pos - beginPos != PAGESIZE - this.freeBytes) {
            throw new BugError(`BUG: buffer written (${buf.pos - beginPos}) != space used (${PAGESIZE - this.freeBytes})`);
        }
    }
    readFrom(buf) {
        const beginPos = buf.pos;
        const type = buf.readU8();
        if (type != this.type) {
            throw new Error(`Wrong type in disk, should be ${this.type}, got ${type}, addr ${this.addr}`);
        }
        if (buf.readU8() != 0) throw new Error("Non-zero reserved field");
        if (buf.readU16() != 0) throw new Error("Non-zero reserved field");
        this._readContent(buf);
        if (buf.pos - beginPos != PAGESIZE - this.freeBytes) {
            throw new BugError(`BUG: buffer read (${buf.pos - beginPos}) != space used (${PAGESIZE - this.freeBytes})`);
        }
    }
    _debugView() {
        return {
            type: this.type,
            addr: this.addr,
            dirty: this.dirty,
            newerCopy: this._newerCopy?._debugView()
        };
    }
    [Runtime.customInspect]() {
        return "Page(" + Runtime.inspect(this._debugView()) + ")";
    }
    _copyTo(page) {
        if (Object.getPrototypeOf(this) != Object.getPrototypeOf(page)) {
            throw new Error("_copyTo() with different types");
        }
    }
    _writeContent(buf) {}
    _readContent(buf) {}
    get _thisCtor() {
        return Object.getPrototypeOf(this).constructor;
    }
}
class NodePage extends Page {
    keys = [];
    children = [];
    init() {
        super.init();
        this.freeBytes -= 2;
    }
    setKeys(newKeys, newChildren) {
        if (!(newKeys.length == 0 && newChildren.length == 0 || newKeys.length + 1 == newChildren.length)) {
            throw new Error("Invalid args");
        }
        if (this.keys) {
            this.freeBytes += calcSizeOfKeys(this.keys) + this.children.length * 4;
        }
        if (newKeys) {
            this.freeBytes -= calcSizeOfKeys(newKeys) + newChildren.length * 4;
        }
        this.keys = newKeys;
        this.children = newChildren;
    }
    spliceKeys(pos, delCount, key, leftChild) {
        if (leftChild < 0) throw new BugError("Invalid leftChild");
        let deleted;
        let deletedChildren;
        if (key) {
            deleted = this.keys.splice(pos, delCount, key);
            deletedChildren = this.children.splice(pos, delCount, leftChild || 0);
            if (delCount == 0 && this.keys.length == 1) {
                this.freeBytes -= 4;
                this.children.push(0);
            }
            this.freeBytes -= key.byteLength + 4;
        } else {
            deleted = this.keys.splice(pos, delCount);
            deletedChildren = this.children.splice(pos, delCount);
            if (delCount && this.keys.length == 0) {
                if (this.children[0] === 0) {
                    this.children.pop();
                    this.freeBytes += 4;
                }
            }
        }
        this.freeBytes += calcSizeOfKeys(deleted) + delCount * 4;
        return [
            deleted,
            deletedChildren
        ];
    }
    setChild(pos, child) {
        if (pos < 0 || this.children.length <= pos) {
            throw new BugError("pos out of range");
        }
        this.children[pos] = child;
    }
    setKey(pos, key) {
        if (pos < 0 || this.keys.length <= pos) {
            throw new BugError("pos out of range");
        }
        this.freeBytes -= key.byteLength - this.keys[pos].byteLength;
        this.keys[pos] = key;
    }
    async readChildPage(pos) {
        const childPage = await this.storage.readPage(this.children[pos], this._childCtor);
        return childPage;
    }
    createChildPage() {
        return new this._childCtor(this.storage).getDirty(true);
    }
    _debugView() {
        return {
            ...super._debugView(),
            keys: this.keys
        };
    }
    _writeContent(buf) {
        super._writeContent(buf);
        buf.writeU16(this.keys.length);
        for(let i11 = 0; i11 < this.keys.length; i11++){
            this.keys[i11].writeTo(buf);
        }
        for(let i12 = 0; i12 < this.children.length; i12++){
            buf.writeU32(this.children[i12]);
        }
    }
    _readContent(buf) {
        super._readContent(buf);
        const keyCount = buf.readU16();
        const posBefore = buf.pos;
        for(let i13 = 0; i13 < keyCount; i13++){
            this.keys.push(this._readValue(buf));
        }
        const childrenCount = keyCount ? keyCount + 1 : 0;
        for(let i21 = 0; i21 < childrenCount; i21++){
            this.children.push(buf.readU32());
        }
        this.freeBytes -= buf.pos - posBefore;
    }
    _copyTo(page) {
        super._copyTo(page);
        page.keys = [
            ...this.keys
        ];
        page.children = [
            ...this.children
        ];
        page.freeBytes = this.freeBytes;
    }
    get _childCtor() {
        return this._thisCtor;
    }
}
function calcSizeOfKeys(keys3) {
    let sum = 0;
    for (const it of keys3){
        sum += it.byteLength;
    }
    return sum;
}
function buildTreePageClasses(options) {
    class ChildNodePage extends NodePage {
        constructor(...args){
            super(...args);
        }
        get type() {
            return options.childPageType;
        }
        _readValue(buf) {
            return options.valueReader(buf);
        }
        get _childCtor() {
            return ChildNodePage;
        }
    }
    class TopNodePage extends ChildNodePage {
        get type() {
            return options.topPageType;
        }
        rev = 1;
        count = 0;
        init() {
            super.init();
            this.freeBytes -= 8;
        }
        _debugView() {
            return {
                ...super._debugView(),
                rev: this.rev,
                count: this.count
            };
        }
        _writeContent(buf) {
            buf.writeU32(this.rev);
            buf.writeU32(this.count);
            super._writeContent(buf);
        }
        _readContent(buf) {
            this.rev = buf.readU32();
            this.count = buf.readU32();
            super._readContent(buf);
        }
        _copyTo(page) {
            super._copyTo(page);
            page.rev = this.rev;
            page.count = this.count;
        }
    }
    return {
        top: TopNodePage,
        child: ChildNodePage
    };
}
function buildSetPageClass(baseClass) {
    class SetPageBase1 extends baseClass {
        prefixedName = "";
        lock = new OneWriterLock();
        _copyTo(page) {
            super._copyTo(page);
            page.prefixedName = this.prefixedName;
        }
        getDirty(addDirty) {
            var r = super.getDirty(addDirty);
            if (r != this) {
                this.storage.dirtySets.push(r);
            }
            return r;
        }
    }
    return SetPageBase1;
}
const { top: SetPageBase , child: RecordsPage  } = buildTreePageClasses({
    valueReader: (buf)=>KValue.readFrom(buf, JSValue.readFrom, PageOffsetValue.readFrom)
    ,
    topPageType: PageType.Set,
    childPageType: PageType.Records
});
const SetPage = buildSetPageClass(SetPageBase);
const { top: DocSetPageBase1 , child: DocsPage  } = buildTreePageClasses({
    valueReader: (buf)=>KValue.readFrom(buf, JSValue.readFrom, PageOffsetValue.readFrom)
    ,
    topPageType: PageType.DocSet,
    childPageType: PageType.DocRecords
});
const DocSetPageBase2 = buildSetPageClass(DocSetPageBase1);
class DocSetPage extends DocSetPageBase2 {
    _lastId = new JSValue(null);
    get lastId() {
        return this._lastId;
    }
    set lastId(val) {
        this.freeBytes += this._lastId.byteLength;
        this._lastId = val;
        this.freeBytes -= this._lastId.byteLength;
    }
    indexes = null;
    indexesInfoAddr = new PageOffsetValue(0, 0);
    indexesAddrs = [];
    indexesAddrMap = {};
    setIndexes(newIndexes, map) {
        const addrs = Object.values(map);
        this.freeBytes += this.indexesAddrs.length * 4;
        this.freeBytes -= addrs.length * 4;
        this.indexes = newIndexes;
        this.indexesAddrs = addrs;
        this.indexesInfoAddr = addrs.length == 0 ? new PageOffsetValue(0, 0) : this.storage.addData(new IndexesInfoValue(newIndexes));
        this.indexesAddrMap = map;
    }
    async ensureIndexes() {
        if (!this.indexes) {
            if (this.indexesInfoAddr.addr == 0 && this.indexesInfoAddr.offset == 0) {
                this.indexes = {};
            } else {
                const value = await this.storage.readData(this.indexesInfoAddr, IndexesInfoValue);
                this.indexes = value.indexes;
                this.indexesAddrMap = Object.fromEntries(Object.keys(this.indexes).map((x, i14)=>[
                        x,
                        this.indexesAddrs[i14]
                    ]
                ));
            }
        }
        return this.indexes;
    }
    init() {
        super.init();
        this.freeBytes -= 1 + 1 + 6;
    }
    _writeContent(buf) {
        super._writeContent(buf);
        this._lastId.writeTo(buf);
        buf.writeU8(this.indexesAddrs.length);
        for (const indexAddr of this.indexesAddrs){
            buf.writeU32(indexAddr);
        }
        this.indexesInfoAddr.writeTo(buf);
    }
    _readContent(buf) {
        super._readContent(buf);
        this.lastId = JSValue.readFrom(buf);
        const indexCount = buf.readU8();
        for(let i15 = 0; i15 < indexCount; i15++){
            this.indexesAddrs.push(buf.readU32());
        }
        this.indexesInfoAddr = PageOffsetValue.readFrom(buf);
        this.freeBytes -= 4 * indexCount;
    }
    _copyTo(other) {
        super._copyTo(other);
        other._lastId = this._lastId;
        other.indexes = this.indexes;
        other.indexesInfoAddr = this.indexesInfoAddr;
        other.indexesAddrs = [
            ...this.indexesAddrs
        ];
        other.indexesAddrMap = {
            ...this.indexesAddrMap
        };
    }
}
class IndexesInfoValue {
    indexes;
    constructor(indexes){
        this.indexes = indexes;
        let size = 1;
        for(const key in indexes){
            if (Object.prototype.hasOwnProperty.call(indexes, key)) {
                const info = indexes[key];
                size += Buffer.calcLenEncodedStringSize(key) + Buffer.calcLenEncodedStringSize(info.funcStr) + 1;
            }
        }
        this.byteLength = size;
    }
    byteLength;
    writeTo(buf) {
        const indexes = Object.entries(this.indexes);
        buf.writeU8(indexes.length);
        for (const [name, info] of indexes){
            buf.writeString(name);
            buf.writeString(info.funcStr);
            buf.writeU8(+info.unique);
        }
    }
    static readFrom(buf) {
        const indexCount = buf.readU8();
        const indexes = {};
        for(let i16 = 0; i16 < indexCount; i16++){
            const k = buf.readString();
            indexes[k] = new IndexInfo(buf.readString(), !!buf.readU8(), null);
        }
        return new IndexesInfoValue(indexes);
    }
}
class IndexInfo {
    funcStr;
    unique;
    cachedFunc;
    constructor(funcStr, unique, cachedFunc){
        this.funcStr = funcStr;
        this.unique = unique;
        this.cachedFunc = cachedFunc;
    }
    get func() {
        if (!this.cachedFunc) {
            this.cachedFunc = (1, eval)(this.funcStr);
        }
        return this.cachedFunc;
    }
}
const { top: IndexTopPage , child: IndexPage  } = buildTreePageClasses({
    valueReader: (buf)=>KValue.readFrom(buf, JSValue.readFrom, PageOffsetValue.readFrom)
    ,
    topPageType: PageType.IndexTop,
    childPageType: PageType.Index
});
class DataPage extends Page {
    get type() {
        return PageType.Data;
    }
    next = 0;
    buffer = null;
    init() {
        super.init();
        this.freeBytes -= 4;
    }
    createBuffer() {
        this.buffer = new Uint8Array(PAGESIZE - 8);
    }
    get usedBytes() {
        return PAGESIZE - this.freeBytes - 8;
    }
    addUsage(len) {
        this.freeBytes -= len;
    }
    _writeContent(buf) {
        super._writeContent(buf);
        buf.writeU32(this.next);
        buf.writeBuffer(this.buffer.subarray(0, this.usedBytes));
    }
    _readContent(buf) {
        super._readContent(buf);
        this.next = buf.readU32();
        this.buffer = buf.buffer.subarray(buf.pos, PAGESIZE);
        buf.pos = PAGESIZE;
        this.freeBytes = 0;
    }
}
class RootTreeNode extends NodePage {
    get type() {
        return PageType.RootTreeNode;
    }
    _readValue(buf) {
        return KValue.readFrom(buf, StringValue.readFrom, UIntValue.readFrom);
    }
    get _childCtor() {
        return RootTreeNode;
    }
}
class SuperPage extends RootTreeNode {
    get type() {
        return PageType.Super;
    }
    version = 1;
    rev = 1;
    prevSuperPageAddr = 0;
    setCount = 0;
    init() {
        super.init();
        this.freeBytes -= 4 * 4;
    }
    _writeContent(buf) {
        super._writeContent(buf);
        buf.writeU32(this.version);
        buf.writeU32(this.rev);
        buf.writeU32(this.prevSuperPageAddr);
        buf.writeU32(this.setCount);
    }
    _readContent(buf) {
        super._readContent(buf);
        this.version = buf.readU32();
        this.rev = buf.readU32();
        this.prevSuperPageAddr = buf.readU32();
        this.setCount = buf.readU32();
    }
    _copyTo(other) {
        super._copyTo(other);
        other.rev = this.rev + 1;
        other.version = this.version;
        other.prevSuperPageAddr = this.prevSuperPageAddr;
        other.setCount = this.setCount;
    }
    getDirty(addDirty) {
        var dirty = this.storage.superPage = super.getDirty(false);
        return dirty;
    }
    _debugView() {
        return {
            ...super._debugView(),
            rev: this.rev,
            version: this.version,
            setCount: this.setCount
        };
    }
}
class Node {
    page;
    parent;
    posInParent;
    constructor(page, parent, posInParent){
        this.page = page;
        this.parent = parent;
        this.posInParent = posInParent;
    }
    get addr() {
        return this.page.addr;
    }
    get keys() {
        return this.page.keys;
    }
    get children() {
        return this.page.children;
    }
    findKey(key) {
        const keys4 = this.page.keys;
        let l = 0, r = keys4.length - 1;
        while(l <= r){
            const m = Math.round((l + r) / 2);
            const c = key.compareTo(keys4[m]);
            if (c == 0) return {
                found: true,
                pos: m,
                val: keys4[m]
            };
            else if (c > 0) l = m + 1;
            else r = m - 1;
        }
        return {
            found: false,
            pos: l,
            val: undefined
        };
    }
    async getAllValues(array) {
        if (!array) array = [];
        await this.traverseKeys((key)=>{
            array.push(key);
        });
        return array;
    }
    async readChildPage(pos) {
        const childPage = await this.page.readChildPage(pos);
        return new Node(childPage, this, pos);
    }
    async _dumpTree() {
        const result = [
            `(addr ${this.page.addr}${this.page.dirty ? " (dirty)" : ""})`, 
        ];
        for(let pos = 0; pos < this.children.length; pos++){
            const leftAddr = this.children[pos];
            if (leftAddr) {
                const leftPage = await this.readChildPage(pos);
                result.push(await leftPage._dumpTree());
            }
            if (pos < this.keys.length) {
                result.push(this.keys[pos]);
            }
        }
        return result;
    }
    async traverseKeys(func) {
        for(let pos = 0; pos < this.children.length; pos++){
            const leftAddr = this.children[pos];
            if (leftAddr) {
                const leftPage = await this.readChildPage(pos);
                await leftPage.traverseKeys(func);
            }
            if (pos < this.keys.length) {
                await func(this.keys[pos], this, pos);
            }
        }
    }
    async *iterateKeys() {
        for(let pos = 0; pos < this.children.length; pos++){
            const leftAddr = this.children[pos];
            if (leftAddr) {
                const leftPage = await this.readChildPage(pos);
                yield* await leftPage.iterateKeys();
            }
            if (pos < this.keys.length) {
                yield this.keys[pos];
            }
        }
        return;
    }
    async findKeyRecursive(key) {
        let node = this;
        while(true){
            const { found , pos , val  } = node.findKey(key);
            if (found) return {
                found: true,
                node,
                pos,
                val: val
            };
            if (!node.children[pos]) {
                return {
                    found: false,
                    node,
                    pos,
                    val: val
                };
            }
            node = await node.readChildPage(pos);
        }
    }
    async set(key, val, policy) {
        const { found , node , pos , val: oldValue  } = await this.findKeyRecursive(key);
        let action = "noop";
        if (node.page.hasNewerCopy()) {
            console.info({
                cur: node.page._debugView(),
                new: node.page._newerCopy._debugView()
            });
            throw new BugError("BUG: set() -> findIndex() returns old copy.");
        }
        if (val != null) {
            const dirtyNode = node.getDirty(false);
            if (found) {
                if (policy === "no-change") {
                    throw new AlreadyExistError("key already exists");
                } else if (policy === "can-append") {
                    dirtyNode.insertAt(pos, val, dirtyNode.children[pos]);
                    dirtyNode.setChild(pos + 1, 0);
                    action = "added";
                } else {
                    dirtyNode.setKey(pos, val);
                    action = "changed";
                }
            } else {
                if (policy === "change-only") {
                    throw new NotExistError("key doesn't exists");
                }
                dirtyNode.insertAt(pos, val);
                action = "added";
            }
            dirtyNode.postChange();
        } else {
            if (found) {
                await node.deleteAt(pos);
                action = "removed";
            }
        }
        return {
            action,
            oldValue: oldValue ?? null
        };
    }
    async deleteAt(pos) {
        const dirtyNode = this.getDirty(false);
        const oldLeftAddr = dirtyNode.children[pos];
        if (oldLeftAddr) {
            let leftSubNode = await dirtyNode.readChildPage(pos);
            const leftNode = leftSubNode;
            while(leftSubNode.children[leftSubNode.children.length - 1]){
                leftSubNode = await leftSubNode.readChildPage(leftSubNode.children.length - 1);
            }
            const leftKey = leftSubNode.keys[leftSubNode.keys.length - 1];
            dirtyNode.page.spliceKeys(pos, 1, leftKey, leftNode.addr);
            await leftSubNode.deleteAt(leftSubNode.keys.length - 1);
            dirtyNode.postChange();
        } else {
            dirtyNode.page.spliceKeys(pos, 1);
            if (dirtyNode.keys.length == 0 && dirtyNode.parent) {
                const dirtyParent = dirtyNode.parent.getDirty(false);
                dirtyParent.setChild(dirtyNode.posInParent, dirtyNode.children[0]);
                dirtyParent.postChange();
                dirtyNode.parent = undefined;
                dirtyNode.page.removeDirty();
            } else {
                dirtyNode.postChange();
            }
        }
    }
    insertAt(pos, key, leftChild = 0) {
        this.page.spliceKeys(pos, 0, key, leftChild);
    }
    setChild(pos, child) {
        this.page.setChild(pos, child);
    }
    setKey(pos, key) {
        this.page.setKey(pos, key);
    }
    postChange() {
        if (this.page.hasNewerCopy()) {
            throw new BugError("BUG: postChange() on old copy.");
        }
        if (!this.page.dirty) {
            throw new BugError("BUG: postChange() on non-dirty page.");
        }
        if (this.page.freeBytes < 0) {
            if (this.keys.length <= 2) {
                throw new Error("Not implemented. freeBytes=" + this.page.freeBytes + " keys=" + Runtime.inspect(this.keys));
            }
            const leftSib = this.page.createChildPage();
            const leftCount = Math.floor(this.keys.length / 2);
            const leftKeys = this.page.spliceKeys(0, leftCount);
            leftKeys[1].push(0);
            leftSib.setKeys(leftKeys[0], leftKeys[1]);
            const [[middleKey], [middleLeftChild]] = this.page.spliceKeys(0, 1);
            leftSib.setChild(leftCount, middleLeftChild);
            if (this.parent) {
                this.getDirty(true);
                this.getParentDirty();
                this.parent.setChild(this.posInParent, this.addr);
                this.parent.insertAt(this.posInParent, middleKey, leftSib.addr);
                this.parent.postChange();
            } else {
                const rightChild = this.page.createChildPage();
                rightChild.setKeys(this.keys, this.children);
                this.page.setKeys([
                    middleKey
                ], [
                    leftSib.addr,
                    rightChild.addr
                ]);
                this.getDirty(true);
                this.makeDirtyToRoot();
            }
        } else {
            this.getDirty(true);
            if (this.parent) {
                this.makeDirtyToRoot();
            }
        }
    }
    getDirty(addDirty) {
        this.page = this.page.getDirty(addDirty);
        return this;
    }
    getParentDirty() {
        return this.parent = this.parent.getDirty(true);
    }
    makeDirtyToRoot() {
        if (!this.page.dirty) {
            throw new BugError("BUG: makeDirtyToRoot() on non-dirty page");
        }
        let node = this;
        while(node.parent){
            const parent = node.parent;
            const parentWasDirty = parent.page.dirty;
            const dirtyParent = node.parent = parent.getDirty(true);
            dirtyParent.setChild(node.posInParent, node.addr);
            node = dirtyParent;
            if (parentWasDirty) break;
        }
    }
}
function EQ(index, val) {
    return {
        eq: [
            index,
            val
        ],
        async *run (page) {
            const keyv = new JSValue(val);
            const result = await findIndexKey(page, index, keyv, false);
            for await (const it of iterateNode(result.node, result.pos, false)){
                if (compareJSValue(keyv, it.key) === 0) {
                    yield it.value;
                } else {
                    break;
                }
            }
        }
    };
}
function NE(index, val) {
    return NOT(EQ(index, val));
}
function GT(index, val) {
    return BETWEEN(index, val, null, false, false);
}
function GE(index, val) {
    return BETWEEN(index, val, null, true, false);
}
function LT(index, val) {
    return BETWEEN(index, null, val, false, false);
}
function LE(index, val) {
    return BETWEEN(index, null, val, false, true);
}
function BETWEEN(index, min, max, minInclusive, maxInclusive) {
    return {
        between: [
            index,
            min,
            max,
            minInclusive,
            maxInclusive
        ],
        async *run (page) {
            const vMin = min == null ? null : new JSValue(min);
            const vMax = max == null ? null : new JSValue(max);
            let keyIterator;
            if (vMin) {
                const begin = await findIndexKey(page, index, vMin, !minInclusive);
                keyIterator = iterateNode(begin.node, begin.pos, false);
            } else {
                keyIterator = page.iterateKeys();
            }
            for await (const key of keyIterator){
                let _c;
                if (vMax == null || (_c = compareJSValue(vMax, key.key)) > 0 || _c === 0 && maxInclusive) {
                    yield key.value;
                } else {
                    break;
                }
            }
        }
    };
}
function AND(...queries) {
    if (queries.length == 0) throw new Error("No queries");
    return {
        and: queries,
        async *run (page) {
            let set = new Set();
            let nextSet = new Set();
            for await (const val of queries[0].run(page)){
                set.add(val.encode());
            }
            for(let i17 = 1; i17 < queries.length; i17++){
                const qResult = queries[i17].run(page);
                for await (const val of qResult){
                    const valEncoded = val.encode();
                    if (set.has(valEncoded)) {
                        nextSet.add(valEncoded);
                    }
                }
                set.clear();
                [set, nextSet] = [
                    nextSet,
                    set
                ];
            }
            for (const val1 of set){
                yield PageOffsetValue.fromEncoded(val1);
            }
        }
    };
}
function OR(...queries) {
    if (queries.length == 0) throw new Error("No queries");
    return {
        or: queries,
        async *run (page) {
            let set = new Set();
            for (const sub of queries){
                const subResult = sub.run(page);
                for await (const val of subResult){
                    const valEncoded = val.encode();
                    if (!set.has(valEncoded)) {
                        set.add(valEncoded);
                        yield val;
                    }
                }
            }
        }
    };
}
function NOT(query1) {
    return {
        not: query1,
        async *run (page) {
            let set = new Set();
            const subResult = query1.run(page);
            for await (const val of subResult){
                const valEncoded = val.encode();
                set.add(valEncoded);
            }
            for await (const key of page.iterateKeys()){
                if (!set.has(key.value.encode())) {
                    yield key.value;
                }
            }
        }
    };
}
async function findIndexKey(node, index, vKey, rightMost) {
    let indexPage;
    if (index == "id") {
        indexPage = node;
    } else {
        const info = (await node.page.ensureIndexes())[index];
        if (!info) throw new Error("Specified index does not exist.");
        indexPage = new Node(await node.page.storage.readPage(node.page.indexesAddrMap[index], IndexTopPage));
    }
    const indexResult = await indexPage.findKeyRecursive(rightMost ? new KeyRightmostComparator(vKey) : new KeyLeftmostComparator(vKey));
    return indexResult;
}
async function* iterateNode(node, pos, reverse) {
    while(true){
        const val = node.keys[pos];
        if (val) {
            yield val;
        }
        if (reverse) pos--;
        else pos++;
        if (node.children[pos]) {
            do {
                node = await node.readChildPage(pos);
                pos = reverse ? node.keys.length - 1 : 0;
            }while (node.children[pos])
        }
        if ((reverse ? -1 : node.keys.length + 1) == pos) {
            if (node.parent) {
                pos = node.posInParent;
                node = node.parent;
            } else {
                break;
            }
        }
    }
}
function numberIdGenerator(lastId) {
    if (lastId == null) return 1;
    return lastId + 1;
}
var Op;
(function(Op1) {
    Op1[Op1["insert"] = 0] = "insert";
    Op1[Op1["upsert"] = 1] = "upsert";
    Op1[Op1["update"] = 2] = "update";
    Op1[Op1["delete"] = 3] = "delete";
})(Op || (Op = {}));
class DbDocSet {
    _db;
    name;
    isSnapshot;
    _page;
    constructor(page, _db, name, isSnapshot){
        this._db = _db;
        this.name = name;
        this.isSnapshot = isSnapshot;
        this._page = page;
    }
    get page() {
        if (this.isSnapshot) return this._page;
        return this._page = this._page.getLatestCopy();
    }
    get node() {
        return new Node(this.page);
    }
    get count() {
        return this.page.count;
    }
    idGenerator = numberIdGenerator;
    async get(key) {
        const { found , val  } = await this.node.findKeyRecursive(new KeyComparator(new JSValue(key)));
        if (!found) return null;
        const docVal = await this._readDocument(val.value);
        return docVal.val;
    }
    async _getAllRaw() {
        const lockpage = this.page;
        await lockpage.lock.enterReader();
        const thisnode = this.node;
        try {
            return await thisnode.getAllValues();
        } finally{
            lockpage.lock.exitReader();
        }
    }
    async getAll() {
        const lockpage = this.page;
        await lockpage.lock.enterReader();
        const thisnode = this.node;
        try {
            const result = [];
            for await (const kv of thisnode.iterateKeys()){
                const doc = await this._readDocument(kv.value);
                result.push(doc.val);
            }
            return result;
        } finally{
            lockpage.lock.exitReader();
        }
    }
    async forEach(fn) {
        const lockpage = this.page;
        await lockpage.lock.enterReader();
        const thisnode = this.node;
        try {
            for await (const kv of thisnode.iterateKeys()){
                const doc = await this._readDocument(kv.value);
                await fn(doc.val);
            }
        } finally{
            lockpage.lock.exitReader();
        }
    }
    async getIds() {
        return (await this._getAllRaw()).map((x)=>x.key.val
        );
    }
    async insert(doc) {
        await this._set(doc.id, doc, Op.insert);
    }
    async update(doc) {
        const key = doc.id;
        if (key == null) throw new Error('"id" property doesn\'t exist');
        await this._set(key, doc, Op.update);
    }
    async upsert(doc) {
        const key = doc.id;
        if (key == null) throw new Error('"id" property doesn\'t exist');
        await this._set(key, doc, Op.upsert);
    }
    async delete(key) {
        const { action  } = await this._set(key, null, Op.delete);
        return action == "removed";
    }
    async _set(key, doc, op) {
        if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
        await this._db.commitLock.enterWriter();
        const lockpage = this.page.getDirty(false);
        await lockpage.lock.enterWriter();
        const thisnode = this.node;
        try {
            if (op === Op.insert) {
                if (key == null) {
                    key = doc.id = this.idGenerator(lockpage.lastId.val);
                }
            }
            const dataPos = !doc ? null : await lockpage.storage.addData(new DocumentValue(doc));
            const keyv = new JSValue(key);
            if (keyv.byteLength > KEYSIZE_LIMIT) {
                throw new Error(`The id size is too large (${keyv.byteLength}), the limit is ${KEYSIZE_LIMIT}`);
            }
            const valv = !doc ? null : new KValue(keyv, dataPos);
            const { action , oldValue: oldDoc  } = await thisnode.set(new KeyComparator(keyv), valv, op === Op.insert ? "no-change" : op === Op.update ? "change-only" : "can-change");
            if (action == "added") {
                if (keyv.compareTo(lockpage.lastId) > 0) {
                    lockpage.lastId = keyv;
                }
                lockpage.count += 1;
            } else if (action == "removed") {
                lockpage.count -= 1;
            }
            let nextSeq = 0;
            for (const [indexName, indexInfo] of Object.entries(await lockpage.ensureIndexes())){
                const seq = nextSeq++;
                const index = (await lockpage.storage.readPage(lockpage.indexesAddrs[seq], IndexTopPage)).getDirty(false);
                const indexNode = new Node(index);
                if (oldDoc) {
                    const oldKey = new JSValue(indexInfo.func((await this._readDocument(oldDoc.value)).val));
                    const setResult = await indexNode.set(new KValue(oldKey, oldDoc.value), null, "no-change");
                    if (setResult.action != "removed") {
                        throw new BugError("BUG: can not remove index key: " + Runtime.inspect({
                            oldDoc,
                            indexInfo,
                            oldKey,
                            setResult
                        }));
                    }
                }
                if (doc) {
                    const kv = new KValue(new JSValue(indexInfo.func(doc)), dataPos);
                    if (kv.key.byteLength > KEYSIZE_LIMIT) {
                        throw new Error(`The index key size is too large (${kv.key.byteLength}), the limit is ${KEYSIZE_LIMIT}`);
                    }
                    const setResult = await indexNode.set(indexInfo.unique ? new KeyComparator(kv.key) : kv, kv, "no-change");
                    if (setResult.action != "added") {
                        throw new BugError("BUG: can not add index key: " + Runtime.inspect({
                            kv,
                            indexInfo,
                            setResult
                        }));
                    }
                }
                const newIndexAddr = indexNode.page.getDirty(true).addr;
                lockpage.indexesAddrs[seq] = newIndexAddr;
                lockpage.indexesAddrMap[indexName] = newIndexAddr;
            }
            if (action !== "noop") {
                if (this._db.autoCommit) await this._db._autoCommit();
            }
            return {
                action,
                key: keyv
            };
        } finally{
            lockpage.lock.exitWriter();
            this._db.commitLock.exitWriter();
        }
    }
    async getIndexes() {
        await this.page.ensureIndexes();
        return Object.fromEntries(Object.entries(this.page.indexes).map(([k, v])=>[
                k,
                {
                    key: v.funcStr,
                    unique: v.unique
                }
            ]
        ));
    }
    async useIndexes(indexDefs) {
        const toBuild = [];
        const toRemove = [];
        const currentIndex = await this.page.ensureIndexes();
        for(const key in indexDefs){
            if (Object.prototype.hasOwnProperty.call(indexDefs, key)) {
                if (key == "id") throw new Error("Cannot use 'id' as index name");
                const func = indexDefs[key];
                if (!Object.prototype.hasOwnProperty.call(currentIndex, key) || currentIndex[key].funcStr != func.toString()) {
                    toBuild.push(key);
                }
            }
        }
        for(const key1 in currentIndex){
            if (Object.prototype.hasOwnProperty.call(currentIndex, key1)) {
                if (!Object.prototype.hasOwnProperty.call(indexDefs, key1)) {
                    toRemove.push(key1);
                }
            }
        }
        if (toBuild.length || toRemove.length) {
            if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
            await this._db.commitLock.enterWriter();
            const lockpage = this.page.getDirty(false);
            await lockpage.lock.enterWriter();
            const thisnode = this.node;
            try {
                const newIndexes = {
                    ...currentIndex
                };
                const newAddrs = {
                    ...lockpage.indexesAddrMap
                };
                for (const key of toRemove){
                    delete newIndexes[key];
                    delete newAddrs[key];
                }
                for (const key2 of toBuild){
                    const obj = indexDefs[key2];
                    const func = typeof obj == "function" ? obj : obj.key;
                    const unique = typeof obj == "object" && obj.unique == true;
                    const info = new IndexInfo(func.toString(), unique, func);
                    const index = new IndexTopPage(lockpage.storage).getDirty(true);
                    const indexNode = new Node(index);
                    await thisnode.traverseKeys(async (k)=>{
                        const doc = await this._readDocument(k.value);
                        const indexKV = new KValue(new JSValue(func(doc.val)), k.value);
                        if (indexKV.key.byteLength > KEYSIZE_LIMIT) {
                            throw new Error(`The index key size is too large (${indexKV.key.byteLength}), the limit is ${KEYSIZE_LIMIT}`);
                        }
                        await indexNode.set(unique ? new KeyComparator(indexKV.key) : indexKV, indexKV, "no-change");
                    });
                    newAddrs[key2] = index.addr;
                    newIndexes[key2] = info;
                }
                lockpage.setIndexes(newIndexes, newAddrs);
                thisnode.postChange();
                if (this._db.autoCommit) await this._db._autoCommit();
            } finally{
                lockpage.lock.exitWriter();
                this._db.commitLock.exitWriter();
            }
        }
    }
    async query(query2) {
        const lockpage = this.page;
        await lockpage.lock.enterReader();
        try {
            const result = [];
            for await (const docAddr of query2.run(this.node)){
                result.push(await this._readDocument(docAddr));
            }
            return result.sort((a, b)=>a.key.compareTo(b.key)
            ).map((doc)=>doc.val
            );
        } finally{
            lockpage.lock.exitReader();
        }
    }
    findIndex(index, val) {
        return this.query(EQ(index, val));
    }
    _readDocument(dataAddr) {
        return this.page.storage.readData(dataAddr, DocumentValue);
    }
    async _cloneTo(other) {
        const thisStorage = this.page.storage;
        const otherStorage = other.page.storage;
        const dataAddrMap = new Map();
        for await (const key of this.node.iterateKeys()){
            const doc = await thisStorage.readData(key.value, DocumentValue);
            const newAddr = await otherStorage.addData(doc);
            dataAddrMap.set(key.value.encode(), newAddr.encode());
            const newKey = new KValue(key.key, newAddr);
            await new Node(other.page).set(newKey, newKey, "no-change");
        }
        const indexes = await this.page.ensureIndexes();
        const newIndexes = {};
        const newAddrs = {};
        for (const [name, info] of Object.entries(indexes)){
            const indexPage = await thisStorage.readPage(this.page.indexesAddrMap[name], IndexTopPage);
            const otherIndex = new IndexTopPage(otherStorage).getDirty(true);
            const otherIndexNode = new Node(otherIndex);
            for await (const key of new Node(indexPage).iterateKeys()){
                const newKey = new KValue(key.key, PageOffsetValue.fromEncoded(dataAddrMap.get(key.value.encode())));
                await otherIndexNode.set(newKey, newKey, "no-change");
            }
            newIndexes[name] = info;
            newAddrs[name] = otherIndex.addr;
        }
        other.page.setIndexes(newIndexes, newAddrs);
        other.node.postChange();
        other.page.count = this.page.count;
    }
    async _dump() {
        return {
            docTree: await this.node._dumpTree(),
            indexes: Object.fromEntries(await Promise.all(Object.entries(await this.page.ensureIndexes()).map(async ([name, info])=>{
                const indexPage = await this.page.storage.readPage(this.page.indexesAddrMap[name], IndexTopPage);
                return [
                    name,
                    await new Node(indexPage)._dumpTree()
                ];
            })))
        };
    }
}
class DbSet {
    _db;
    name;
    isSnapshot;
    _page;
    constructor(page, _db, name, isSnapshot){
        this._db = _db;
        this.name = name;
        this.isSnapshot = isSnapshot;
        this._page = page;
    }
    get page() {
        if (this.isSnapshot) return this._page;
        return this._page = this._page.getLatestCopy();
    }
    get node() {
        return new Node(this.page);
    }
    get count() {
        return this.page.count;
    }
    async get(key) {
        const lockpage = this.page;
        await lockpage.lock.enterReader();
        try {
            const { found , val  } = await this.node.findKeyRecursive(new KeyComparator(new JSValue(key)));
            if (!found) return null;
            return (await this.readValue(val)).val;
        } finally{
            lockpage.lock.exitReader();
        }
    }
    async _getAllRaw() {
        const lockpage = this.page;
        await lockpage.lock.enterReader();
        try {
            return await this.node.getAllValues();
        } finally{
            lockpage.lock.exitReader();
        }
    }
    async getAll() {
        const result = [];
        const lockpage = this.page;
        await lockpage.lock.enterReader();
        try {
            for await (const key of this.node.iterateKeys()){
                result.push({
                    key: key.key.val,
                    value: (await this.readValue(key)).val
                });
            }
            return result;
        } finally{
            lockpage.lock.exitReader();
        }
    }
    async getKeys() {
        return (await this._getAllRaw()).map((x)=>x.key.val
        );
    }
    async forEach(fn) {
        const lockpage = this.page;
        await lockpage.lock.enterReader();
        try {
            for await (const key of this.node.iterateKeys()){
                await fn(key.key.val, (await this.readValue(key)).val);
            }
        } finally{
            lockpage.lock.exitReader();
        }
    }
    async set(key, val) {
        if (this.isSnapshot) throw new Error("Cannot change set in DB snapshot.");
        const keyv = new JSValue(key);
        if (keyv.byteLength > KEYSIZE_LIMIT) {
            throw new Error(`The key size is too large (${keyv.byteLength}), the limit is ${KEYSIZE_LIMIT}`);
        }
        await this._db.commitLock.enterWriter();
        const lockpage = this.page.getDirty(false);
        await lockpage.lock.enterWriter();
        try {
            const dataAddr = this.page.storage.addData(new JSValue(val));
            const valv = val == null ? null : new KValue(keyv, dataAddr);
            const { action  } = await this.node.set(new KeyComparator(keyv), valv, "can-change");
            if (action == "added") {
                lockpage.count += 1;
            } else if (action == "removed") {
                lockpage.count -= 1;
            }
            if (action == "noop") {
                return false;
            } else {
                if (this._db.autoCommit) await this._db._autoCommit();
                return true;
            }
        } finally{
            lockpage.lock.exitWriter();
            this._db.commitLock.exitWriter();
        }
    }
    delete(key) {
        return this.set(key, null);
    }
    readValue(node) {
        return this.page.storage.readData(node.value, JSValue);
    }
    async _cloneTo(other) {
        const otherStorage = other._page.storage;
        for await (const kv of this.node.iterateKeys()){
            const newKv = new KValue(kv.key, otherStorage.addData(await this.readValue(kv)));
            await other.node.set(new KeyComparator(newKv.key), newKv, "no-change");
        }
        other.page.count = this.page.count;
    }
    async _dump() {
        return {
            kvTree: await this.node._dumpTree()
        };
    }
}
class LRUMap {
    map = new Map();
    newest = null;
    oldest = null;
    get size() {
        return this.map.size;
    }
    add(key, val) {
        const entry = {
            key,
            value: val,
            older: this.newest,
            newer: null
        };
        this.map.set(key, entry);
        if (this.newest !== null) this.newest.newer = entry;
        this.newest = entry;
        if (this.oldest === null) this.oldest = entry;
    }
    set(key, val) {
        this.delete(key);
        this.add(key, val);
    }
    get(key) {
        const entry = this.map.get(key);
        if (entry === undefined) return undefined;
        if (entry !== this.newest) {
            if (entry === this.oldest) {
                this.oldest = entry.newer;
                this.oldest.older = null;
            } else {
                entry.newer.older = entry.older;
                entry.older.newer = entry.newer;
            }
            this.newest.newer = entry;
            entry.older = this.newest;
            entry.newer = null;
            this.newest = entry;
        }
        return entry.value;
    }
    delete(key) {
        const entry = this.map.get(key);
        if (entry === undefined) return false;
        this.map.delete(key);
        if (entry === this.newest) {
            this.newest = entry.older;
            if (this.newest) this.newest.newer = null;
            else this.oldest = null;
        } else if (entry === this.oldest) {
            this.oldest = entry.newer;
            this.oldest.older = null;
        } else {
            entry.newer.older = entry.older;
            entry.older.newer = entry.newer;
        }
        return true;
    }
    *valuesFromOldest() {
        for(let node = this.oldest; node !== null; node = node.newer){
            yield node.value;
        }
    }
    *[Symbol.iterator]() {
        for(let node = this.oldest; node !== null; node = node.newer){
            yield [
                node.key,
                node.value
            ];
        }
    }
}
const METADATA_CACHE_LIMIT = Math.round(8 * 1024 * 1024 / PAGESIZE);
const DATA_CACHE_LIMIT = Math.round(8 * 1024 * 1024 / PAGESIZE);
const TOTAL_CACHE_LIMIT = METADATA_CACHE_LIMIT + DATA_CACHE_LIMIT;
class PageStorageCounter {
    pageWrites = 0;
    pageFreebyteWrites = 0;
    acutalPageReads = 0;
    cachedPageReads = 0;
    cacheCleans = 0;
    dataAdds = 0;
    dataReads = 0;
}
class PageStorage {
    metaCache = new LRUMap();
    dataCache = new LRUMap();
    dirtyPages = [];
    nextAddr = 0;
    superPage = undefined;
    cleanSuperPage = undefined;
    dirtySets = [];
    deferWritingQueue = new TaskQueue();
    dataPage = undefined;
    dataPageBuffer = undefined;
    get cleanAddr() {
        return this.cleanSuperPage?.addr ?? 0;
    }
    writtenAddr = 0;
    perfCounter = new PageStorageCounter();
    async init() {
        const lastAddr = await this._getLastAddr();
        if (lastAddr == 0) {
            this.superPage = new SuperPage(this).getDirty(true);
            await this.commit(true);
        } else {
            this.nextAddr = lastAddr;
            let rootAddr = lastAddr - 1;
            while(rootAddr >= 0){
                try {
                    const page = await this.readPage(rootAddr, SuperPage, true);
                    if (!page) {
                        rootAddr--;
                        continue;
                    }
                    this.superPage = page;
                    this.cleanSuperPage = page;
                    this.writtenAddr = page.addr;
                    break;
                } catch (error) {
                    console.error(error);
                    console.info("[RECOVERY] trying read super page from addr " + --rootAddr);
                }
            }
            if (rootAddr < 0) {
                throw new Error("Failed to open database");
            }
        }
    }
    readPage(addr, type, nullOnTypeMismatch = false) {
        const cache1 = this.getCacheForPageType(type);
        const cached = cache1.get(addr);
        if (cached) {
            this.perfCounter.cachedPageReads++;
            return Promise.resolve(cached);
        }
        if (addr < 0 || addr >= this.nextAddr) {
            throw new Error("Invalid page addr " + addr);
        }
        this.perfCounter.acutalPageReads++;
        const buffer = new Uint8Array(PAGESIZE);
        const promise = this._readPageBuffer(addr, buffer).then(()=>{
            const page = new type(this);
            page.dirty = false;
            page.addr = addr;
            if (nullOnTypeMismatch && page.type != buffer[0]) return null;
            page.readFrom(new Buffer(buffer, 0));
            cache1.set(page.addr, page);
            this.checkCache();
            return page;
        });
        cache1.set(addr, promise);
        return promise;
    }
    getCacheForPageType(type) {
        if (type === DataPage) {
            return this.dataCache;
        } else {
            return this.metaCache;
        }
    }
    getCacheForPage(page) {
        if (Object.getPrototypeOf(page) === DataPage.prototype) {
            return this.dataCache;
        } else {
            return this.metaCache;
        }
    }
    checkCache() {
        this._checkCache(METADATA_CACHE_LIMIT, this.metaCache);
        this._checkCache(DATA_CACHE_LIMIT, this.dataCache);
    }
    _checkCache(limit, cache2) {
        const cleanCacheSize = cache2.size - (this.nextAddr - 1 - this.writtenAddr);
        if (limit > 0 && cleanCacheSize > limit) {
            let deleteCount = cleanCacheSize - limit * 3 / 4;
            let deleted = 0;
            for (const page of cache2.valuesFromOldest()){
                if (page instanceof Page && page.addr <= this.writtenAddr) {
                    this.perfCounter.cacheCleans++;
                    cache2.delete(page.addr);
                    if (++deleted == deleteCount) break;
                }
            }
        }
    }
    addDirty(page) {
        if (page.hasAddr) {
            if (page.dirty) {
                console.info("re-added dirty", page.type, page.addr);
                return;
            } else {
                throw new Error("Can't mark on-disk page as dirty");
            }
        }
        page.addr = this.nextAddr++;
        this.dirtyPages.push(page);
        this.getCacheForPage(page).set(page.addr, page);
    }
    addData(val) {
        this.perfCounter.dataAdds++;
        if (!this.dataPage || this.dataPage.freeBytes == 0) {
            this.createDataPage(false);
        }
        const valLength = val.byteLength;
        const headerLength = Buffer.calcEncodedUintSize(valLength);
        const totalLength = headerLength + valLength;
        let pageAddr;
        let offset;
        if (this.dataPage.freeBytes >= totalLength) {
            pageAddr = this.dataPage.addr;
            offset = this.dataPageBuffer.pos;
            this.dataPageBuffer.writeEncodedUint(valLength);
            val.writeTo(this.dataPageBuffer);
            this.dataPage.freeBytes -= totalLength;
        } else {
            if (this.dataPage.freeBytes < headerLength) {
                this.createDataPage(false);
            }
            pageAddr = this.dataPage.addr;
            offset = this.dataPageBuffer.pos;
            this.dataPageBuffer.writeEncodedUint(valLength);
            this.dataPage.freeBytes -= headerLength;
            const valBuffer = new Buffer(new Uint8Array(valLength), 0);
            val.writeTo(valBuffer);
            let written = 0;
            while(written < valLength){
                if (this.dataPage.freeBytes == 0) {
                    this.createDataPage(true);
                }
                const toWrite = Math.min(valLength - written, this.dataPage.freeBytes);
                this.dataPageBuffer.writeBuffer(valBuffer.buffer.subarray(written, written + toWrite));
                written += toWrite;
                this.dataPage.freeBytes -= toWrite;
            }
        }
        return new PageOffsetValue(pageAddr, offset);
    }
    async readData(pageOffset, type) {
        this.perfCounter.dataReads++;
        let page = await this.readPage(pageOffset.addr, DataPage);
        let buffer = new Buffer(page.buffer, pageOffset.offset);
        const valLength = buffer.readEncodedUint();
        let bufferLeft = buffer.buffer.length - buffer.pos;
        if (valLength <= bufferLeft) {
            return type ? type.readFrom(buffer) : buffer.buffer.subarray(buffer.pos, valLength);
        } else {
            const valBuffer = new Buffer(new Uint8Array(valLength), 0);
            while(valBuffer.pos < valLength){
                if (bufferLeft == 0) {
                    if (!page.next) throw new BugError("BUG: expected next page.");
                    page = await this.readPage(page.next, DataPage);
                    buffer = new Buffer(page.buffer, 0);
                    bufferLeft = buffer.buffer.length;
                }
                const toRead = Math.min(bufferLeft, valLength - valBuffer.pos);
                valBuffer.writeBuffer(buffer.pos || buffer.buffer.length != toRead ? buffer.buffer.subarray(buffer.pos, buffer.pos + toRead) : buffer.buffer);
                bufferLeft -= toRead;
            }
            valBuffer.pos = 0;
            return type ? type.readFrom(valBuffer) : valBuffer.buffer;
        }
    }
    createDataPage(continued) {
        const prev = this.dataPage;
        this.dataPage = new DataPage(this);
        this.dataPage.createBuffer();
        this.dataPageBuffer = new Buffer(this.dataPage.buffer, 0);
        this.addDirty(this.dataPage);
        if (continued) prev.next = this.dataPage.addr;
    }
    async commitMark() {
        if (!this.superPage) throw new Error("superPage does not exist.");
        if (this.dirtySets.length) {
            const rootTree = new Node(this.superPage);
            for (const set of this.dirtySets){
                if (set.hasNewerCopy()) {
                    console.info(this.dirtySets.map((x)=>[
                            x.addr,
                            x.prefixedName
                        ]
                    ));
                    console.info("dirtySets length", this.dirtySets.length);
                    throw new Error("non-latest page in dirtySets");
                }
                set.getDirty(true);
                try {
                    await rootTree.set(new KeyComparator(new StringValue(set.prefixedName)), new KValue(new StringValue(set.prefixedName), new UIntValue(set.addr)), "change-only");
                } catch (error) {
                    if (error instanceof NotExistError) {
                        continue;
                    }
                    throw error;
                }
            }
            this.dirtySets = [];
        }
        if (!this.superPage.dirty) {
            if (this.dirtyPages.length == 0) {
                return [];
            } else {
                throw new Error("super page is not dirty");
            }
        }
        this.dataPage = undefined;
        this.dataPageBuffer = undefined;
        if (this.cleanSuperPage) {
            this.superPage.prevSuperPageAddr = this.cleanSuperPage.addr;
        }
        this.addDirty(this.superPage);
        for (const page of this.dirtyPages){
            page.dirty = false;
        }
        const currentDirtyPages = this.dirtyPages;
        this.dirtyPages = [];
        this.cleanSuperPage = this.superPage;
        return currentDirtyPages;
    }
    async commit(waitWriting) {
        const pages = await this.commitMark();
        this.deferWritingQueue.enqueue({
            run: ()=>{
                return this._commit(pages);
            }
        });
        if (waitWriting) {
            await this.waitDeferWriting();
        }
        return pages.length > 0;
    }
    rollback() {
        if (this.superPage.dirty) {
            this.metaCache.delete(this.superPage.addr);
            this.superPage = this.cleanSuperPage;
            this.cleanSuperPage._newerCopy = null;
        }
        if (this.dirtySets.length > 0) {
            for (const page of this.dirtySets){
                page._discard = true;
            }
            this.dirtySets = [];
        }
        if (this.dirtyPages.length > 0) {
            for (const page of this.dirtyPages){
                page._discard = true;
                if (Object.getPrototypeOf(page) == DataPage.prototype) {
                    this.dataCache.delete(page.addr);
                } else {
                    this.metaCache.delete(page.addr);
                }
            }
            this.dirtyPages = [];
            this.nextAddr = this.cleanAddr + 1;
        }
        this.dataPage = undefined;
        this.dataPageBuffer = undefined;
    }
    waitDeferWriting() {
        return this.deferWritingQueue.waitCurrentLastTask();
    }
    close() {
        if (this.deferWritingQueue.running) {
            throw new Error("Some deferred writing tasks are still running. " + "Please `await waitDeferWriting()` before closing.");
        }
        this._close();
    }
}
class InFileStorage extends PageStorage {
    file = undefined;
    filePath = undefined;
    lock = new OneWriterLock();
    commitBuffer = new Buffer(new Uint8Array(PAGESIZE * 32), 0);
    fsync = "final-only";
    async openPath(path) {
        if (this.file) throw new Error("Already opened a file.");
        this.file = await Runtime.open(path, {
            read: true,
            write: true,
            create: true
        });
        this.filePath = path;
    }
    async _readPageBuffer(addr, buffer) {
        await this.lock.enterWriter();
        await this.file.seek(addr * PAGESIZE, Runtime.SeekMode.Start);
        for(let i18 = 0; i18 < PAGESIZE;){
            const nread = await this.file.read(buffer.subarray(i18));
            if (nread === null) throw new Error("Unexpected EOF");
            i18 += nread;
        }
        this.lock.exitWriter();
    }
    async _commit(pages) {
        await this.lock.enterWriter();
        const buffer = this.commitBuffer;
        const pagesLen = pages.length;
        let filePos = -1;
        for(let i19 = 0; i19 < pagesLen; i19++){
            const beginAddr = pages[i19].addr;
            const beginI = i19;
            let combined = 1;
            while(i19 + 2 < pagesLen && pages[i19 + 1].addr === beginAddr + combined && combined < 32){
                i19++;
                combined++;
            }
            for(let p = 0; p < combined; p++){
                buffer.pos = p * PAGESIZE;
                const page = pages[beginI + p];
                page.writeTo(buffer);
                this.perfCounter.pageWrites++;
                this.perfCounter.pageFreebyteWrites += page.freeBytes;
            }
            const targerPos = beginAddr * PAGESIZE;
            if (filePos !== targerPos) {
                await this.file.seek(targerPos, Runtime.SeekMode.Start);
            }
            const toWrite = combined * PAGESIZE;
            for(let i110 = 0; i110 < toWrite;){
                const nwrite = await this.file.write(buffer.buffer.subarray(i110, toWrite));
                if (nwrite <= 0) {
                    throw new Error("Unexpected return value of write(): " + nwrite);
                }
                i110 += nwrite;
            }
            filePos = targerPos + toWrite;
            buffer.buffer.set(InFileStorage.emptyBuffer.subarray(0, toWrite), 0);
            buffer.pos = 0;
            this.writtenAddr = beginAddr + combined - 1;
            if (i19 % TOTAL_CACHE_LIMIT === TOTAL_CACHE_LIMIT - 1) {
                this.checkCache();
            }
            if (i19 === pagesLen - 2 && this.fsync && this.fsync !== "final-only") {
                await Runtime.fdatasync(this.file.rid);
            }
        }
        if (this.fsync) {
            await Runtime.fdatasync(this.file.rid);
        }
        this.lock.exitWriter();
    }
    async _getLastAddr() {
        return Math.floor(await this.file.seek(0, Runtime.SeekMode.End) / PAGESIZE);
    }
    _close() {
        this.file.close();
    }
    static emptyBuffer = new Uint8Array(PAGESIZE * 32);
}
class InMemoryData {
    pageBuffers = [];
}
class InMemoryStorage extends PageStorage {
    data;
    constructor(data){
        super();
        this.data = data;
    }
    async _commit(pages) {
        var buf = new Buffer(null, 0);
        for (const page of pages){
            buf.buffer = new Uint8Array(PAGESIZE);
            buf.pos = 0;
            page.writeTo(buf);
            this.data.pageBuffers.push(buf.buffer);
            this.perfCounter.pageWrites++;
            this.perfCounter.pageFreebyteWrites += page.freeBytes;
        }
    }
    async _readPageBuffer(addr, buffer) {
        buffer.set(this.data.pageBuffers[addr]);
    }
    _getLastAddr() {
        return Promise.resolve(this.data.pageBuffers.length);
    }
    _close() {}
}
class TransactionService {
    db;
    constructor(db){
        this.db = db;
    }
    debug = false;
    txn = 0;
    maxConcurrent = 10;
    running = 0;
    waitingForCommit = 0;
    needReplaying = false;
    blockingNew = null;
    cycleCompleted = null;
    async run(fn) {
        const txn = ++this.txn;
        if (this.debug) console.info("txn", txn, "new transaction");
        if (this.blockingNew) {
            if (this.debug) console.info("txn", txn, "blocking");
            do {
                await this.blockingNew;
            }while (this.blockingNew)
        }
        let replaying = false;
        let returnValue = undefined;
        while(true){
            this.running++;
            if (this.running + this.waitingForCommit >= this.maxConcurrent) {
                if (this.debug) {
                    console.info("txn", txn, "maxConcurrent start blocking");
                }
                if (!this.blockingNew) this.blockingNew = deferred();
            }
            if (replaying && this.debug) console.info("txn", txn, "replaying");
            if (this.running == 2) this.cycleCompleted = deferred();
            try {
                returnValue = await fn({
                    db: this.db,
                    replaying
                });
            } catch (e) {
                if (this.debug) console.info("txn", txn, "error running");
                this.running--;
                if (this.running == 0) {
                    if (this.waitingForCommit) {
                        if (this.debug) console.info("txn", txn, "[start replay]");
                        if (!this.blockingNew) this.blockingNew = deferred();
                        this.startReplay();
                    }
                } else {
                    this.needReplaying = true;
                }
                throw e;
            }
            this.waitingForCommit++;
            this.running--;
            if (this.debug) {
                console.info("txn", txn, "finish, running =", this.running, "waiting =", this.waitingForCommit);
            }
            if (this.running == 0) {
                if (!this.blockingNew) this.blockingNew = deferred();
                if (this.needReplaying) {
                    if (this.debug) console.info("txn", txn, "[start replay]");
                    await this.startReplay();
                } else {
                    if (this.debug) console.info("txn", txn, "[start commit]");
                    try {
                        await this.db.commit();
                    } catch (error) {
                        if (this.debug) console.info("txn", txn, "[commit error]");
                        this.cycleCompleted?.reject(error);
                        throw error;
                    }
                    this.waitingForCommit = 0;
                    this.cycleCompleted?.resolve(true);
                    this.cycleCompleted = null;
                    this.blockingNew.resolve();
                    this.blockingNew = null;
                    if (this.debug) console.info("txn", txn, "[comitted]");
                    break;
                }
            } else {
                if (await this.cycleCompleted) {
                    break;
                }
            }
            replaying = true;
        }
        return returnValue;
    }
    async startReplay() {
        await this.db.rollback();
        this.cycleCompleted?.resolve(false);
        this.cycleCompleted = null;
        this.waitingForCommit = 0;
        this.needReplaying = false;
    }
}
const _setTypeInfo = {
    kv: {
        page: SetPage,
        dbset: DbSet
    },
    doc: {
        page: DocSetPage,
        dbset: DbDocSet
    }
};
class DatabaseEngine {
    storage = undefined;
    transaction = new TransactionService(this);
    snapshot = null;
    autoCommit = false;
    autoCommitWaitWriting = true;
    defaultWaitWriting = true;
    commitLock = new OneWriterLock();
    get superPage() {
        return this.snapshot || this.storage.superPage;
    }
    getTree() {
        return new Node(this.superPage);
    }
    async openFile(path, options) {
        const stor = new InFileStorage();
        if (options) Object.assign(stor, options);
        await stor.openPath(path);
        await stor.init();
        this.storage = stor;
    }
    async openMemory(data) {
        const stor = new InMemoryStorage(data ?? new InMemoryData());
        await stor.init();
        this.storage = stor;
    }
    static async openFile(...args) {
        const db = new DatabaseEngine();
        await db.openFile(...args);
        return db;
    }
    static async openMemory(data) {
        const db = new DatabaseEngine();
        await db.openMemory(data);
        return db;
    }
    async createSet(name, type = "kv") {
        let lockWriter = false;
        const lock = this.commitLock;
        await lock.enterReader();
        try {
            let set = await this._getSet(name, type, false);
            if (set) return set;
            await lock.enterWriterFromReader();
            lockWriter = true;
            set = await this._getSet(name, type, false);
            if (set) return set;
            const prefixedName = this._getPrefixedName(type, name);
            const { dbset: Ctordbset , page: Ctorpage  } = _setTypeInfo[type];
            const setPage = new Ctorpage(this.storage).getDirty(true);
            setPage.prefixedName = prefixedName;
            const keyv = new StringValue(prefixedName);
            await this.getTree().set(new KeyComparator(keyv), new KValue(keyv, new UIntValue(setPage.addr)), "no-change");
            this.superPage.setCount++;
            if (this.autoCommit) await this._autoCommit();
            return new Ctordbset(setPage, this, name, !!this.snapshot);
        } finally{
            if (lockWriter) lock.exitWriter();
            else lock.exitReader();
        }
    }
    getSet(name, type = "kv") {
        if (type == "snapshot") {
            throw new Error("Cannot call getSet() with type 'snapshot'");
        }
        return this._getSet(name, type, true);
    }
    async _getSet(name, type, useLock) {
        const lock = this.commitLock;
        if (useLock) await lock.enterReader();
        try {
            const prefixedName = this._getPrefixedName(type, name);
            const r = await this.getTree().findKeyRecursive(new KeyComparator(new StringValue(prefixedName)));
            if (!r.found) return null;
            const { dbset: Ctordbset , page: Ctorpage  } = _setTypeInfo[type];
            const setPage = await this.storage.readPage(r.val.value.val, Ctorpage);
            setPage.prefixedName = prefixedName;
            return new Ctordbset(setPage, this, name, !!this.snapshot);
        } finally{
            if (useLock) lock.exitReader();
        }
    }
    async deleteSet(name, type) {
        return await this.deleteObject(name, type);
    }
    async deleteObject(name, type) {
        const lock = this.commitLock;
        await lock.enterWriter();
        try {
            const prefixedName = this._getPrefixedName(type, name);
            const { action  } = await this.getTree().set(new KeyComparator(new StringValue(prefixedName)), null, "no-change");
            if (action == "removed" && type != "snapshot") {
                this.superPage.setCount--;
                if (this.autoCommit) await this._autoCommit();
                return true;
            } else if (action == "noop") {
                return false;
            } else {
                throw new BugError("Unexpected return value: " + action);
            }
        } finally{
            lock.exitWriter();
        }
    }
    async getSetCount() {
        return this.superPage.setCount;
    }
    async getObjects() {
        const lock = this.commitLock;
        await lock.enterReader();
        try {
            return await this._getObjectsNoLock();
        } finally{
            lock.exitReader();
        }
    }
    async _getObjectsNoLock() {
        return (await this.getTree().getAllValues()).map((x)=>{
            return this._parsePrefixedName(x.key.str);
        });
    }
    async createSnapshot(name, overwrite = false) {
        const lock = this.commitLock;
        await lock.enterWriter();
        try {
            await this._autoCommit();
            const prefixedName = "s_" + name;
            const kv = new KValue(new StringValue(prefixedName), new UIntValue(this.storage.cleanSuperPage.addr));
            await this.getTree().set(new KeyComparator(kv.key), kv, overwrite ? "can-change" : "no-change");
            if (this.autoCommit) await this._autoCommit();
        } finally{
            lock.exitWriter();
        }
    }
    async getSnapshot(name) {
        const lock = this.commitLock;
        await lock.enterReader();
        try {
            const prefixedName = "s_" + name;
            const result = await this.getTree().findKeyRecursive(new KeyComparator(new StringValue(prefixedName)));
            if (!result.found) return null;
            return await this._getSnapshotByAddr(result.val.value.val);
        } finally{
            lock.exitReader();
        }
    }
    runTransaction(fn) {
        return this.transaction.run(fn);
    }
    async commit(waitWriting) {
        await this.commitLock.enterWriter();
        try {
            return await this._commitNoLock(waitWriting ?? this.defaultWaitWriting);
        } finally{
            this.commitLock.exitWriter();
        }
    }
    async _commitNoLock(waitWriting) {
        const r = await this.storage.commit(waitWriting);
        return r;
    }
    _autoCommit() {
        return this._commitNoLock(this.autoCommitWaitWriting);
    }
    async getPrevCommit() {
        if (!this.superPage?.prevSuperPageAddr) return null;
        return await this._getSnapshotByAddr(this.superPage.prevSuperPageAddr);
    }
    async _getSnapshotByAddr(addr) {
        var snapshot = new DatabaseEngine();
        snapshot.storage = this.storage;
        snapshot.snapshot = await this.storage.readPage(addr, SuperPage);
        return snapshot;
    }
    _getPrefixedName(type, name) {
        const prefix = type == "kv" ? "k" : type == "doc" ? "d" : type == "snapshot" ? "s" : null;
        if (!prefix) throw new Error("Unknown type '" + type + "'");
        return prefix + "_" + name;
    }
    _parsePrefixedName(prefixedName) {
        const prefix = prefixedName[0];
        if (prefixedName[1] != "_") {
            throw new Error("Unexpected prefixedName '" + prefixedName + "'");
        }
        const type = prefix == "k" ? "kv" : prefix == "d" ? "doc" : prefix == "s" ? "snapshot" : null;
        if (!type) throw new Error("Unknown prefix '" + prefix + "'");
        return {
            type,
            name: prefixedName.substr(2)
        };
    }
    waitWriting() {
        return this.storage.waitDeferWriting();
    }
    async rollback() {
        await this.commitLock.enterWriter();
        try {
            this.storage.rollback();
        } finally{
            this.commitLock.exitWriter();
        }
    }
    close() {
        this.storage.close();
    }
    async _cloneToNoLock(other) {
        const sets = (await this._getObjectsNoLock()).filter((x)=>x.type != "snapshot"
        );
        for (const { name , type  } of sets){
            const oldSet = await this._getSet(name, type, false);
            const newSet = await other.createSet(name, type);
            await oldSet._cloneTo(newSet);
        }
    }
    async rebuild() {
        let lockWriter = false;
        await this.commitLock.enterReader();
        try {
            if (this.storage instanceof InFileStorage) {
                const dbPath = this.storage.filePath;
                const tempPath = dbPath + ".tmp";
                try {
                    await Runtime.remove(tempPath);
                } catch  {}
                const tempdb = await DatabaseEngine.openFile(tempPath);
                await this._cloneToNoLock(tempdb);
                await tempdb.commit(true);
                await this.waitWriting();
                await this.commitLock.enterWriterFromReader();
                lockWriter = true;
                this.storage.close();
                await Runtime.rename(tempPath, dbPath);
                this.storage = tempdb.storage;
            } else if (this.storage instanceof InMemoryStorage) {
                const tempData = new InMemoryData();
                const tempdb = await DatabaseEngine.openMemory(tempData);
                await this._cloneToNoLock(tempdb);
                await tempdb.commit(true);
                await this.waitWriting();
                await this.commitLock.enterWriterFromReader();
                lockWriter = true;
                this.storage.close();
                this.storage.data.pageBuffers = tempData.pageBuffers;
                this.storage = tempdb.storage;
            }
        } finally{
            if (lockWriter) this.commitLock.exitWriter();
            else this.commitLock.exitReader();
        }
    }
    async dump() {
        const obj = {
            btrdbDumpVersion: "0",
            sets: []
        };
        for (const { type , name  } of (await this.getObjects())){
            if (type == "snapshot") {} else if (type == "kv") {
                const set = await this.getSet(name, "kv");
                obj.sets.push({
                    type,
                    name,
                    kvs: await set.getAll()
                });
            } else if (type == "doc") {
                const set = await this.getSet(name, "doc");
                obj.sets.push({
                    type,
                    name,
                    indexes: await set.getIndexes(),
                    docs: await set.getAll()
                });
            } else {
                throw new Error(`Unknown type '${type}'`);
            }
        }
        return JSON.stringify(obj);
    }
    async import(data) {
        const obj = JSON.parse(data);
        if (obj.btrdbDumpVersion != "0") {
            throw new Error(`Unknown version '${obj.btrdbDumpVersion}'`);
        }
        for (const setData of obj.sets){
            if (setData.type == "doc") {
                const set = await this.createSet(setData.name, "doc");
                for (const idx of Object.values(setData.indexes)){
                    idx.key = (0, eval)(idx.key);
                }
                await set.useIndexes(setData.indexes);
                for (const doc of setData.docs){
                    await set.insert(doc);
                }
            } else if (setData.type == "kv") {
                const set = await this.createSet(setData.name, "kv");
                for (const kv of setData.kvs){
                    await set.set(kv.key, kv.value);
                }
            }
        }
    }
}
const Database = DatabaseEngine;
const cache = globalThis.WeakMap ? new WeakMap() : null;
function query(plainText, ...args) {
    let ast = cache?.get(plainText);
    if (!ast) {
        ast = new Parser(plainText).parseExpr().optimize();
        cache?.set(plainText, ast);
    }
    return ast.compute(args);
}
const Operators = {
    "==": {
        func: EQ,
        bin: true,
        prec: 2,
        type: "name-value"
    },
    "!=": {
        func: NE,
        bin: true,
        prec: 2,
        type: "name-value"
    },
    "<": {
        func: LT,
        bin: true,
        prec: 2,
        type: "name-value"
    },
    ">": {
        func: GT,
        bin: true,
        prec: 2,
        type: "name-value"
    },
    "<=": {
        func: LE,
        bin: true,
        prec: 2,
        type: "name-value"
    },
    ">=": {
        func: GE,
        bin: true,
        prec: 2,
        type: "name-value"
    },
    "NOT": {
        func: NOT,
        bin: false,
        prec: 1,
        type: "bool"
    },
    "AND": {
        func: AND,
        bin: true,
        prec: 0,
        type: "bool"
    },
    "OR": {
        func: OR,
        bin: true,
        prec: 0,
        type: "bool"
    }
};
const hasOwnProperty = Object.prototype.hasOwnProperty;
class Parser {
    constructor(plainText){
        this.gen = this.generator(plainText);
    }
    gen;
    buffer = [];
    peek() {
        this.ensure(0);
        return this.buffer[0];
    }
    consume(type) {
        this.ensure(0);
        return this.buffer.shift();
    }
    expect(type) {
        if (!this.tryExpect(type)) throw new Error("Expected token type " + type);
    }
    expectAndConsume(type) {
        if (!this.tryExpect(type)) throw new Error("Expected token type " + type);
        else return this.consume();
    }
    tryExpect(type) {
        return this.peek().type === type;
    }
    tryExpectAndConsume(type) {
        if (this.tryExpect(type)) {
            return this.consume();
        }
        return null;
    }
    ensure(pos) {
        while(pos >= this.buffer.length){
            const result = this.gen.next();
            if (result.done) {
                this.buffer.push({
                    type: "end"
                });
            } else {
                this.buffer.push(result.value);
            }
        }
    }
    *generator(plainText) {
        const re = /\s*(\w+|\(\)|[!<>=]+|[()])(\s*|$)/ym;
        for(let i20 = 0; i20 < plainText.length; i20++){
            const str = plainText[i20];
            while(true){
                const match = re.exec(str);
                if (!match) break;
                const word = match[1];
                if (hasOwnProperty.call(Operators, word)) {
                    yield {
                        type: "op",
                        str: word,
                        op: Operators[word]
                    };
                } else if (word === "(" || word === ")") {
                    yield {
                        type: word
                    };
                } else {
                    yield {
                        type: "name",
                        value: word
                    };
                }
            }
            if (i20 < plainText.length - 1) {
                yield {
                    type: "arg",
                    value: i20
                };
            }
        }
    }
    parseExpr() {
        return this.parseBinOp(this.parseValue(), 0);
    }
    parseValue() {
        if (this.tryExpect("name")) {
            return new NameAST(this.consume("name").value);
        } else if (this.tryExpect("arg")) {
            return new ArgAST(this.consume("arg").value);
        } else if (this.tryExpectAndConsume("(")) {
            const ast = this.parseExpr();
            this.expectAndConsume(")");
            return ast;
        } else if (this.tryExpect("op")) {
            const op = this.consume("op");
            const value = this.parseBinOp(this.parseValue(), op.op.prec);
            return new OpAST(op.op, [
                value
            ]);
        } else {
            throw new Error("Expected a value");
        }
    }
    parseBinOp(left, minPrec) {
        while(true){
            const t = this.peek();
            if (t.type !== "op" || !t.op.bin || t.op.prec < minPrec) break;
            this.consume();
            let right = this.parseValue();
            while(true){
                const nextop = this.peek();
                if (nextop.type !== "op" || !t.op.bin || nextop.op.prec <= t.op.prec) {
                    break;
                }
                right = this.parseBinOp(right, nextop.op.prec);
            }
            left = new OpAST(t.op, [
                left,
                right
            ]);
        }
        return left;
    }
}
class AST {
}
class ArgAST extends AST {
    argPos;
    constructor(argPos){
        super();
        this.argPos = argPos;
    }
    compute(args) {
        return args[this.argPos];
    }
    optimize() {
        return this;
    }
}
class NameAST extends AST {
    name;
    constructor(name){
        super();
        this.name = name;
    }
    compute(args) {
        return this.name;
    }
    optimize() {
        return this;
    }
}
class OpAST extends AST {
    op;
    children;
    constructor(op, children){
        super();
        this.op = op;
        this.children = children;
    }
    compute(args) {
        return this.op.func(...this.children.map((x)=>x.compute(args)
        ));
    }
    optimize() {
        let optimizedChildren = this.children.map((x)=>x.optimize()
        );
        if (this.op.func === AND || this.op.func === OR) {
            const first = optimizedChildren[0];
            if (first instanceof OpAST && first.op.func === this.op.func) {
                optimizedChildren.splice(0, 1);
                optimizedChildren = [
                    ...first.children,
                    ...optimizedChildren
                ];
            }
        }
        if (this.op.type === "name-value") {
            if (optimizedChildren.length !== 2) {
                throw new Error("Wrong count of operands");
            }
            if (optimizedChildren[0] instanceof NameAST && optimizedChildren[1] instanceof ArgAST) {} else if (optimizedChildren[0] instanceof ArgAST && optimizedChildren[1] instanceof NameAST) {
                [optimizedChildren[1], optimizedChildren[0]] = [
                    optimizedChildren[0],
                    optimizedChildren[1], 
                ];
            } else {
                throw new Error("Wrong type of operands");
            }
        }
        return new OpAST(this.op, optimizedChildren);
    }
}
class HttpApiServer {
    db;
    constructor(db){
        this.db = db;
    }
    async serve(listener) {
        for await (const conn of listener){
            this.serveConn(conn);
        }
    }
    async serveConn(conn) {
        const httpConn = Deno.serveHttp(conn);
        for await (const requestEvent of httpConn){
            await this.serveRequest(requestEvent);
        }
    }
    async serveRequest(event) {
        try {
            const ret = await this.handler(event);
            if (ret === undefined) {
                event.respondWith(new Response(null, {
                    status: 200
                }));
            } else {
                event.respondWith(new Response(JSON.stringify(ret), {
                    headers: {
                        "content-type": "application/json"
                    }
                }));
            }
        } catch (error) {
            if (error instanceof ApiError) {
                event.respondWith(new Response(JSON.stringify({
                    error: error.message
                }), {
                    status: error.statusCode,
                    headers: {
                        "content-type": "application/json"
                    }
                }));
            } else {
                console.error(error);
                event.respondWith(new Response(null, {
                    status: 500
                }));
            }
        }
    }
    async handler(event) {
        const req = event.request;
        const url = new URL(req.url);
        const path = url.pathname.split("/").slice(1, url.pathname.endsWith("/") ? -1 : undefined);
        if (path.length >= 2) {
            const [settype, setname] = decodeSetId(path[1]);
            if (path[0] == "sets") {
                if (path.length === 2 && url.search == "") {
                    if (req.method == "POST") {
                        await this.db.createSet(setname, settype);
                        return;
                    } else if (req.method == "DELETE") {
                        await this.db.deleteSet(setname, settype);
                        return;
                    }
                }
                if (settype == "kv") {
                    if (path[2]) {
                        const key = JSON.parse(decodeURIComponent(path[2]));
                        if (req.method == "GET") {
                            const set = await this.getSet(setname, settype);
                            return await set.get(key);
                        } else if (req.method == "PUT") {
                            const set = await this.getSet(setname, settype);
                            await set.set(key, await req.json());
                            return;
                        } else if (req.method == "DELETE") {
                            const set = await this.getSet(setname, settype);
                            if (!await set.delete(key)) {
                                throw new ApiError(404, `key not found`);
                            }
                            return;
                        }
                    } else if (url.search == "?keys") {
                        const set = await this.getSet(setname, settype);
                        return await set.getKeys();
                    } else if (url.search == "?count") {
                        const set = await this.getSet(setname, settype);
                        return set.count;
                    } else if (url.search == "") {
                        const set = await this.getSet(setname, settype);
                        return await set.getAll();
                    }
                } else if (settype == "doc") {
                    if (path[2]) {
                        const id = JSON.parse(decodeURIComponent(path[2]));
                        if (req.method == "GET") {
                            const set = await this.getSet(setname, settype);
                            return await set.get(id);
                        } else if (req.method == "PUT") {
                            const set = await this.getSet(setname, settype);
                            const doc = await req.json();
                            await set.upsert(doc);
                            return;
                        } else if (req.method == "DELETE") {
                            const set = await this.getSet(setname, settype);
                            if (!await set.delete(id)) {
                                throw new ApiError(404, `key not found`);
                            }
                            return;
                        }
                    } else {
                        if (req.method == "GET") {
                            if (url.searchParams.get("query")) {
                                const set = await this.getSet(setname, settype);
                                const querystr = url.searchParams.get("query");
                                const values = url.searchParams.getAll("value").map((x)=>JSON.parse(x)
                                );
                                const q = query(querystr.split("{}"), ...values);
                                return await set.query(q);
                            } else if (url.search == "?count") {
                                const set = await this.getSet(setname, settype);
                                return set.count;
                            } else if (url.search == "?ids") {
                                const set = await this.getSet(setname, settype);
                                return await set.getIds();
                            }
                        } else if (req.method == "POST") {
                            if (url.search == "?query") {
                                const set = await this.getSet(setname, settype);
                                const { query: querystr , values  } = await req.json();
                                const q = query(querystr.split("{}"), ...values);
                                return await set.query(q);
                            } else if (url.search == "?insert") {
                                const set = await this.getSet(setname, settype);
                                const doc = await req.json();
                                await set.insert(doc);
                                return doc["id"];
                            } else if (url.search == "?indexes") {
                                const set = await this.getSet(setname, settype);
                                const indexes = Object.fromEntries(Object.entries(await req.json()).map(([name, def])=>{
                                    if (typeof def == "string") {
                                        def = propNameToKeySelector(def);
                                    } else {
                                        def = {
                                            key: propNameToKeySelector(def.key),
                                            unique: def.unique || false
                                        };
                                    }
                                    return [
                                        name,
                                        def
                                    ];
                                }));
                                await set.useIndexes(indexes);
                                return;
                            }
                        }
                    }
                }
            }
        } else if (path.length == 1 && path[0] == "objects") {
            return await this.db.getObjects();
        }
        throw new ApiError(400, "Unknown URL");
    }
    async getSet(name, type) {
        const set = await this.db.getSet(name, type);
        if (!set) throw new ApiError(404, `set not found`);
        return set;
    }
}
class ApiError extends Error {
    statusCode;
    constructor(statusCode, msg){
        super(msg);
        this.statusCode = statusCode;
    }
}
function decodeSetId(setid) {
    return setid.split(":", 2);
}
function propNameToKeySelector(name) {
    return (0, eval)(`x => x[${JSON.stringify(name)}]`);
}
const mod = {
    Database: Database,
    InMemoryData: InMemoryData,
    HttpApiServer: HttpApiServer,
    numberIdGenerator: numberIdGenerator,
    AND: AND,
    BETWEEN: BETWEEN,
    EQ: EQ,
    GE: GE,
    GT: GT,
    LE: LE,
    LT: LT,
    NE: NE,
    NOT: NOT,
    OR: OR,
    query: query
};
module.exports = mod;
const util = require("util");
const fs = require("fs");
const fsPromises = require("fs/promises");
const global = globalThis;
if (!global["Deno"]) {
    const Runtime = {};
    setRuntimeImplementaion(Runtime);
    Runtime.inspect = util.inspect;
    let SeekMode1;
    (function(SeekMode2) {
        SeekMode2[SeekMode2["Start"] = 0] = "Start";
        SeekMode2[SeekMode2["Current"] = 1] = "Current";
        SeekMode2[SeekMode2["End"] = 2] = "End";
    })(SeekMode1 || (SeekMode1 = {}));
    Runtime.SeekMode = SeekMode1;
    Runtime.mkdir = fsPromises.mkdir;
    Runtime.test = function() {};
    Runtime.remove = (path)=>fsPromises.rm(path)
    ;
    Runtime.rename = (oldPath, newPath)=>fsPromises.rename(oldPath, newPath)
    ;
    Runtime.writeTextFile = (path, text)=>fsPromises.writeFile(path, text)
    ;
    Runtime.readTextFile = (path)=>fsPromises.readFile(path, "utf-8")
    ;
    class File {
        fh;
        pos = 0;
        get rid() {
            return this.fh.fd;
        }
        constructor(fh){
            this.fh = fh;
        }
        write(p) {
            return this.fh.write(p, 0, p.byteLength, this.pos);
        }
        truncate(len) {
            return this.fh.truncate(len);
        }
        read(p) {
            return this.fh.read(p, 0, p.byteLength, this.pos);
        }
        async seek(offset, whence) {
            if (whence == SeekMode1.Start) {
                this.pos = offset;
            } else if (whence == SeekMode1.Current) {
                this.pos += offset;
            } else {
                this.pos = (await this.stat()).size + offset;
            }
            return this.pos;
        }
        stat() {
            return this.fh.stat();
        }
        close() {
            this.fh.close();
        }
    }
    Runtime.open = async function(path, options) {
        return new File(await fsPromises.open(path, "a+"));
    };
    Runtime.fdatasync = function(fd) {
        return new Promise((resolve, reject)=>{
            fs.fdatasync(fd, (err)=>{
                if (!err) resolve();
                else reject(err);
            });
        });
    };
}
const noColor = globalThis.Deno?.noColor ?? true;
let enabled = !noColor;
function code(open, close) {
    return {
        open: `\x1b[${open.join(";")}m`,
        close: `\x1b[${close}m`,
        regexp: new RegExp(`\\x1b\\[${close}m`, "g")
    };
}
function run(str, code1) {
    return enabled ? `${code1.open}${str.replace(code1.regexp, code1.open)}${code1.close}` : str;
}
function bold(str) {
    return run(str, code([
        1
    ], 22));
}
function red(str) {
    return run(str, code([
        31
    ], 39));
}
function green(str) {
    return run(str, code([
        32
    ], 39));
}
function white(str) {
    return run(str, code([
        37
    ], 39));
}
function gray(str) {
    return brightBlack(str);
}
function brightBlack(str) {
    return run(str, code([
        90
    ], 39));
}
function bgRed(str) {
    return run(str, code([
        41
    ], 49));
}
function bgGreen(str) {
    return run(str, code([
        42
    ], 49));
}
new RegExp([
    "[\\u001B\\u009B][[\\]()#;?]*(?:(?:(?:[a-zA-Z\\d]*(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]*)*)?\\u0007)",
    "(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PR-TZcf-ntqry=><~]))", 
].join("|"), "g");
var DiffType;
(function(DiffType1) {
    DiffType1["removed"] = "removed";
    DiffType1["common"] = "common";
    DiffType1["added"] = "added";
})(DiffType || (DiffType = {}));
const REMOVED = 1;
const COMMON = 2;
const ADDED = 3;
function createCommon(A, B, reverse) {
    const common = [];
    if (A.length === 0 || B.length === 0) return [];
    for(let i22 = 0; i22 < Math.min(A.length, B.length); i22 += 1){
        if (A[reverse ? A.length - i22 - 1 : i22] === B[reverse ? B.length - i22 - 1 : i22]) {
            common.push(A[reverse ? A.length - i22 - 1 : i22]);
        } else {
            return common;
        }
    }
    return common;
}
function diff(A1, B1) {
    const prefixCommon = createCommon(A1, B1);
    const suffixCommon = createCommon(A1.slice(prefixCommon.length), B1.slice(prefixCommon.length), true).reverse();
    A1 = suffixCommon.length ? A1.slice(prefixCommon.length, -suffixCommon.length) : A1.slice(prefixCommon.length);
    B1 = suffixCommon.length ? B1.slice(prefixCommon.length, -suffixCommon.length) : B1.slice(prefixCommon.length);
    const swapped1 = B1.length > A1.length;
    [A1, B1] = swapped1 ? [
        B1,
        A1
    ] : [
        A1,
        B1
    ];
    const M1 = A1.length;
    const N1 = B1.length;
    if (!M1 && !N1 && !suffixCommon.length && !prefixCommon.length) return [];
    if (!N1) {
        return [
            ...prefixCommon.map((c)=>({
                    type: DiffType.common,
                    value: c
                })
            ),
            ...A1.map((a)=>({
                    type: swapped1 ? DiffType.added : DiffType.removed,
                    value: a
                })
            ),
            ...suffixCommon.map((c)=>({
                    type: DiffType.common,
                    value: c
                })
            ), 
        ];
    }
    const offset = N1;
    const delta = M1 - N1;
    const size = M1 + N1 + 1;
    const fp1 = new Array(size).fill({
        y: -1
    });
    const routes = new Uint32Array((M1 * N1 + size + 1) * 2);
    const diffTypesPtrOffset = routes.length / 2;
    let ptr = 0;
    let p = -1;
    function backTrace(A, B, current, swapped) {
        const M = A.length;
        const N = B.length;
        const result = [];
        let a = M - 1;
        let b = N - 1;
        let j = routes[current.id];
        let type = routes[current.id + diffTypesPtrOffset];
        while(true){
            if (!j && !type) break;
            const prev = j;
            if (type === 1) {
                result.unshift({
                    type: swapped ? DiffType.removed : DiffType.added,
                    value: B[b]
                });
                b -= 1;
            } else if (type === 3) {
                result.unshift({
                    type: swapped ? DiffType.added : DiffType.removed,
                    value: A[a]
                });
                a -= 1;
            } else {
                result.unshift({
                    type: DiffType.common,
                    value: A[a]
                });
                a -= 1;
                b -= 1;
            }
            j = routes[prev];
            type = routes[prev + diffTypesPtrOffset];
        }
        return result;
    }
    function createFP(slide, down, k, M) {
        if (slide && slide.y === -1 && down && down.y === -1) {
            return {
                y: 0,
                id: 0
            };
        }
        if (down && down.y === -1 || k === M || (slide && slide.y) > (down && down.y) + 1) {
            const prev = slide.id;
            ptr++;
            routes[ptr] = prev;
            routes[ptr + diffTypesPtrOffset] = ADDED;
            return {
                y: slide.y,
                id: ptr
            };
        } else {
            const prev = down.id;
            ptr++;
            routes[ptr] = prev;
            routes[ptr + diffTypesPtrOffset] = REMOVED;
            return {
                y: down.y + 1,
                id: ptr
            };
        }
    }
    function snake(k, slide, down, _offset, A, B) {
        const M = A.length;
        const N = B.length;
        if (k < -N || M < k) return {
            y: -1,
            id: -1
        };
        const fp = createFP(slide, down, k, M);
        while(fp.y + k < M && fp.y < N && A[fp.y + k] === B[fp.y]){
            const prev = fp.id;
            ptr++;
            fp.id = ptr;
            fp.y += 1;
            routes[ptr] = prev;
            routes[ptr + diffTypesPtrOffset] = COMMON;
        }
        return fp;
    }
    while(fp1[delta + offset].y < N1){
        p = p + 1;
        for(let k = -p; k < delta; ++k){
            fp1[k + offset] = snake(k, fp1[k - 1 + offset], fp1[k + 1 + offset], offset, A1, B1);
        }
        for(let k1 = delta + p; k1 > delta; --k1){
            fp1[k1 + offset] = snake(k1, fp1[k1 - 1 + offset], fp1[k1 + 1 + offset], offset, A1, B1);
        }
        fp1[delta + offset] = snake(delta, fp1[delta - 1 + offset], fp1[delta + 1 + offset], offset, A1, B1);
    }
    return [
        ...prefixCommon.map((c)=>({
                type: DiffType.common,
                value: c
            })
        ),
        ...backTrace(A1, B1, fp1[delta + offset], swapped1),
        ...suffixCommon.map((c)=>({
                type: DiffType.common,
                value: c
            })
        ), 
    ];
}
function diffstr(A, B) {
    function tokenize(string, { wordDiff =false  } = {}) {
        if (wordDiff) {
            const tokens = string.split(/([^\S\r\n]+|[()[\]{}'"\r\n]|\b)/);
            const words = /^[a-zA-Z\u{C0}-\u{FF}\u{D8}-\u{F6}\u{F8}-\u{2C6}\u{2C8}-\u{2D7}\u{2DE}-\u{2FF}\u{1E00}-\u{1EFF}]+$/u;
            for(let i23 = 0; i23 < tokens.length - 1; i23++){
                if (!tokens[i23 + 1] && tokens[i23 + 2] && words.test(tokens[i23]) && words.test(tokens[i23 + 2])) {
                    tokens[i23] += tokens[i23 + 2];
                    tokens.splice(i23 + 1, 2);
                    i23--;
                }
            }
            return tokens.filter((token)=>token
            );
        } else {
            const tokens = [], lines = string.split(/(\n|\r\n)/);
            if (!lines[lines.length - 1]) {
                lines.pop();
            }
            for(let i24 = 0; i24 < lines.length; i24++){
                if (i24 % 2) {
                    tokens[tokens.length - 1] += lines[i24];
                } else {
                    tokens.push(lines[i24]);
                }
            }
            return tokens;
        }
    }
    function createDetails(line, tokens) {
        return tokens.filter(({ type  })=>type === line.type || type === DiffType.common
        ).map((result, i25, t)=>{
            if (result.type === DiffType.common && t[i25 - 1] && t[i25 - 1]?.type === t[i25 + 1]?.type) {
                result.type = t[i25 - 1].type;
            }
            return result;
        });
    }
    const diffResult = diff(tokenize(`${A}\n`), tokenize(`${B}\n`));
    const added = [], removed = [];
    for (const result1 of diffResult){
        if (result1.type === DiffType.added) {
            added.push(result1);
        }
        if (result1.type === DiffType.removed) {
            removed.push(result1);
        }
    }
    const aLines = added.length < removed.length ? added : removed;
    const bLines = aLines === removed ? added : removed;
    for (const a of aLines){
        let tokens = [], b;
        while(bLines.length){
            b = bLines.shift();
            tokens = diff(tokenize(a.value, {
                wordDiff: true
            }), tokenize(b?.value ?? "", {
                wordDiff: true
            }));
            if (tokens.some(({ type , value  })=>type === DiffType.common && value.trim().length
            )) {
                break;
            }
        }
        a.details = createDetails(a, tokens);
        if (b) {
            b.details = createDetails(b, tokens);
        }
    }
    return diffResult;
}
const CAN_NOT_DISPLAY = "[Cannot display]";
class AssertionError extends Error {
    constructor(message){
        super(message);
        this.name = "AssertionError";
    }
}
function _format(v) {
    return globalThis.Deno ? Deno.inspect(v, {
        depth: Infinity,
        sorted: true,
        trailingComma: true,
        compact: false,
        iterableLimit: Infinity
    }) : `"${String(v).replace(/(?=["\\])/g, "\\")}"`;
}
function createColor(diffType, { background =false  } = {}) {
    switch(diffType){
        case DiffType.added:
            return (s)=>background ? bgGreen(white(s)) : green(bold(s))
            ;
        case DiffType.removed:
            return (s)=>background ? bgRed(white(s)) : red(bold(s))
            ;
        default:
            return white;
    }
}
function createSign(diffType) {
    switch(diffType){
        case DiffType.added:
            return "+   ";
        case DiffType.removed:
            return "-   ";
        default:
            return "    ";
    }
}
function buildMessage(diffResult, { stringDiff =false  } = {}) {
    const messages = [], diffMessages = [];
    messages.push("");
    messages.push("");
    messages.push(`    ${gray(bold("[Diff]"))} ${red(bold("Actual"))} / ${green(bold("Expected"))}`);
    messages.push("");
    messages.push("");
    diffResult.forEach((result)=>{
        const c = createColor(result.type);
        const line = result.details?.map((detail)=>detail.type !== DiffType.common ? createColor(detail.type, {
                background: true
            })(detail.value) : detail.value
        ).join("") ?? result.value;
        diffMessages.push(c(`${createSign(result.type)}${line}`));
    });
    messages.push(...stringDiff ? [
        diffMessages.join("")
    ] : diffMessages);
    messages.push("");
    return messages;
}
function isKeyedCollection(x) {
    return [
        Symbol.iterator,
        "size"
    ].every((k)=>k in x
    );
}
function equal(c, d) {
    const seen = new Map();
    return (function compare(a, b) {
        if (a && b && (a instanceof RegExp && b instanceof RegExp || a instanceof URL && b instanceof URL)) {
            return String(a) === String(b);
        }
        if (a instanceof Date && b instanceof Date) {
            const aTime = a.getTime();
            const bTime = b.getTime();
            if (Number.isNaN(aTime) && Number.isNaN(bTime)) {
                return true;
            }
            return a.getTime() === b.getTime();
        }
        if (Object.is(a, b)) {
            return true;
        }
        if (a && typeof a === "object" && b && typeof b === "object") {
            if (a instanceof WeakMap || b instanceof WeakMap) {
                if (!(a instanceof WeakMap && b instanceof WeakMap)) return false;
                throw new TypeError("cannot compare WeakMap instances");
            }
            if (a instanceof WeakSet || b instanceof WeakSet) {
                if (!(a instanceof WeakSet && b instanceof WeakSet)) return false;
                throw new TypeError("cannot compare WeakSet instances");
            }
            if (seen.get(a) === b) {
                return true;
            }
            if (Object.keys(a || {}).length !== Object.keys(b || {}).length) {
                return false;
            }
            if (isKeyedCollection(a) && isKeyedCollection(b)) {
                if (a.size !== b.size) {
                    return false;
                }
                let unmatchedEntries = a.size;
                for (const [aKey, aValue] of a.entries()){
                    for (const [bKey, bValue] of b.entries()){
                        if (aKey === aValue && bKey === bValue && compare(aKey, bKey) || compare(aKey, bKey) && compare(aValue, bValue)) {
                            unmatchedEntries--;
                        }
                    }
                }
                return unmatchedEntries === 0;
            }
            const merged = {
                ...a,
                ...b
            };
            for (const key of [
                ...Object.getOwnPropertyNames(merged),
                ...Object.getOwnPropertySymbols(merged), 
            ]){
                if (!compare(a && a[key], b && b[key])) {
                    return false;
                }
                if (key in a && !(key in b) || key in b && !(key in a)) {
                    return false;
                }
            }
            seen.set(a, b);
            if (a instanceof WeakRef || b instanceof WeakRef) {
                if (!(a instanceof WeakRef && b instanceof WeakRef)) return false;
                return compare(a.deref(), b.deref());
            }
            return true;
        }
        return false;
    })(c, d);
}
function assert(expr, msg = "") {
    if (!expr) {
        throw new AssertionError(msg);
    }
}
function assertEquals(actual, expected, msg) {
    if (equal(actual, expected)) {
        return;
    }
    let message = "";
    const actualString = _format(actual);
    const expectedString = _format(expected);
    try {
        const stringDiff = typeof actual === "string" && typeof expected === "string";
        const diffResult = stringDiff ? diffstr(actual, expected) : diff(actualString.split("\n"), expectedString.split("\n"));
        const diffMsg = buildMessage(diffResult, {
            stringDiff
        }).join("\n");
        message = `Values are not equal:\n${diffMsg}`;
    } catch  {
        message = `\n${red(CAN_NOT_DISPLAY)} + \n\n`;
    }
    if (msg) {
        message = msg;
    }
    throw new AssertionError(message);
}
const testFile = "testdata/testdb.db";
const inmemory = globalThis.Deno?.args?.includes("--in-memory");
const memoryData = new InMemoryData();
const databaseTests = [];
if (true) {
    Runtime.test({
        fn: recreateDatabase,
        name: "recreate database"
    });
}
async function recreateDatabase() {
    await Runtime.mkdir("testdata", {
        recursive: true
    });
    try {
        await Runtime.remove(testFile);
    } catch  {}
}
function dumpObjectToFile(file, obj) {
    const inspectOptions = {
        colors: false,
        iterableLimit: 100000,
        depth: 10,
        compact: false,
        trailingComma: true
    };
    return Runtime.writeTextFile(file, Runtime.inspect(obj, inspectOptions));
}
function runWithDatabase(func, only) {
    Runtime.test({
        name: (inmemory ? "(in memory) " : "") + func.name,
        fn: ()=>runDbTest(func)
        ,
        only: only === true,
        ignore: only === "ignore"
    });
    databaseTests.push({
        func,
        only
    });
}
async function runDbTest(func) {
    console.time("open");
    const db = new Database();
    if (inmemory) {
        await db.openMemory(memoryData);
    } else {
        await db.openFile(testFile, {
            fsync: false
        });
    }
    console.timeEnd("open");
    console.time("run");
    await func(db);
    console.timeEnd("run");
    db.close();
    if (!inmemory) {
        const file = await Runtime.open(testFile);
        const size = (await file.stat()).size;
        file.close();
        console.info("file size:", size, `(${size / PAGESIZE} pages)`);
    }
    const counter = db.storage.perfCounter;
    if (counter.pageWrites) {
        console.info("pageWrites:", counter.pageWrites, "space efficient:", (1 - counter.pageFreebyteWrites / (counter.pageWrites * PAGESIZE)).toFixed(3));
    }
    console.info("acutalReads:", counter.acutalPageReads, "cachedReads:", counter.cachedPageReads);
    console.info("dataReads:", counter.dataReads, "dataAdds:", counter.dataAdds);
    if (counter.cacheCleans) console.info("cacheCleans:", counter.cacheCleans);
}
async function run1() {
    if (true) {
        await recreateDatabase();
    }
    const useOnly = databaseTests.filter((x)=>x.only === true
    ).length > 0;
    let total = databaseTests.length, passed = 0, failed = 0, ignored = 0;
    for (const { func , only  } of databaseTests){
        if (only != "ignore" && (!useOnly || only)) {
            console.info("");
            console.info("=============================");
            console.info("==> test " + (inmemory ? "(in memory) " : "") + func.name);
            console.info("=============================");
            try {
                await runDbTest(func);
                passed++;
            } catch (error) {
                console.error("error in test", error);
                failed++;
            }
        } else {
            ignored++;
        }
    }
    const stat = {
        total,
        passed,
        failed,
        ignored
    };
    console.info("Tests completed", stat);
    return stat;
}
function assertQueryEquals(a, b) {
    assertEquals(queryValueFilter(a), queryValueFilter(b));
}
function queryValueFilter(obj) {
    if (typeof obj == "object") {
        if (obj instanceof Array) {
            return obj.map(queryValueFilter);
        } else {
            const newobj = {
                ...obj
            };
            delete newobj["run"];
            for(const key in newobj){
                if (Object.prototype.hasOwnProperty.call(newobj, key)) {
                    newobj[key] = queryValueFilter(newobj[key]);
                }
            }
            return newobj;
        }
    }
    return obj;
}
console.info("preparing test data...");
runWithDatabase(async function createSet(db) {
    var set = await db.createSet("test");
    await set.set("testkey", "testval");
    await set.set("testkey2", "testval2");
    assertEquals(await set.get("testkey"), "testval");
    assertEquals(await set.get("testkey2"), "testval2");
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function Set_get(db) {
    var set = await db.getSet("test");
    assertEquals(await set.get("testkey"), "testval");
    assertEquals(await set.get("testkey2"), "testval2");
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function Set_getAll(db) {
    var set = await db.getSet("test");
    assertEquals(await set.getAll(), [
        {
            key: "testkey",
            value: "testval"
        },
        {
            key: "testkey2",
            value: "testval2"
        }, 
    ]);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function Set_forEach(db) {
    var set = await db.getSet("test");
    const all = [];
    await set.forEach((key, value)=>{
        all.push({
            key,
            value
        });
    });
    assertEquals(all, [
        {
            key: "testkey",
            value: "testval"
        },
        {
            key: "testkey2",
            value: "testval2"
        }, 
    ]);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function Set_delete(db) {
    var set = await db.getSet("test");
    await set.delete("testkey");
    assertEquals(await set.get("testkey"), null);
    assertEquals(await set.get("testkey2"), "testval2");
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function getSetCount(db) {
    assertEquals(await db.getSetCount(), 1);
    assert(await db.createSet("testCount1"));
    assertEquals(await db.getSetCount(), 2);
    assert(await db.createSet("testCount2"));
    assertEquals(await db.getSetCount(), 3);
    assert(await db.createSet("testCount1"));
    assertEquals(await db.getSetCount(), 3);
    assert(await db.createSet("testCount3"));
    assertEquals(await db.getSetCount(), 4);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function deleteSet(db) {
    assertEquals(await db.deleteSet("testCount3", "kv"), true);
    assertEquals(await db.getSet("testCount3"), null);
    assertEquals(await db.getSetCount(), 3);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function deleteSet_afterModify(db) {
    const set = await db.getSet("testCount1");
    assert(set);
    await set.set("somechange", "somevalue");
    assertEquals(await db.deleteSet("testCount1", "kv"), true);
    assertEquals(await db.getSet("testCount1"), null);
    assertEquals(await db.getSetCount(), 2);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function deleteSet_check(db) {
    assertEquals(await db.getSet("testCount1"), null);
    assertEquals(await db.getSet("testCount3"), null);
    assertEquals(await db.deleteSet("testCount1", "kv"), false);
    assertEquals(await db.deleteSet("testCount3", "kv"), false);
    assertEquals(await db.getSetCount(), 2);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function DocSet_insert(db) {
    var set = await db.createSet("testdoc", "doc");
    await set.insert({
        "username": "btrdb"
    });
    await set.insert({
        "username": "test"
    });
    assertEquals(await set.get(1), {
        "id": 1,
        "username": "btrdb"
    });
    assertEquals(await set.get(2), {
        "id": 2,
        "username": "test"
    });
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_upsert(db) {
    var set = await db.createSet("testdoc", "doc");
    await set.upsert({
        "id": 1,
        "username": "whatdb"
    });
    await set.upsert({
        "id": 2,
        "username": "nobody"
    });
    assertEquals(await set.get(1), {
        "id": 1,
        "username": "whatdb"
    });
    assertEquals(await set.get(2), {
        "id": 2,
        "username": "nobody"
    });
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_get(db) {
    var set = await db.getSet("testdoc", "doc");
    assertEquals(await set.get(1), {
        "id": 1,
        "username": "whatdb"
    });
    assertEquals(await set.get(2), {
        "id": 2,
        "username": "nobody"
    });
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function DocSet_getAll(db) {
    var set = await db.getSet("testdoc", "doc");
    assertEquals(await set.getAll(), [
        {
            "id": 1,
            "username": "whatdb"
        },
        {
            "id": 2,
            "username": "nobody"
        }, 
    ]);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function DocSet_forEach(db) {
    var set = await db.getSet("testdoc", "doc");
    const all = [];
    await set.forEach((doc)=>{
        all.push(doc);
    });
    assertEquals(all, [
        {
            "id": 1,
            "username": "whatdb"
        },
        {
            "id": 2,
            "username": "nobody"
        }, 
    ]);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function DocSet_getIds(db) {
    var set = await db.getSet("testdoc", "doc");
    assertEquals(await set.getIds(), [
        1,
        2
    ]);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function DocSet_delete(db) {
    var set = await db.getSet("testdoc", "doc");
    await set.delete(1);
    assertEquals(await set.getAll(), [
        {
            "id": 2,
            "username": "nobody"
        }
    ]);
    assertEquals(await db.commit(), true);
});
let longString = "";
for(let i7 = 0; i7 < 10000; i7++){
    longString += Math.floor(Math.abs(Math.sin(i7 + 1)) * 100000000000).toString();
}
runWithDatabase(async function DocSet_largeDocument(db) {
    var set = await db.getSet("testdoc", "doc");
    await set.insert({
        "username": longString
    });
    assertEquals(await set.getAll(), [
        {
            "id": 2,
            "username": "nobody"
        },
        {
            "id": 3,
            "username": longString
        }
    ]);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_blob(db) {
    var set = await db.createSet("testblob", "doc");
    const buffer = encoder.encode(longString);
    await set.insert({
        data: buffer
    });
    await set.insert({
        data: new Uint8Array(0)
    });
    assertEquals(await set.getAll(), [
        {
            id: 1,
            data: buffer
        },
        {
            id: 2,
            data: new Uint8Array(0)
        }, 
    ]);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_largeDocument_after_index(db) {
    var set = await db.getSet("testdoc", "doc");
    assertEquals(await set.getAll(), [
        {
            "id": 2,
            "username": "nobody"
        },
        {
            "id": 3,
            "username": longString
        }
    ]);
    await set.useIndexes({
        username10: (u)=>u.username.substr(0, Math.min(10, u.username.length))
    });
    assertEquals(await set.findIndex("username10", longString.substr(0, 10)), [
        {
            "id": 3,
            "username": longString
        }
    ]);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_largeDocument_before_index(db) {
    var set = await db.getSet("testdoc", "doc");
    await set.useIndexes({
        username10: (u)=>u.username.substr(0, Math.min(10, u.username.length))
        ,
        username8: (u)=>u.username.substr(0, Math.min(8, u.username.length))
    });
    assertEquals(await set.findIndex("username8", longString.substr(0, 8)), [
        {
            "id": 3,
            "username": longString
        }
    ]);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_indexes_before_insert(db) {
    var set = await db.createSet("testindexes", "doc");
    await set.useIndexes({
        username: {
            unique: true,
            key: (u)=>u.username
        },
        gender: (u)=>u.gender
    });
    await set.insert({
        "username": "btrdb",
        gender: "m"
    });
    await set.insert({
        "username": "test",
        gender: "m"
    });
    await set.insert({
        "username": "the3rd",
        gender: "f"
    });
    assertEquals(await set.findIndex("username", "btrdb"), [
        {
            "id": 1,
            "username": "btrdb",
            "gender": "m"
        }
    ]);
    assertEquals(await set.findIndex("username", "test"), [
        {
            "id": 2,
            "username": "test",
            "gender": "m"
        }
    ]);
    assertEquals(await set.findIndex("username", "the3rd"), [
        {
            "id": 3,
            "username": "the3rd",
            "gender": "f"
        }
    ]);
    assertEquals(await set.findIndex("gender", "m"), [
        {
            "id": 1,
            "username": "btrdb",
            "gender": "m"
        },
        {
            "id": 2,
            "username": "test",
            "gender": "m"
        }, 
    ]);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_indexes_after_insert(db) {
    var set = await db.createSet("testindexes2", "doc");
    await set.insert({
        "username": "btrdb",
        "gender": "m"
    });
    await set.insert({
        "username": "test",
        "gender": "m"
    });
    await set.insert({
        "username": "the3rd",
        "gender": "f"
    });
    await set.useIndexes({
        username: {
            unique: true,
            key: (u)=>u.username
        },
        gender: (u)=>u.gender
    });
    assertEquals(await set.findIndex("username", "btrdb"), [
        {
            "id": 1,
            "username": "btrdb",
            "gender": "m"
        }
    ]);
    assertEquals(await set.findIndex("username", "test"), [
        {
            "id": 2,
            "username": "test",
            "gender": "m"
        }
    ]);
    assertEquals(await set.findIndex("username", "the3rd"), [
        {
            "id": 3,
            "username": "the3rd",
            "gender": "f"
        }
    ]);
    assertEquals(await set.findIndex("gender", "m"), [
        {
            "id": 1,
            "username": "btrdb",
            "gender": "m"
        },
        {
            "id": 2,
            "username": "test",
            "gender": "m"
        }, 
    ]);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_indexes_after_upsert(db) {
    var set = await db.getSet("testindexes2", "doc");
    assert(set);
    await set.upsert({
        "id": 2,
        "username": "nobody",
        "gender": "f"
    });
    assertEquals(await set.getAll(), [
        {
            "id": 1,
            "username": "btrdb",
            "gender": "m"
        },
        {
            "id": 2,
            "username": "nobody",
            "gender": "f"
        },
        {
            "id": 3,
            "username": "the3rd",
            "gender": "f"
        }, 
    ]);
    assertEquals(await set.findIndex("username", "btrdb"), [
        {
            "id": 1,
            "username": "btrdb",
            "gender": "m"
        }
    ]);
    assertEquals(await set.findIndex("username", "nobody"), [
        {
            "id": 2,
            "username": "nobody",
            "gender": "f"
        }
    ]);
    assertEquals(await set.findIndex("gender", "m"), [
        {
            "id": 1,
            "username": "btrdb",
            "gender": "m"
        }, 
    ]);
    assertEquals(await set.findIndex("gender", "f"), [
        {
            "id": 2,
            "username": "nobody",
            "gender": "f"
        },
        {
            "id": 3,
            "username": "the3rd",
            "gender": "f"
        }, 
    ]);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_indexes_after_delete(db) {
    var set = await db.getSet("testindexes2", "doc");
    assert(set);
    await set.delete(1);
    assertEquals(await set.getAll(), [
        {
            "id": 2,
            "username": "nobody",
            "gender": "f"
        },
        {
            "id": 3,
            "username": "the3rd",
            "gender": "f"
        }, 
    ]);
    assertEquals(await set.findIndex("username", "btrdb"), []);
    assertEquals(await set.findIndex("username", "nobody"), [
        {
            "id": 2,
            "username": "nobody",
            "gender": "f"
        }
    ]);
    assertEquals(await set.findIndex("gender", "m"), []);
    assertEquals(await set.findIndex("gender", "f"), [
        {
            "id": 2,
            "username": "nobody",
            "gender": "f"
        },
        {
            "id": 3,
            "username": "the3rd",
            "gender": "f"
        }, 
    ]);
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function DocSet_indexes_demo(db) {
    const userSet = await db.createSet("users", "doc");
    userSet.useIndexes({
        status: (user)=>user.status
        ,
        role: (user)=>user.role
        ,
        username: {
            unique: true,
            key: (user)=>user.username
        },
        onlineAdmin: (user)=>user.status == "online" && user.role == "admin"
    });
    await userSet.insert({
        username: "yuuza",
        status: "online",
        role: "user"
    });
    await userSet.insert({
        username: "foo",
        status: "offline",
        role: "admin"
    });
    await userSet.insert({
        username: "bar",
        status: "online",
        role: "admin"
    });
    assertEquals(await db.commit(), true);
    assertEquals(await userSet.findIndex("status", "online"), [
        {
            username: "yuuza",
            status: "online",
            role: "user",
            id: 1
        },
        {
            username: "bar",
            status: "online",
            role: "admin",
            id: 3
        }, 
    ]);
    assertEquals(await userSet.findIndex("username", "yuuza"), [
        {
            username: "yuuza",
            status: "online",
            role: "user",
            id: 1
        }
    ]);
    assertEquals(await userSet.findIndex("onlineAdmin", true), [
        {
            username: "bar",
            status: "online",
            role: "admin",
            id: 3
        }
    ]);
    assertEquals(await userSet.query(AND(EQ("status", "offline"), EQ("role", "admin"))), [
        {
            username: "foo",
            status: "offline",
            role: "admin",
            id: 2
        }
    ]);
    assertEquals(await userSet.query(AND(EQ("status", "online"), NOT(EQ("id", 1)))), [
        {
            username: "bar",
            status: "online",
            role: "admin",
            id: 3
        }
    ]);
});
const usersNoId = [
    {
        username: "yuuza0",
        status: "online",
        role: "admin"
    },
    {
        username: "yuuza3",
        status: "online",
        role: "user"
    },
    {
        username: "foo",
        status: "offline",
        role: "admin"
    },
    {
        username: "foo2",
        status: "online",
        role: "user"
    },
    {
        username: "foo3",
        status: "offline",
        role: "user"
    },
    {
        username: "bar",
        status: "offline",
        role: "admin"
    },
    {
        username: "bar2",
        status: "online",
        role: "admin"
    }, 
];
const users = usersNoId.map((x, i111)=>({
        id: i111 + 1,
        ...x
    })
);
runWithDatabase(async function DocSet_query(db) {
    const userSet = await db.createSet("users2", "doc");
    userSet.useIndexes({
        status: (user)=>user.status
        ,
        role: (user)=>user.role
        ,
        username: {
            unique: true,
            key: (user)=>user.username
        },
        onlineAdmin: (user)=>user.status == "online" && user.role == "admin"
        ,
        status_role: (user)=>[
                user.status,
                user.role
            ]
    });
    for (const doc of usersNoId){
        await userSet.insert(doc);
    }
    assertEquals(await db.commit(), true);
    checkQueryString();
    await checkQuery(userSet);
});
function checkQueryString() {
    assertQueryEquals(query`
      status == ${"online"}
      AND role == ${"admin"}
    `, AND(EQ("status", "online"), EQ("role", "admin")));
    assertQueryEquals(query`
      NOT(
        status == ${"offline"}
        OR role == ${"user"}
      )
    `, NOT(OR(EQ("status", "offline"), EQ("role", "user"))));
    assertQueryEquals(query`name == ${"foo"} AND age == ${123}`, AND(EQ("name", "foo"), EQ("age", 123)));
    assertQueryEquals(query`(name >= ${"foo"}) AND (age <= ${123})`, AND(GE("name", "foo"), LE("age", 123)));
    assertQueryEquals(query`(name > ${"foo"}) AND (age < ${123})`, AND(GT("name", "foo"), LT("age", 123)));
    assertQueryEquals(query`NOT((name > ${"foo"}) AND (age < ${123}))`, NOT(AND(GT("name", "foo"), LT("age", 123))));
    assertQueryEquals(query`name == ${"foo"} AND age == ${123} AND c == ${1111}`, AND(EQ("name", "foo"), EQ("age", 123), EQ("c", 1111)));
    assertQueryEquals(query`name == ${"foo"} OR age == ${123} OR c == ${3} OR d == ${4}`, OR(EQ("name", "foo"), EQ("age", 123), EQ("c", 3), EQ("d", 4)));
}
async function checkQuery(userSet) {
    assertEquals(await userSet.query(AND(EQ("status", "online"), EQ("role", "admin"))), users.filter((x)=>x.status == "online" && x.role == "admin"
    ));
    assertEquals(await userSet.query(query`
      status == ${"online"}
      AND role == ${"admin"}
    `), users.filter((x)=>x.status == "online" && x.role == "admin"
    ));
    assertEquals(await userSet.query(query`
      ${"online"} == status
      AND ${"admin"} == role
    `), users.filter((x)=>x.status == "online" && x.role == "admin"
    ));
    assertEquals(await userSet.query(EQ("status_role", [
        "online",
        "admin"
    ])), users.filter((x)=>x.status == "online" && x.role == "admin"
    ));
    assertEquals(await userSet.query(query`
      status_role == ${[
        "online",
        "admin"
    ]}
    `), users.filter((x)=>x.status == "online" && x.role == "admin"
    ));
    assertEquals(await userSet.query(OR(EQ("status", "offline"), EQ("role", "user"))), users.filter((x)=>x.status == "offline" || x.role == "user"
    ));
    assertEquals(await userSet.query(NOT(OR(EQ("status", "offline"), EQ("role", "user")))), await userSet.query(AND(EQ("status", "online"), EQ("role", "admin"))));
    assertEquals(await userSet.query(query`
      NOT(
        status == ${"offline"}
        OR role == ${"user"}
      )
    `), await userSet.query(AND(EQ("status", "online"), EQ("role", "admin"))));
    assertEquals(await userSet.query(BETWEEN("id", 2, 5, false, false)), users.filter((x)=>x.id > 2 && x.id < 5
    ));
    assertEquals(await userSet.query(query`
      id > ${2} AND id < ${5}
    `), users.filter((x)=>x.id > 2 && x.id < 5
    ));
    assertEquals(await userSet.query(BETWEEN("id", 2, 5, true, true)), users.filter((x)=>x.id >= 2 && x.id <= 5
    ));
    assertEquals(await userSet.query(query`
      id >= ${2} AND id <= ${5}
    `), users.filter((x)=>x.id >= 2 && x.id <= 5
    ));
    assertEquals(await userSet.query(AND(GE("id", 2), LE("id", 5))), users.filter((x)=>x.id >= 2 && x.id <= 5
    ));
    assertEquals(await userSet.query(NOT(AND(GE("id", 2), LE("id", 5)))), users.filter((x)=>!(x.id >= 2 && x.id <= 5)
    ));
    assertEquals(await userSet.query(OR(LT("id", 2), GT("id", 5))), users.filter((x)=>!(x.id >= 2 && x.id <= 5)
    ));
    assertEquals(await userSet.query(GT("id", 2)), users.filter((x)=>x.id > 2
    ));
    assertEquals(await userSet.query(LT("id", 5)), users.filter((x)=>x.id < 5
    ));
    assertEquals(await userSet.query(LE("id", 5)), users.filter((x)=>x.id <= 5
    ));
}
runWithDatabase(async function createSetSnap(db) {
    var set = await db.createSet("snap1");
    await set.set("somekey", "somevalue");
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function checkSnap(db) {
    var set = await db.getSet("snap1");
    assertEquals(await set.get("somekey"), "somevalue");
    var snap = await db.getPrevCommit();
    assertEquals(await snap.getSet("snap1"), null);
    assert(!!await snap.getSet("test"));
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function changeSnap(db) {
    var set = await db.getSet("snap1");
    await set.set("somekey", "someothervalue");
    await set.set("newkey", "newvalue");
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function checkSnap2(db) {
    var set = await db.getSet("snap1");
    assertEquals(await set.count, 2);
    assertEquals(await set.get("somekey"), "someothervalue");
    assertEquals(await set.get("newkey"), "newvalue");
    var snap = await db.getPrevCommit();
    var snapset = await snap.getSet("snap1");
    assertEquals(await snapset.count, 1);
    assertEquals(await snapset.get("somekey"), "somevalue");
    var snap2 = await snap.getPrevCommit();
    assertEquals(await snap2.getSet("snap1"), null);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function transaction(db) {
    db.transaction.debug = true;
    function getSet() {
        return db.getSet("transaction", "doc");
    }
    function runTransactionDeleteAll() {
        return db.runTransaction(async ()=>{
            const set = await getSet();
            for (const id of (await set.getIds())){
                await set.delete(id);
            }
        });
    }
    await db.runTransaction(async ()=>{
        await db.createSet("transaction", "doc");
    });
    await db.runTransaction(async ()=>{
        const set = await getSet();
        await set.useIndexes({
            val: {
                key: (x)=>x.val
                ,
                unique: true
            }
        });
    });
    await Promise.all([
        db.runTransaction(async ()=>{
            const set = await getSet();
            await set.insert({
                val: 1
            });
        }),
        db.runTransaction(async ()=>{
            const set = await getSet();
            await set.insert({
                val: 2
            });
        }),
        db.runTransaction(async ()=>{
            const set = await getSet();
            await set.insert({
                val: 3
            });
        }), 
    ]);
    assertEquals((await (await getSet()).getAll()).map((x)=>x.val
    ).sort(), [
        1,
        2,
        3
    ]);
    await runTransactionDeleteAll();
    const testValues = new Array(100).fill(0).map((x, i26)=>i26
    );
    const failedValues = [
        13,
        36,
        45,
        46,
        49,
        90
    ];
    await Promise.all(testValues.map(async (x)=>{
        try {
            await db.runTransaction(async ()=>{
                if (failedValues.includes(x)) {
                    throw new Error("just failed");
                }
                const set = await getSet();
                await set.insert({
                    val: x
                });
            });
        } catch (err) {
            if (err.message != "just failed") {
                throw err;
            }
        }
    }));
    assertEquals((await (await getSet()).getAll()).map((x)=>x.val
    ).sort((a, b)=>a - b
    ), testValues.filter((x)=>!failedValues.includes(x)
    ));
});
runWithDatabase(async function dumpAndImport(db) {
    await Runtime.writeTextFile("testdata/dump.json", await db.dump());
    for (const obj of (await db.getObjects())){
        await db.deleteObject(obj.name, obj.type);
    }
    await db.import(await Runtime.readTextFile("testdata/dump.json"));
    await db.commit();
    const userSet = await db.getSet("users2", "doc");
    await checkQuery(userSet);
    var testkv = await db.getSet("test");
    assertEquals(await testkv.getAll(), [
        {
            key: "testkey2",
            value: "testval2"
        }, 
    ]);
});
runWithDatabase(async function namedSnap1(db) {
    await db.createSnapshot("before_a");
    var set = await db.createSet("namedsnap1");
    await set.set("somekey", "somevalue");
    await db.createSnapshot("a");
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function namedSnap2(db) {
    var set = await db.getSet("namedsnap1");
    assertEquals(await set.get("somekey"), "somevalue");
    var snap = await db.getSnapshot("before_a");
    assert(snap);
    assertEquals(await snap.getSet("namedsnap1"), null);
    assert(!!await snap.getSet("test"));
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function namedSnap3(db) {
    var set = await db.getSet("namedsnap1");
    await set.set("somekey", "someothervalue");
    await set.set("newkey", "newvalue");
    await db.createSnapshot("b");
    assertEquals(await db.commit(), true);
});
runWithDatabase(async function namedSnap4(db) {
    var set = await db.getSet("namedsnap1");
    assertEquals(await set.count, 2);
    assertEquals(await set.get("somekey"), "someothervalue");
    assertEquals(await set.get("newkey"), "newvalue");
    var snap = await db.getSnapshot("a");
    var snapset = await snap.getSet("namedsnap1");
    assertEquals(await snapset.count, 1);
    assertEquals(await snapset.get("somekey"), "somevalue");
    var snap2 = await snap.getSnapshot("before_a");
    assertEquals(await snap2.getSet("namedsnap1"), null);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function rebuild(db) {
    await db.rebuild();
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function check_after_rebuild(db) {
    var set = await db.getSet("test");
    assertEquals(await set.get("testkey2"), "testval2");
    await checkQuery(await db.getSet("users2", "doc"));
});
const concurrentKeys = new Array(200).fill(0).map((x, i31)=>Math.floor(Math.abs(Math.sin(i31)) * 100000000000).toString()
);
const expectedConcurrentKeys = [
    ...new Set(concurrentKeys)
].sort();
const expectedConcurrentSetNames = [
    ...new Set(concurrentKeys.map((k)=>"k" + k[0]
    )), 
].sort();
runWithDatabase(async function setGetCommitConcurrent(db) {
    var set = await db.createSet("testConcurrent");
    var tasks = [];
    for (const k of concurrentKeys){
        tasks.push((async ()=>{
            await set.set("key" + k, "val" + k);
            const val = await set.get("key" + k);
            assertEquals(val, "val" + k);
            await db.commit();
        })());
    }
    await Promise.all(tasks);
    assertEquals(set.count, expectedConcurrentKeys.length);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function getAfterConcurrent(db) {
    var set = await db.getSet("testConcurrent");
    assertEquals(set.count, expectedConcurrentKeys.length);
    let errors = [];
    for (const k of concurrentKeys){
        const val = await set.get("key" + k);
        if (val != "val" + k) {
            errors.push("expect " + k + " got " + val);
        }
    }
    console.info("read done, total", concurrentKeys.length);
    if (errors) {
        console.info("errors", errors.length, errors);
    }
    assertEquals(errors, []);
    assertEquals(await db.commit(), false);
});
runWithDatabase(async function createSetGetCommitConcurrent(db) {
    var tasks = [];
    for (const k of concurrentKeys){
        tasks.push((async ()=>{
            const set = await db.createSet("k" + k[0]);
            await set.set("key" + k, "val" + k);
            const val = await set.get("key" + k);
            if (val == "val" + k) {} else {
                console.info(">>> expect " + k + " got " + val);
            }
            await db.commit();
        })());
    }
    await Promise.all(tasks);
    assertEquals(await db.commit(), false);
    assertEquals((await db.getObjects()).filter((x)=>x.name[0] == "k"
    ), expectedConcurrentSetNames.map((x)=>({
            type: "kv",
            name: x
        })
    ));
});
const keys = false == "ignore" ? [] : new Array(100000).fill(0).map((x, i41)=>Math.floor(Math.abs(Math.sin(i41 + 1)) * 100000000000).toString()
);
const expectedKeys = [
    ...new Set(keys)
].sort();
runWithDatabase(async function setMassive(db) {
    var set = await db.createSet("testMassive");
    for (const k of keys){
        await set.set("key" + k, "val" + k);
    }
    assertEquals(set.count, expectedKeys.length);
    assertEquals(await db.commit(), true);
}, false);
runWithDatabase(async function getMassive(db) {
    var set = await db.getSet("testMassive");
    const errors = [];
    assertEquals(set.count, expectedKeys.length);
    for (const k of keys){
        const val = await set.get("key" + k);
        if (val != "val" + k) {
            errors.push("expect " + k + " got " + val);
        }
    }
    assertEquals(errors, []);
    assertEquals(await db.commit(), false);
}, false);
runWithDatabase(async function getKeys(db) {
    var set = await db.getSet("testMassive");
    var r = await set.getKeys();
    var uniqueKeys = [
        ...new Set(keys)
    ].map((x)=>"key" + x
    ).sort();
    for(let i51 = 0; i51 < uniqueKeys.length; i51++){
        if (uniqueKeys[i51] != r[i51]) {
            throw new Error(`${uniqueKeys[i51]} != ${r[i51]}, i = ${i51}}`);
        }
    }
}, false);
const lastThreeSet = false == "ignore" ? [] : [
    ...new Set(keys.map((x)=>x.substr(x.length - 3)
    ))
].sort();
const lastThreeMap = lastThreeSet.map((three)=>[
        three,
        keys.filter((x)=>x.endsWith(three)
        ).sort()
    ]
);
runWithDatabase(async function DocSet_upsertMassive(db) {
    var set = await db.createSet("docMassive", "doc");
    await set.useIndexes({
        lastThree: (d)=>d.id.substr(d.id.length - 3)
    });
    for (const k of keys){
        await set.upsert({
            id: k
        });
    }
    assertEquals(await db.commit(), true);
    const actualIndexResults = (await Promise.all(lastThreeSet.map((three)=>set.findIndex("lastThree", three)
    ))).map((x1)=>x1.map((x)=>x.id
        ).sort()
    );
    const expectedIndexResults = lastThreeMap.map((x)=>x[1]
    );
    try {
        assertEquals(actualIndexResults, expectedIndexResults);
    } catch (error) {
        await dumpObjectToFile("testdata/tree.txt", await set._dump());
        await dumpObjectToFile("testdata/actual.txt", actualIndexResults);
        await dumpObjectToFile("testdata/expected.txt", expectedIndexResults);
        throw new Error("test failed, dump is created under 'testdata' folder: " + error);
    }
    assertEquals(set.count, expectedKeys.length);
}, false);
const fives = keys.map((x)=>x.substring(0, 5)
);
const fivesSet = [
    ...new Set(fives)
].sort();
const fiveLastThreeSet = [
    ...new Set(fivesSet.map((x)=>x.substring(x.length - 3)
    )), 
];
const fiveLastThreeMap = fiveLastThreeSet.map((three)=>[
        three,
        fivesSet.filter((x)=>x.endsWith(three)
        ).sort()
    ]
);
runWithDatabase(async function DocSet_upsertOverrideMassive(db) {
    var set = await db.createSet("docMassive2", "doc");
    await set.useIndexes({
        lastThree: (d)=>d.id.substr(d.id.length - 3)
    });
    const expectedIndexResults = fiveLastThreeMap.map((x)=>x[1]
    );
    let actualIndexResults = null;
    try {
        for (const k of fives){
            await set.upsert({
                id: k
            });
        }
        assertEquals(await db.commit(), true);
        actualIndexResults = (await Promise.all(fiveLastThreeSet.map((three)=>set.findIndex("lastThree", three)
        ))).map((x2)=>x2.map((x)=>x.id
            ).sort()
        );
        assertEquals(actualIndexResults, expectedIndexResults);
    } catch (error) {
        await dumpObjectToFile("testdata/five_tree.txt", await set._dump());
        await dumpObjectToFile("testdata/five_actual.txt", actualIndexResults);
        await dumpObjectToFile("testdata/five_expected.txt", expectedIndexResults);
        throw new Error("test failed, dump is created under 'testdata' folder: " + error);
    }
    assertEquals(set.count, fivesSet.length);
}, false);
const AD_toDelete = fivesSet.filter((x)=>x[1] == "0"
);
const AD_expectedIndexResults = fiveLastThreeMap.map((x3)=>x3[1].filter((x)=>x[1] != "0"
    )
);
runWithDatabase(async function DocSet_deleteMassive(db) {
    var set = await db.createSet("docMassive2", "doc");
    let actualIndexResults = null;
    try {
        for (const k of AD_toDelete){
            await set.delete(k);
        }
        assertEquals(await db.commit(), true);
        actualIndexResults = (await Promise.all(fiveLastThreeSet.map((three)=>set.findIndex("lastThree", three)
        ))).map((x4)=>x4.map((x)=>x.id
            ).sort()
        );
        assertEquals(actualIndexResults, AD_expectedIndexResults);
        assertEquals(set.count, fivesSet.length - AD_toDelete.length);
    } catch (error) {
        console.info(error);
        console.info("generating dump...");
        await dumpObjectToFile("testdata/five_delete_tree.txt", await set._dump());
        await dumpObjectToFile("testdata/five_delete_actual.txt", actualIndexResults);
        await dumpObjectToFile("testdata/five_delete_expected.txt", AD_expectedIndexResults);
        throw new Error("test failed, dump is created under 'testdata' folder: " + error);
    }
}, false);
runWithDatabase(async function rebuild_after_massive(db) {
    await db.rebuild();
    assertEquals(await db.commit(), false);
}, false);
runWithDatabase(async function check_after_rebuild(db) {
    var kv = await db.getSet("test");
    assertEquals(await kv.get("testkey2"), "testval2");
    await checkQuery(await db.getSet("users2", "doc"));
    let actualIndexResults = null;
    const expectedIndexResults = fiveLastThreeMap.map((x5)=>x5[1].filter((x)=>x[1] != "0"
        )
    );
    var set = await db.getSet("docMassive2", "doc");
    try {
        actualIndexResults = (await Promise.all(fiveLastThreeSet.map((three)=>set.findIndex("lastThree", three)
        ))).map((x6)=>x6.map((x)=>x.id
            ).sort()
        );
        assertEquals(actualIndexResults, expectedIndexResults);
        assertEquals(set.count, fivesSet.length - AD_toDelete.length);
    } catch (error) {
        console.info(error);
        console.info("generating dump...");
        await dumpObjectToFile("testdata/after_rebuild_tree.txt", await set._dump());
        await dumpObjectToFile("testdata/after_rebuild_actual.txt", actualIndexResults);
        await dumpObjectToFile("testdata/after_rebuild_expected.txt", expectedIndexResults);
        throw new Error("test failed, dump is created under 'testdata' folder: " + error);
    }
}, false);
runWithDatabase(async function delete_massive_then_rebuild(db) {
    await db.deleteSet("docMassive", "doc");
    await db.rebuild();
}, false);
if (globalThis.Deno) {
    if (globalThis.Deno.args[0] == "run") {
        (async ()=>{
            const result = await run1();
            globalThis.Deno.exit(result.total == result.passed ? 0 : 1);
        })();
    }
}
const { exit  } = require("process");
run1().then((stat)=>exit(stat.total == stat.passed ? 0 : 1)
);

