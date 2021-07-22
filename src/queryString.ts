import { AND, EQ, GT, LT } from "./query.ts";

export function query(plainText: TemplateStringsArray, ...args: any[]) {
  // for (const token of lexer(plainText, args)) {
  //     console.info(token);
  // }
}

type Token =
  | { type: "word"; value: string }
  | { type: "binop"; str: string; op: any }
  | { type: "value"; value: number }
  | { type: "end" };

type OpInfo = { func: (left: any, right: any) => any; prec: number };

const BinOps: Record<string, OpInfo> = {
  "==": { func: EQ, prec: 1 },
  "<": { func: LT, prec: 1 },
  ">": { func: GT, prec: 1 },
  "AND": { func: AND, prec: 1 },
};

class Parser {
  constructor(plainText: ReadonlyArray<string>) {
    this.gen = this.generator(plainText);
  }

  // Lexer part

  gen: Generator<Token>;
  buffer: Token[] = [];

  peek() {
    this.ensure(0);
    return this.buffer[0];
  }

  consume() {
    this.ensure(0);
    this.buffer.shift();
  }

  ensure(pos: number) {
    while (pos >= this.buffer.length) {
      const result = this.gen.next();
      if (result.done) {
        this.buffer.push({ type: "end" });
      } else {
        this.buffer.push(result.value);
      }
    }
  }

  *generator(plainText: ReadonlyArray<string>): Generator<Token> {
    const re = /\s*(\w+)(\s|$)*/ym;
    for (let i = 0; i < plainText.length; i++) {
      const str = plainText[i];
      while (true) {
        const match = re.exec(str);
        if (!match) break;
        yield { type: "word", value: match[1] };
      }
      if (i < plainText.length - 1) {
        yield { type: "value", value: i };
      }
    }
  }

  // Parser part

  parseBinOp(left, minPrec) {
    var op;
    while (op = this.peek(), op.type === "binop" && op.val.prec >= minPrec) {
      this.consume();
      var right = parseValue();
      var nextop;
      while (
        nextop = this.peek(),
          nextop.type === "binop" && nextop.val.prec > op.val.prec
      ) {
        right = this.parseBinOp(right, nextop.val.prec);
      }
      left = new BinOpAST(op, left, right);
    }
    return left;
  }
}

abstract class AST {
}

class BinOpAST extends AST {
  constructor(
    public op: any,
    public left: any,
    public right: any,
  ) {
    super();
  }
}
