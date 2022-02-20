import {
  AND,
  EQ,
  GE,
  GT,
  LE,
  LIMIT,
  LT,
  NE,
  NOT,
  OR,
  SKIP,
  SLICE,
} from "./query.ts";

const cache = globalThis.WeakMap
  ? new WeakMap<TemplateStringsArray, AST>()
  : null;

export function query(plainText: TemplateStringsArray, ...args: any[]) {
  // console.info({ plainText, args });
  let ast = cache?.get(plainText);
  if (!ast) {
    // console.info('no cache');
    ast = new Parser(plainText)
      .parseExpr()
      .optimize();
    cache?.set(plainText, ast);
  }
  // console.info(ast);
  // console.info(ast.compute(args));
  return ast.compute(args);
}

type Token =
  | { type: "name"; value: string }
  | { type: "op"; str: string; op: OpInfo }
  | { type: "arg"; value: number }
  | { type: "(" }
  | { type: ")" }
  | { type: "end" };

type TokenType<T> = Extract<Token, { type: T }>;

type OpInfo = {
  func: (...args: any) => any;
  prec: number;
  bin: boolean;
  type: "name-value" | "bool" | "slice";
};

const Operators: Record<string, OpInfo> = {
  "==": { func: EQ, bin: true, prec: 3, type: "name-value" },
  "!=": { func: NE, bin: true, prec: 3, type: "name-value" },
  "<": { func: LT, bin: true, prec: 3, type: "name-value" },
  ">": { func: GT, bin: true, prec: 3, type: "name-value" },
  "<=": { func: LE, bin: true, prec: 3, type: "name-value" },
  ">=": { func: GE, bin: true, prec: 3, type: "name-value" },
  "NOT": { func: NOT, bin: false, prec: 2, type: "bool" },
  "AND": { func: AND, bin: true, prec: 1, type: "bool" },
  "OR": { func: OR, bin: true, prec: 1, type: "bool" },
  "SKIP": { func: SKIP, bin: true, prec: 0, type: "slice" },
  "LIMIT": { func: LIMIT, bin: true, prec: 0, type: "slice" },
};

const hasOwnProperty = Object.prototype.hasOwnProperty;

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

  consume<T extends Token["type"]>(type?: T): TokenType<T> {
    this.ensure(0);
    return this.buffer.shift() as any;
  }

  expect(type: Token["type"]) {
    if (!this.tryExpect(type)) throw new Error("Expected token type " + type);
  }

  expectAndConsume<T extends Token["type"]>(type: T): TokenType<T> {
    if (!this.tryExpect(type)) throw new Error("Expected token type " + type);
    else return this.consume();
  }

  tryExpect(type: Token["type"]) {
    return this.peek().type === type;
  }

  tryExpectAndConsume<T extends Token["type"]>(type: T): TokenType<T> | null {
    if (this.tryExpect(type)) {
      return this.consume();
    }
    return null;
  }

  ensure(pos: number) {
    while (pos >= this.buffer.length) {
      const result = this.gen.next();
      // console.info("token", result.value);
      if (result.done) {
        this.buffer.push({ type: "end" });
      } else {
        this.buffer.push(result.value);
      }
    }
  }

  *generator(plainText: ReadonlyArray<string>): Generator<Token> {
    const re = /\s*(\w+|\(\)|[!<>=]+|[()])(\s*|$)/ym;
    for (let i = 0; i < plainText.length; i++) {
      const str = plainText[i];
      while (true) {
        const match = re.exec(str);
        if (!match) break;
        const word = match[1];
        if (hasOwnProperty.call(Operators, word)) {
          yield { type: "op", str: word, op: Operators[word] };
        } else if (word === "(" || word === ")") {
          yield { type: word };
        } else {
          yield { type: "name", value: word };
        }
      }
      // Yield a "arg" token between every plain string
      if (i < plainText.length - 1) {
        yield { type: "arg", value: i };
      }
    }
  }

  // Parser part

  // https://github.com/lideming/lideming.github.io/blob/ee31c3865/toolbox/calc.html#L308
  parseExpr(): AST {
    return this.parseBinOp(this.parseValue(), 0);
  }

  parseValue(): AST {
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
      return new OpAST(op.op, [value]);
    } else {
      throw new Error("Expected a value");
    }
  }

  parseBinOp(left: AST, minPrec: number): AST {
    while (true) {
      const t = this.peek();
      if (t.type !== "op" || !t.op.bin || t.op.prec < minPrec) break;
      this.consume();
      let right = this.parseValue();
      while (true) {
        const nextop = this.peek();
        if (nextop.type !== "op" || !t.op.bin || nextop.op.prec <= t.op.prec) {
          break;
        }
        right = this.parseBinOp(right, nextop.op.prec);
      }
      left = new OpAST(t.op, [left, right]);
    }
    return left;
  }
}

abstract class AST {
  abstract compute(args: any[]): any;
  abstract optimize(): AST;
}

class ArgAST extends AST {
  constructor(
    readonly argPos: number,
  ) {
    super();
  }

  compute(args: any[]) {
    return args[this.argPos];
  }
  optimize() {
    return this;
  }
}

class NameAST extends AST {
  constructor(
    readonly name: string,
  ) {
    super();
  }

  compute(args: any[]) {
    return this.name;
  }
  optimize() {
    return this;
  }
}

class OpAST extends AST {
  constructor(
    readonly op: OpInfo,
    readonly children: AST[],
  ) {
    super();
  }
  compute(args: any[]) {
    return this.op.func(...this.children.map((x) => x.compute(args)));
  }
  optimize() {
    let optimizedChildren = this.children.map((x) => x.optimize());

    // AND(AND(a, b), c) => AND(a, b, c)
    if (this.op.func === AND || this.op.func === OR) {
      const first = optimizedChildren[0];
      if (first instanceof OpAST && first.op.func === this.op.func) {
        optimizedChildren.splice(0, 1);
        optimizedChildren = [...first.children, ...optimizedChildren];
      }
    }

    if (this.op.type === "name-value") {
      if (optimizedChildren.length !== 2) {
        throw new Error("Wrong count of operands");
      }
      if (
        optimizedChildren[0] instanceof NameAST &&
        optimizedChildren[1] instanceof ArgAST
      ) {
        // noop
      } else if (
        optimizedChildren[0] instanceof ArgAST &&
        optimizedChildren[1] instanceof NameAST
      ) {
        // value == name
        //    => name == value
        [optimizedChildren[1], optimizedChildren[0]] = [
          optimizedChildren[0],
          optimizedChildren[1],
        ];
      } else {
        throw new Error("Wrong type of operands");
      }
    } else if (this.op.type == "slice") {
      // Checking for SKIP/LIMIT
      if (optimizedChildren.length !== 2) {
        throw new Error("Wrong count of operands");
      }
      const parameter = optimizedChildren[1];
      if (!(parameter instanceof ArgAST)) {
        throw new Error(
          `Thr right side of ${this.op.func.name} should be a number value`,
        );
      }

      // (query SKIP 1) LIMIT 2 => SLICE(query, 1, 2)
      let child = optimizedChildren[0];
      if (child instanceof OpAST && child.op.type == "slice") {
        let skip: ArgAST;
        let limit: ArgAST;
        if (this.op.func === SKIP) {
          skip = parameter;
        } else {
          limit = parameter;
        }
        if (child.op.func === this.op.func) {
          throw new Error(`Two nested ${this.op.func.name}`);
        }
        if (child.op.func === SKIP) {
          skip = child.children[1] as ArgAST;
        } else {
          limit = child.children[1] as ArgAST;
        }
        return new SliceAST(child.children[0], skip!, limit!);
      }
    }

    // TODO: more transforms
    //
    // name > min AND name < max
    //    => BETWEEN(name, min, max, false, false)

    return new OpAST(this.op, optimizedChildren);
  }
}

class SliceAST extends AST {
  constructor(
    readonly queryChild: AST,
    readonly skip: ArgAST,
    readonly limit: ArgAST,
  ) {
    super();
  }

  compute(args: any[]) {
    return SLICE(
      this.queryChild.compute(args),
      this.skip.compute(args),
      this.limit.compute(args),
    );
  }
  optimize() {
    return this;
  }
}
