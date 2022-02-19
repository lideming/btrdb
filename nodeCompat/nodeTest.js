import "./mod.ts";
import { run } from "../tests/test.ts";
const { exit } = require("process");

run().then((stat) => exit(stat.total == stat.passed ? 0 : 1));
