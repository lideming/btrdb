import "./mod.ts";
import { run } from "../tests/test.ts";
const { exit } = require("process");

run().then((stat) => exit(!stat.failed ? 0 : 1));
