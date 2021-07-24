import "./mod.ts";
import { run } from "../tests/test.ts";
import { exit } from "process";

run().then((stat) => exit(stat.total == stat.passed ? 0 : 1));
