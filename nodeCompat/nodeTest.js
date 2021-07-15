import "./mod.ts";
import { run } from "../test.ts";
const { exit } = await import("process");

const stat = await run();
exit(stat.total == stat.passed ? 0 : 1);
