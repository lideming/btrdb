import "./mod.ts";
import { run } from "../test.ts";
import { exit } from "std";

run().then((stat) => exit(stat.total == stat.passed ? 0 : 1), (err) => {
  print(err, err.stack);
});
