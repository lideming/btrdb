import { Transaction } from "../btrdb.d.ts";
import { Database } from "./database.ts";
import { Deferred, deferred } from "../utils/util.ts";

export class TransactionService {
  constructor(readonly db: Database) {
  }

  debug = false;
  txn = 0;

  maxConcurrent = 10;
  running = 0;
  waitingForCommit = 0;
  needReplaying = false;

  blockingNew: Deferred<void> | null = null;
  cycleCompleted: Deferred<boolean> | null = null;

  async run<T>(fn: Transaction<T>): Promise<T> {
    const txn = ++this.txn;
    if (this.debug) console.info("txn", txn, "new transaction");

    // If we are blocking, wait until the running transactions are done.
    if (this.blockingNew) {
      if (this.debug) console.info("txn", txn, "blocking");
      do {
        await this.blockingNew;
        // In case there are lots of blocking transactions, and it's blocking again:
      } while (this.blockingNew);
    }

    let replaying = false;
    let returnValue = undefined;

    // Replay the transaction if any other transaction failed.
    while (true) {
      // About to run the transaction.
      this.running++;

      // If we exceed the max concurrent transactions, start blocking.
      if (this.running + this.waitingForCommit >= this.maxConcurrent) {
        if (this.debug) {
          console.info("txn", txn, "maxConcurrent start blocking");
        }
        if (!this.blockingNew) this.blockingNew = deferred();
      }

      if (replaying && this.debug) console.info("txn", txn, "replaying");

      // Init the completed promise if the second transaction joined the cycle.
      // (It doesn't need the promise if only one transaction is running.)
      if (this.running == 2) this.cycleCompleted = deferred();

      try {
        // Run the transaction and get the return value.
        returnValue = await fn({ db: this.db, replaying });
      } catch (e) {
        // Give up this transaction.
        if (this.debug) console.info("txn", txn, "error running");
        this.running--;
        if (this.running == 0) {
          if (this.waitingForCommit) {
            // If there are other transactions were running in the cycle.
            // Start the replay now, since this is the last transaction.
            if (this.debug) console.info("txn", txn, "[start replay]");
            if (!this.blockingNew) this.blockingNew = deferred();
            this.startReplay();
          }
        } else {
          // Some other transactions are running.
          // Mark this cycle to be replay later.
          this.needReplaying = true;
        }
        throw e;
      }

      // Exit from running state.
      this.waitingForCommit++;
      this.running--;

      if (this.debug) {
        console.info(
          "txn",
          txn,
          "finish, running =",
          this.running,
          "waiting =",
          this.waitingForCommit,
        );
      }

      if (this.running == 0) {
        // This is the last finished transaction, start commit or rollback/replay.
        // Before ending the cycle, start blocking new transactions.
        if (!this.blockingNew) this.blockingNew = deferred();
        if (this.needReplaying) {
          if (this.debug) console.info("txn", txn, "[start replay]");
          await this.startReplay();
        } else {
          // End of cycle, starting commit
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
        // Other transactions are running, just wait for the cycle end.
        if (await this.cycleCompleted) {
          // The transaction is committed, break the loop and return to caller.
          break;
        }
        // If result is false, meaning the cycle was rolled back, we need to replay.
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
