import fs from 'fs-extra';

const TASK_NOT_STARTED_STATUS = 'http://mu.semte.ch/services/poc-diff-consumer-service/sync-task-statuses/not-started';
const TASK_ONGOING_STATUS = 'http://mu.semte.ch/services/poc-diff-consumer-service/sync-task-statuses/ongoing';
const TASK_SUCCESS_STATUS = 'http://mu.semte.ch/services/poc-diff-consumer-service/sync-task-statuses/success';
const TASK_FAILED_STATUS = 'http://mu.semte.ch/services/poc-diff-consumer-service/sync-task-statuses/failed';

class SyncTask {
  constructor() {
    this.since = '2019-07-21T09:46:50.678Z';
    this.created = new Date();
    this.status = TASK_NOT_STARTED_STATUS;
    this.handledFiles = 0;
    this.latestDeltaMs = 0;
    this.files = [];
  }

  get latestDelta() {
    return new Date(this.latestDeltaMs);
  }

  get totalFiles() {
    return this.files.length;
  }

  async execute() {
    try {
      console.log(`Found ${this.totalFiles} new files to be consumed`);
      if (this.totalFiles) {
        await this.consumeNext();
      }
    } catch (e) {
      console.log(`Something went wrong while consuming the files`);
      console.log(e);
    }
  }

  async consumeNext() {
    const file = this.files[this.handledFiles];
    await file.consume((file, isSuccess) => this.updateStatus(file, isSuccess));
  };

  async updateStatus(file, isSuccess) {
    this.handledFiles++;

    if (isSuccess && this.status != TASK_FAILED_STATUS) {
      this.status = TASK_SUCCESS_STATUS;

      const deltaMs = Date.parse(file.created);
      if (deltaMs > this.latestDeltaMs) {
        this.latestDeltaMs = deltaMs;
      }
    } else if (!isSuccess) {
      this.status = TASK_FAILED_STATUS;
    }

    console.log(`Consumed ${this.handledFiles}/${this.totalFiles} files`);

    if (this.status == TASK_SUCCESS_STATUS) {
      if (this.handledFiles >= this.totalFiles) {
        console.log(`Finished task with status ${this.status}. Most recent delta file consumed is created at ${this.latestDelta.toISOString()}.`);
        // TODO write success to store with most recent delta timestamp
      } else {
        await this.consumeNext();
      }
    } else {
      console.log(`Failed to finish sync task. Skipping the remaining files. Most recent delta file successfully consumed is created at ${this.latestDelta.toISOString()}.`);
      // TODO write failure to store with most recent delta timestamp
    }
  }

}

export default SyncTask;
