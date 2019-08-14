import fs from 'fs-extra';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import mu, { sparqlEscapeDateTime, uuid } from 'mu';

const TASK_NOT_STARTED_STATUS = 'http://mu.semte.ch/services/poc-diff-consumer-service/sync-task-statuses/not-started';
const TASK_ONGOING_STATUS = 'http://mu.semte.ch/services/poc-diff-consumer-service/sync-task-statuses/ongoing';
const TASK_SUCCESS_STATUS = 'http://mu.semte.ch/services/poc-diff-consumer-service/sync-task-statuses/success';
const TASK_FAILED_STATUS = 'http://mu.semte.ch/services/poc-diff-consumer-service/sync-task-statuses/failed';

class SyncTask {
  constructor({ uri, since, until, created, status }) {
    this.uri = uri;
    this.since = since;
    this.until = until;
    this.created = created;
    this.status = status;
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
    await file.consume(async (file, isSuccess) => {
      this.handledFiles++;
      console.log(`Consumed ${this.handledFiles}/${this.totalFiles} files`);
      this.updateStatus(file, isSuccess);

      if (this.status == TASK_SUCCESS_STATUS && this.handledFiles < this.totalFiles) {
        await this.consumeNext();
      } else {
        await this.finish();
      }
    });
  };

  updateStatus(file, isSuccess) {
    if (isSuccess && this.status != TASK_FAILED_STATUS) {
      this.status = TASK_SUCCESS_STATUS;

      const deltaMs = Date.parse(file.created);
      if (deltaMs > this.latestDeltaMs) {
        this.latestDeltaMs = deltaMs;
      }
    } else if (!isSuccess) {
      this.status = TASK_FAILED_STATUS;
    }
  }

  async finish() {
    if (this.status == TASK_SUCCESS_STATUS) {
      console.log(`Finished task with status ${this.status}. Most recent delta file consumed is created at ${this.latestDelta.toISOString()}.`);
    } else {
      console.log(`Failed to finish sync task. Skipping the remaining files. Most recent delta file successfully consumed is created at ${this.latestDelta.toISOString()}.`);
    }
    await this.writeStatus();
    await insertNextSyncTask(this.latestDelta);
  }

  async writeStatus() {
    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      DELETE {
        GRAPH ?g {
          <${this.uri}> ext:taskStatus ?status ;
                        ext:deltaUntil ?latestDelta .
        }
      } INSERT {
        GRAPH ?g {
          <${this.uri}> ext:taskStatus <${this.status}> ;
                        ext:deltaUntil ${sparqlEscapeDateTime(this.latestDelta)} .
        }
      } WHERE {
        GRAPH ?g {
          <${this.uri}> ext:taskStatus ?status .
          OPTIONAL { <${this.uri}> ext:deltaUntil ?latestDelta . }
        }
      }
    `);
  }
}

async function insertNextSyncTask(since = new Date()) {
  const uuid = mu.uuid();
  const uri = `http://mu.semte.ch/services/poc-diff-consumer-service/sync-tasks/${uuid}`;

  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {
      GRAPH <http://mu.semte.ch/graphs/public> {
        <${uri}> a ext:SyncTask ;
           mu:uuid "${uuid}" ;
           ext:taskStatus <${TASK_NOT_STARTED_STATUS}> ;
           ext:deltaSince ${sparqlEscapeDateTime(since)} ;
           dct:created ${sparqlEscapeDateTime(new Date())} .
      }
    }
  `);
  console.log(`Scheduled new sync task to ingest diff files since ${since.toISOString()}`);
}

async function getNextSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?s ?since ?created WHERE {
      ?s a ext:SyncTask ;
         ext:taskStatus <${TASK_NOT_STARTED_STATUS}> ;
         ext:deltaSince ?since ;
         dct:created ?created .
    } ORDER BY ?since LIMIT 1
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];
    return new SyncTask({
      uri: b['s'].value,
      status: TASK_NOT_STARTED_STATUS,
      since: new Date(Date.parse(b['since'].value)),
      created: new Date(Date.parse(b['created'].value))
    });
  } else {
    return null;
  }
}

async function getLatestSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?s ?status ?latestDelta ?created WHERE {
      ?s a ext:SyncTask ;
         ext:taskStatus ?status ;
         ext:deltaUntil ?latestDelta ;
         dct:created ?created .
    } ORDER BY DESC(?latestDelta) LIMIT 1
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];
    return new SyncTask({
      uri: b['s'].value,
      status: b['status'].value,
      until: new Date(Date.parse(b['latestDelta'].value)),
      created: new Date(Date.parse(b['created'].value))
    });
  } else {
    return null;
  }
}

export default SyncTask;
export {
  getNextSyncTask,
  getLatestSyncTask,
  insertNextSyncTask
}
