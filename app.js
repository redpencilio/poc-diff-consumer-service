import requestPromise from 'request-promise';
import { app, errorHandler } from 'mu';

import { getNextSyncTask, getLatestSyncTask, insertNextSyncTask } from './lib/sync-task';
import { getUnconsumedFiles } from './lib/delta-file';

const INGEST_INTERVAL = process.env.INGEST_INTERVAL_MS || 5000;

function triggerIngest() {
  console.log(`Consuming diff files at ${new Date().toISOString()}`);
  requestPromise.post('http://localhost/ingest/');
  setTimeout( triggerIngest, INGEST_INTERVAL );
}

triggerIngest();

app.post('/ingest', async function( req, res, next ) {
  const task = await getNextSyncTask();
  if (task) {
    console.log(`Ingesting new delta files since ${task.since.toISOString()}`);
    try {
      const files = await getUnconsumedFiles(task.since);
      task.files = files;
      task.execute();
      return res.status(202).end();
    } catch(e) {
      console.log(`Something went wrong while ingesting`);
      console.trace(e);
      // TODO write failure to store
      return next(new Error(e));
    }
  } else {
    console.log(`No sync task found`);
    const latestTask = getLatestSyncTask();

    if (latestTask) {
      await insertNextSyncTask(latestTask.until);
    } else {
      await insertNextSyncTask();
    }
    return res.status(200).end();
  }
});

app.use(errorHandler);
