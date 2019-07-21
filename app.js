import requestPromise from 'request-promise';
import { app, errorHandler } from 'mu';

import SyncTask from './lib/sync-task';
import { getUnconsumedFiles } from './lib/delta-file';

const INGEST_INTERVAL = process.env.INGEST_INTERVAL_MS || 5000000;

function triggerIngest() {
  console.log(`Consuming diff files at ${new Date().toISOString()}`);
  requestPromise.post('http://localhost/ingest/');
  setTimeout( triggerIngest, INGEST_INTERVAL );
}

triggerIngest();

app.post('/ingest', async function( req, res, next ) {
  // TODO create sync task (should be done by a task-scheduler-service) that starts where the previous task ended
  // TODO get not-started sync tasks sorted by creation date
  const task = new SyncTask();
  console.log(`Ingesting new delta files since ${task.since}`);

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
});

app.use(errorHandler);
