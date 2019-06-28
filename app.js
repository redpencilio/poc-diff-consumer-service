import { CronJob } from 'cron';
import request from 'request';
import fs from 'fs-extra';
import { app, errorHandler, sparqlEscapeString, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';

/** Schedule export cron job */
const cronFrequency = process.env.PACKAGE_CRON_PATTERN || '*/10 * * * * *';

const shareFolder = '/share';

new CronJob(cronFrequency, function() {
  console.log(`Consuming diff files at ${new Date().toISOString()}`);
  request.post('http://localhost/ingest/');
}, null, true);


app.post('/ingest', async function( req, res, next ) {
  try {
    const files = getUnconsumedFiles();

    return res.status(202).end();
  } catch(e) {
    console.log(`Something went wrong while ingesting`);
    console.trace(e);
    return next(new Error(e));
  }
});

// Helpers

async function getUnconsumedFiles() {
  const allFiles = await fs.readdir(shareFolder);
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT ?file WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        <http://mu.semte.ch/services/poc-diff-consumer-service> ext:hasConsumed ?file .
      }
    }
  `);
  const consumedFiles = result.results.bindings.map(b => b['file'].value);
  const unconsumedFiles = diff(allFiles, consumedFiles);

  console.log(`Found ${unconsumedFiles.length} new files to be consumed`);

  for (let file of unconsumedFiles) {
    await consume(file);
  }
};

function diff(a, b) {
  return a.filter(function (item) {
    return b.indexOf(item) == -1;
  });
}

async function consume(file) {
  console.log(`Start consuming ${file}`);

  const content = await fs.readFile(`${shareFolder}/${file}`, { encoding: 'utf-8' });
  const changeSets = JSON.parse(content);

  for (let { inserts, deletes } of changeSets) {
    await insertTriples(inserts);
    await deleteTriples(deletes);
  }

  await update(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    INSERT DATA {
      GRAPH <http://mu.semte.ch/graphs/public> {
        <http://mu.semte.ch/services/poc-diff-consumer-service> ext:hasConsumed ${sparqlEscapeString(file)} .
      }
    }
  `);

  console.log(`Successfully finished consuming ${file}`);
};

async function insertTriples(triples) {
  // TODO insert in batches of 1000
  if (triples.length) {
    const statements = toStatements(triples);

    await update(`
    INSERT DATA {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${statements}
      }
    }
  `);
  }
}

async function deleteTriples(triples) {
  // TODO delete in batches of 1000
  if (triples.length) {
    const statements = toStatements(triples);

    await update(`
    DELETE DATA {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${statements}
      }
    }
  `);
  }
}

function toStatements(triples) {
  const escape = function(type, value) {
    if (type == "uri")
      return sparqlEscapeUri(value);
    else
      return sparqlEscapeString(value);
    // TODO take datatype into account
  };
  return triples.map(function(t) {
    const subject = escape(t.subject.type, t.subject.value);
    const predicate = escape(t.predicate.type, t.predicate.value);
    const object = escape(t.object.type, t.object.value);
    return `${subject} ${predicate} ${object} .`;
  }).join('');
}

app.use(errorHandler);
