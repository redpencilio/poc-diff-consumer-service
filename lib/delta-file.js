import fs from 'fs-extra';
import fetch from 'node-fetch';
import path  from "path";
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';

const SYNC_BASE_URL = process.env.SYNC_BASE_URL || 'http://producer-identifier';
const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}/sync/deltas`;
const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}/files/:id/download`;
const BATCH_SIZE = 100;
const DOWNLOAD_SHARE_ENDPOINT = process.env.DOWNLOAD_SHARE_ENDPOINT || `${SYNC_BASE_URL}/fileshare/:id/download`;
const FILE_FOLDER = process.env.FILE_FOLDER || "/share/";

class DeltaFile {
  constructor(data) {
    this.id = data.id;
    this.created = data.attributes.created;
    this.name = data.attributes.name;
  }

  get downloadUrl() {
    return DOWNLOAD_FILE_ENDPOINT.replace(':id', this.id);
  }

  get tmpFilepath() {
    return `/tmp/${this.id}.json`;
  }

  async consume(onFinish) {
    const writeStream = fs.createWriteStream(this.tmpFilepath);
    writeStream.on('finish', () => this.ingest(onFinish));

    try {
      const response = await fetch(this.downloadUrl, { method: "GET" });
      response.body.pipe(writeStream);
    } catch (e) {
      console.log(`Something went wrong while consuming the file ${this.id}`);
      throw e;
    }

  }

  async ingest(onFinish) {
    console.log(`Start ingesting file ${this.id} stored at ${this.tmpFilepath}`);
    try {
      const changeSets = await fs.readJson(this.tmpFilepath, { encoding: 'utf-8' });
      for (let { inserts, deletes } of changeSets) {
        //Filter the triples with URIs that start with share:// for basic file synchronisation
        const shareInserts = filterShareTriples(inserts);
        const shareDeletes = filterShareTriples(deletes);
        console.log("Downloading URIs:", shareInserts);
        console.log("Deleting files URIs:", shareDeletes);
        //Download or delete files
        await downloadFiles(shareInserts);
        await deleteFiles(shareDeletes);
        //Insert and delete all triples as normal
        await processInserts(inserts);
        await processDeletes(deletes);
      }
      console.log(`Successfully finished ingesting file ${this.id} stored at ${this.tmpFilepath}`);
      await onFinish(this, true);
      await fs.unlink(this.tmpFilepath);
    } catch (e) {
      await onFinish(this, false);
      console.log(`Something went wrong while ingesting file ${this.id} stored at ${this.tmpFilepath}`);
      console.log(e);
    }
  }
}

/**
 * Inserts are triples that go into different graphs. This groups them by graph and inserts the in the database.
 *
 * @private
 * @async
 * @function processInserts
 * @param {Array} inserts - data object with inserts, delta-file format
 * @returns {void} Nothing
 */
async function processInserts(inserts) {
  let grouping = {};

  for (let insert of inserts)
    if (grouping[insert.graph.value])
      grouping[insert.graph.value].push(insert);
    else
      grouping[insert.graph.value] = [insert];

  for (let graph in grouping)
    await insertTriplesInGraph(grouping[graph], graph);
}

/**
 * Deletes are triples that go into different graphs. This groups them by graph and inserts the in the database.
 *
 * @private
 * @async
 * @function processInserts
 * @param {Array} inserts - data object with inserts, delta-file format
 * @returns {void} Nothing
 */
async function processDeletes(deletes) {
  let grouping = {};

  for (let del of deletes)
    if (grouping[del.graph.value])
      grouping[del.graph.value].push(del);
    else
      grouping[del.graph.value] = [del];

  for (let graph in grouping)
    await deleteTriplesInGraph(grouping[graph], graph);
}

async function getUnconsumedFiles(since) {
  const url = `${SYNC_FILES_ENDPOINT}?since=${since.toISOString()}`;
  try {
    const response = await fetch(url, { method: "GET", headers: { 'Accept': 'application/vnd.api+json' }});
    if (response.ok) {
      const jsondata = await response.json();
      return jsondata.data.map(f => new DeltaFile(f));
    }
    else {
      throw new Error("Response while retreiving unconsumed file not ok");
    }
  } catch (e) {
    console.log(`Unable to retrieve unconsumed files from ${url}`);
    console.error(e);
    throw e;
  }
}

/**
 * Insert the list of triples in a defined graph in the store
 *
 * @param triples {Array} Array of triples from an insert changeset
 * @param graph {string} Graph to insert the triples into
 * @param {String} scope - Scope identifier for filtering in the deltanotifier.
 * @method insertTriplesInGraph
 * @private
 */
async function insertTriplesInGraph(triples, graph, scope) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Inserting ${triples.length} triples in batches. Current batch: ${i}-${i + BATCH_SIZE}`);
    const batch = triples.slice(i, i + BATCH_SIZE);
    const statements = toStatements(batch);
    const scopeID = (scope ? { 'mu-call-scope-id': scope } : undefined);
    await update(`
      INSERT DATA {
          GRAPH <${graph}> {
              ${statements}
          }
      }
    `, scopeID);
  }
}

/**
 * Delete the list of triples in a defined graph in the store
 *
 * @param triples {Array} Array of triples from an insert changeset
 * @param graph {string} Graph to insert the triples into
 * @param {String} scope - Scope identifier for filtering in the deltanotifier.
 * @method insertTriplesInGraph
 * @private
 */
async function deleteTriplesInGraph(triples, graph, scope) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Inserting ${triples.length} triples in batches. Current batch: ${i}-${i + BATCH_SIZE}`);
    const batch = triples.slice(i, i + BATCH_SIZE);
    const statements = toStatements(batch);
    const scopeID = (scope ? { 'mu-call-scope-id': scope } : undefined);
    await update(`
      DELETE DATA {
          GRAPH <${graph}> {
              ${statements}
          }
      }
    `, scopeID);
  }
}

/**
 * Delete the triples from the given list from all graphs in the store, including the temporary graph.
 * Note: Triples are deleted one by one to avoid the need to use OPTIONAL in the WHERE clause
 *
 * @param {Array} triples Array of triples from an insert changeset
 * @method insertTriplesInTmpGraph
 * @private
 */
async function deleteTriplesInAllGraphs(triples) {
  console.log(`Deleting ${triples.length} triples one by one in all graphs`);
  for (let i = 0; i < triples.length; i++) {
    const statements = toStatements([triples[i]]);
    await update(`
      DELETE WHERE {
          GRAPH ?g {
              ${statements}
          }
      }
    `);
  }
}

/**
 * Transform an array of triples to a string of statements to use in a SPARQL query
 *
 * @public
 * @function toStatements
 * @param {Array} - triples Array of triples to convert to a string
 * @returns {String} String that can be used in a SPARQL query
 */
function toStatements(triples) {
  return triples.map(t => {
    const subject   = escapeRDFTerm(t.subject);
    const predicate = escapeRDFTerm(t.predicate);
    const object    = escapeRDFTerm(t.object);
    return `${subject} ${predicate} ${object} . `;
  }).join('\n');
}

/**
 * This transforms a JSON binding object in SPARQL result format to a string that can be used in a SPARQL query
 *
 * @public
 * @function escapeRDFTerm
 * @param {Object} rdfTerm - Object of the form { value: "...", type: "..." [...] }
 * @returns {String} String representation of the RDF term in SPARQL syntax
 */
function escapeRDFTerm(rdfTerm) {
  const { type, value, datatype, "xml:lang":lang } = rdfTerm;
  switch (type) {
    case "uri":
      return sparqlEscapeUri(value);
    case "typed-literal":
    case "literal":
      if (datatype)
        return `${sparqlEscapeString(value)}^^${sparqlEscapeUri(datatype)}`;
      if (lang)
        return `${sparqlEscapeString(value)}@${lang}`;
      else
        return `${sparqlEscapeString(value)}`;
    default:
      return sparqlEscapeString(value);
  }
}

/*
 * Takes a collection of triples and returns only unique URIs that contain share references
 *
 * @private
 * @function filterShareTriples
 * @param {Array} triples - Array of triples
 * @return {Set} Set of unique strings of the URIs that contain share references
 */
function filterShareTriples(triples) {
  let filtered = new Set();
  for (let triple of triples) {
    if (triple.subject.value.startsWith("share://")) {
      filtered.add(triple.subject.value);
    }
  }
  return filtered;
}

/*
 * Downloads all the files related to some share URIs
 *
 * @private
 * @async
 * @function downloadFiles
 * @param {Iterable} shareURIs - Collection of URIs starting with share:// for the files that need to be downloaded, can be an Array, Set, ...
 * @returns {void} Nothing
 */
async function downloadFiles(shareURIs) {
  for (let uri of shareURIs) {
    const filepath = uriToPath(uri);
    //If, for some reason, the filepath does not start with the mounted volume's path, throw error or files get lost.
    if (!filepath.startsWith(FILE_FOLDER)) {
      throw new Error(`The file wants to be downloaded to a file path ${filepath} that is not in the configured folder of ${FILE_FOLDER}. Files can get lost like this!`);
    }
    //Prepare the URL
    const fileDownloadURL = DOWNLOAD_SHARE_ENDPOINT.replace(":id", encodeURIComponent(uri));
    try {
      await createTargetDir(filepath);
      const writeStream = await createWriteStream(filepath);
      const response = await fetch(fileDownloadURL);
      if (response.ok) {
        return response.body.pipe(writeStream);
      }
      else {
        throw new Error("Fetching file failed, response not ok");
      }
    }
    catch (err) {
      //On error, remove the file path again. If left behind, next download attempts are prevented because the file already exists
      await deleteFile(filepath);
      throw err;
    }
  }
}

/*
 * Deletes all the files related to some share URIs
 *
 * @private
 * @async
 * @function deleteFiles
 * @param {Iterable} shareURIs - Collection of URIs starting with share:// for the files that need to be removed, can be an Array, Set, ...
 * @returns {void} Nothing
 */
async function deleteFiles(shareURIs) {
  let ps = [];
  for (let uri of shareURIs)
    ps.push(deleteFile(uriToPath(uri)));
  return Promise.all(ps);
}

/*
 * Creates a write stream to the filepath, not overwriting existing files.
 * Closes by itself on error and success. Resolves only on ready state.
 *
 * @private
 * @async
 * @function createWriteStream
 * @param {String} filepath - Path to create the write stream for
 * @returns {WriteStream} The created write stream
 */
function createWriteStream(filepath) {
  return new Promise((resolve, reject) => {
    //With the 'x' flag, error if file already exists
    const writeStream = fs.createWriteStream(filepath, { flags: "wx" });
    //writeStream.on("ready", () => {
    //  resolve(writeStream);
    //});
    writeStream.on("finish", () => {
      writeStream.close();
    });
    writeStream.on("error", (err) => {
      if (err.errno == -17) {
        console.log("File already exists on local storage");
        resolve(writeStream);
      } else {
        writeStream.close();
        reject(`Error while downloading and saving file ${filepath} on the consumer, during writing to the filestream.`);
      }
    });
    resolve(writeStream);
  });
}

/*
 * Creates all folders in the given folder or file path recursively if they do not exist
 *
 * @private
 * @async
 * @function creteTargetDir
 * @param {String} filepath - Target folder or file path of which all folders need to be created
 * ®returns {void} Nothing
 */
function createTargetDir(filepath) {
  return new Promise((resolve, reject) => {
    //Get the folderpath, without filename
    const targetdir = path.dirname(filepath);
    //With the recursive property, no error is raised if the folders already exist
    fs.mkdir(targetdir, { recursive: true }, err => {
      if (err) {
        reject(new Error(`Target directory ${targetdir} creation failed`));
      }
      resolve();
    });
  });
}

/**
 * Delete a file. Ignores when the file does not exist.
 *
 * @async
 * @private
 * @function deleteFile
 * @param {string} filepath - Name with full path of the file to be removed.
 * @return {void} Nothing
 */
function deleteFile(filepath) {
  return new Promise((resolve, reject) => {
    fs.rm(filepath, { force: false }, (err) => {
      if (err) {
        //Ignore when file is already removed
        if (err.errno != -2)
          reject(new StackableError(`Error while deleting file ${filepath}.`, err))
        resolve();
      }
      else {
        console.log("Removing file gave message:", err);
        resolve()
      }
    });
  });
}

function uriToPath(uri) {
  return uri.replace(/(\w+):\/\/(.*)/, "/$1/$2");
}

export {
  getUnconsumedFiles
}
