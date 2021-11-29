# consumer-service-graph-mirror with naive file synchronisation

See [app-poc-diff](http://github.com/redpencilio/app-poc-diff) for the complete PoC application, especially the producer for the corresponding delta-file api.

Consumer side of the PoC of data synchronization between 2 mu.semte.ch apps based on diff files. At regular intervals the consumer checks for new diff files and ingests the data found in the files. This consumer ingests the triples in the same graphs as they where in on the producer.

This consumer synchronises files too, but in a naive way. If file downloading failes, the process continues and ignores the problems. Files are downloaded to a path equivalent to their original path, no remapping.

## Environment variables

* `SYNC_BASE_URL`: the endpoint from which diff files are retrieved (default: `http://identifier`)
* `INGEST_INTERVAL_MS`: interval in milliseconds of the time between ingest operation starts (default: 5000)
* `FILE_FOLDER`: this contains the path to the shared volume. This is not a way to configure the location of the files. In this naive file synchronisation mechanism, files will be written to the path described in the URI. This variable is used to check if files might get lost by putting them in other folder than the mounted volume. (optional, default: `/share/`)

