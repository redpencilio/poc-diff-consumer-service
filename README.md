# poc-diff-consumer-service

Consumer side of the PoC of data synchronization between 2 mu.semte.ch apps based on diff files. At regular intervals the consumer checks for new diff files and ingests the data found in the files.

The endpoint from which diff files are retrieved can be configured through the `SYNC_BASE_URL` environment variable (default: 'http://172.17.0.1:81`).

See [app-poc-diff](http://github.com/redpencilio/app-poc-diff) for the complete PoC application.

## Known limitations
* No pagination when quering the database for already consumed files (will become a problem if 10000 files have been consumed)
* No authorization. All data is ingested in the same graph.
* No batching during ingest. May reach the limitation of number of triples to be inserted/deleted in 1 query.
