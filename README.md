# poc-diff-consumer-service

Consumer side of the PoC of data synchronization between 2 mu.semte.ch apps based on diff files. At regular intervals the consumer checks for new diff files - currently in a mounted volume - and ingests the data found in the files.

See [app-poc-diff](http://github.com/redpencilio/app-poc-diff) for the complete PoC application.

## Known limitations
* Retrieves diff files from a shared volume. Should become independent of where the producer app is located.
* No pagination when quering the database for already consumed files (will become a problem if 10000 files have been consumed)
* No authorization. All data is ingested in the same graph.
* No batching during ingest. May reach the limitation of number of triples to be inserted/deleted in 1 query.
