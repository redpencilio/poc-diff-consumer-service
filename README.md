# consumer-service-graph-mirror

See [app-poc-diff](http://github.com/redpencilio/app-poc-diff) for the complete PoC application, espacially the producer for the corresponding delta-file api.

Consumer side of the PoC of data synchronization between 2 mu.semte.ch apps based on diff files. At regular intervals the consumer checks for new diff files and ingests the data found in the files. This consumer ingests the triples in the same graphs as they where in on the producer.

## Environment variables

* `SYNC_BASE_URL`: the endpoint from which diff files are retrieved (default: `http://identifier`)
* `INGEST_INTERVAL_MS`: interval in milliseconds of the time between ingest operation starts (default: 5000)

