# poc-diff-consumer-service

See [app-poc-diff](http://github.com/redpencilio/app-poc-diff) for the complete PoC application.

Consumer side of the PoC of data synchronization between 2 mu.semte.ch apps based on diff files. At regular intervals the consumer checks for new diff files and ingests the data found in the files.

The endpoint from which diff files are retrieved can be configured through the `SYNC_BASE_URL` environment variable (default: `http://identifier`).

## Tunnel
The endpoint where diff files are retrieved is in *a different semantic.works stack*. This application is written to always pass requests through a [mu-tunnel](http://github.com/redpencilio/mu-tunnel) service to the remote stack to sync triples from that stack.

The local endpoint for the tunnel can be configured through the `TUNNEL_ENDPOINT` environment variable, which defaults to `http://tunnel/out`.

**The identity of the remote stack** can be configured using the `TUNNEL_DEST_IDENTITY` environment variable, which defaults to `producer@redpencil.io`. This tells the tunnel which peer to forward the request to.

## Known limitations
* No pagination when quering the database for already consumed files (will become a problem if 10000 files have been consumed)
* No authorization. All data is ingested in the same graph.
* No batching during ingest. May reach the limitation of number of triples to be inserted/deleted in 1 query.
