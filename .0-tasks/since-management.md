keydb improve since management for loading only the required data

1. Nodes (node/node.go) shouldn't populate the "since" map on startup
2. Therefore, the `forceSkipFilesListing` configuration setting can be removed
3. When a node is asked to create a snapshot, it should check S3 first, and, if for a given hash range there are some 
   files, it should create the snapshots using the "since" from the filenames that are on S3
4. When loading a snapshot, a node should list all files on S3 first and check their "since" and only load the ones that 
   makes sense
5. if a node has already loaded a snapshot in the past, it could save locally in its badger that it has already loaded 
   that filename specifically (we could call this "load snapshots checkpoint")
   * we would need an endpoint to clean up that data or change it in the "scaler" (see cmd/scaler and internal/scaler)
