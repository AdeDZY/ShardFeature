# ShardFeature
Generate index-based features for LeToRResources and SHRCK.
Use '-h' to get help message in most python scripts.

## Compile
1. modify Makefile.app using your indri.
2. compile the following c++ files:
  1. shardFeature.cpp (It gets the shard-level features such as df, tf)
  2. unionInter_shard.cpp (It gets the CSI inverted list union/inter features)
  
## To get shard-level features
1. `./genShardFeatureJobs.py partition_name shardmaps_dir repo_dir query_term_file`
  - partition_name        a name for this run. e.g. cw09-s1
  - shardmaps_dir         dir containing the shardmaps from the clustering
  - repo_dir              dir to the index
  - query_term_file       each line is a query term in string; for bigram features, each line is a query
2. `./jobSubmitter partition_name 1`
3. output will be in `./output/{partition_name}/features/{shardID}.feat`
4. In each feature file:
  1. First line: "-1 shard_size shard_total_TF shard_size", e.g. "-1 353817 206779078 353817"
  2. From the second line: "termID, shard_df, shard_tf, language_model_score", e.g. "10 199768 591807 1567.07"
  3. language_model_score is the sum over the term's LM in all documents.
You can use -h to see help message of th above scripts.
  
## To get CSI union/inter features
1. copy the CSI document list from `fedsearch/output/CSI/run_name/sample1/sampled` into  `./output/{partition_name}/csi_sample'
2. replace ',' into ' ' (space)
3. `./unionInter_shard`
  - repoPath      dir of the index
  - extidFile     ./output/{partition_name}/csi_sample
  - outFile       ./output/{partition_name}/csi_unioninter
  - queryFile     each line is a query in string
  - nShardsStr    total number of shards
