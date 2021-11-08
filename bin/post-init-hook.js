// Use this for custom configuration needed of cluster
// after the general init has been run

printjson(sh.addShardTag('shard01', 'current'))
printjson(sh.addShardTag('shard02', 'favorites'))
printjson(sh.addShardTag('shard03', 'favorites'))

// https://www.mongodb.com/blog/post/tiered-storage-models-in-mongodb-optimizing
// https://docs.mongodb.com/manual/core/zone-sharding/#std-label-zone-sharding
// (v3.4 switched tag to zone aware)
// https://docs.mongodb.com/manual/tutorial/sharding-tiered-hardware-for-varying-slas/
// https://docs.mongodb.com/manual/core/sharded-cluster-query-router/#std-label-sharding-mongos-targeted
// https://docs.mongodb.com/manual/core/sharded-cluster-query-router/#std-label-sharding-mongos-broadcast
printjson(sh.addTagRange(`keyhole.favorites`,{_id:MinKey},{_id:MaxKey},'favorites'))
