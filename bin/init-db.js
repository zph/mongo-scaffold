var d = "keyhole"
printjson(db.createCollection(d))
printjson(sh.enableSharding(d))
printjson(sh.shardCollection(`${d}.favorites`, {"_id": "hashed"}))
