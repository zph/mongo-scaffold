// Example post-initialization script for mlaunch
// This script creates a new collection and shards it
//
// Usage:
//   mlaunch init --preset cluster --post-init-script examples/shard-collection.js

// Database and collection names
const dbName = "testdb";
const collectionName = "sharded_collection";

print("=".repeat(60));
print("Post-initialization script: Creating and sharding collection");
print("=".repeat(60));

// Switch to the database
db = db.getSiblingDB(dbName);
print(`Using database: ${dbName}`);

// Enable sharding on the database
print(`\nEnabling sharding on database: ${dbName}`);
try {
    sh.enableSharding(dbName);
    print(`✓ Sharding enabled on database: ${dbName}`);
} catch (e) {
    print(`✗ Error enabling sharding: ${e.message}`);
    throw e;
}

// Create the collection
print(`\nCreating collection: ${collectionName}`);
try {
    db.createCollection(collectionName);
    print(`✓ Collection created: ${collectionName}`);
} catch (e) {
    print(`✗ Error creating collection: ${e.message}`);
    throw e;
}

// Create a hashed index on the shard key field (required before hashed sharding)
// Using 'userId' as the shard key field
const shardKeyField = "userId";
print(`\nCreating hashed index on shard key field: ${shardKeyField}`);
try {
    db[collectionName].createIndex({ [shardKeyField]: "hashed" });
    print(`✓ Hashed index created on: ${shardKeyField}`);
} catch (e) {
    print(`✗ Error creating hashed index: ${e.message}`);
    throw e;
}

// Shard the collection using hashed sharding
const shardKey = { [shardKeyField]: "hashed" };
print(`\nSharding collection with hashed shard key: ${JSON.stringify(shardKey)}`);
try {
    sh.shardCollection(`${dbName}.${collectionName}`, shardKey);
    print(`✓ Collection sharded with hashed sharding: ${dbName}.${collectionName}`);
} catch (e) {
    print(`✗ Error sharding collection: ${e.message}`);
    throw e;
}

// Verify sharding status
print(`\nVerifying sharding status...`);
try {
    const status = sh.status();
    const shardInfo = status.databases.find(d => d.database === dbName);
    if (shardInfo && shardInfo.partitioned) {
        print(`✓ Database ${dbName} is sharded`);
        const collectionInfo = shardInfo.collections.find(c => c[collectionName]);
        if (collectionInfo) {
            print(`✓ Collection ${collectionName} is sharded`);
        }
    }
} catch (e) {
    print(`⚠ Warning: Could not verify sharding status: ${e.message}`);
}

// Insert some sample data to demonstrate
print(`\nInserting sample data...`);
try {
    const sampleData = [];
    for (let i = 1; i <= 10; i++) {
        sampleData.push({
            userId: i,
            name: `User ${i}`,
            email: `user${i}@example.com`,
            createdAt: new Date()
        });
    }
    db[collectionName].insertMany(sampleData);
    print(`✓ Inserted ${sampleData.length} sample documents`);
} catch (e) {
    print(`⚠ Warning: Could not insert sample data: ${e.message}`);
}

// Display collection stats
print(`\nCollection statistics:`);
try {
    const stats = db[collectionName].stats();
    print(`  Documents: ${stats.count}`);
    print(`  Size: ${(stats.size / 1024).toFixed(2)} KB`);
    print(`  Storage size: ${(stats.storageSize / 1024).toFixed(2)} KB`);
} catch (e) {
    print(`⚠ Warning: Could not get collection stats: ${e.message}`);
}

print("\n" + "=".repeat(60));
print("Post-initialization script completed successfully!");
print("=".repeat(60));

