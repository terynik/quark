system "l /Users/nik/workspace/quark/cache.q";

.queue.streams:2!flip `tableName`streamName`batchSize`handler!"ssjs"$\:();
.queue.sources:2!flip `tableName`partition`tableCount`cacheCount!"sdjj"$\:();
.queue.positions:3!flip `tableName`partition`streamName`streamPosition!"sdsj"$\:();
/ TODO: partition type
/.queue.queues:flip `tableName`partition`tableCount`cacheCount`streamName`handler`streamPosition!"sdjjssj"$\:();

.queue.init:{[server;path]
    .cache.init[server;path;1b;`.queue.writeHandler;`.queue.flushHandler];
 };

.queue.writeHandler:{[tableName;data]
    / insert the data into in-memory cache
    if[.cache.isLive[tableName];.Q.dd[`.quarkCache;tableName] insert data];

    / update counts
    `.queue.sources upsert `tableName`partition xkey select tableName, cacheCount:count i by partition:date from .Q.dd[`.quarkCache;tableName]
 };

.queue.flushHandler:{[tableCount]
    .queue.resetSources[];
 };

.queue.resetSources:{[]
    `.queue.sources set raze {[tableName] 
        :`tableName`partition xkey select tableName, tableCount:count i, cacheCount:0j by partition:date from tableName;
    } each exec distinct tableName from .queue.streams;
 };

.queue.processBatches:{
    :max .queue.processBatch each select from (ej[`tableName;.queue.sources;.queue.streams] lj .queue.positions) where streamPosition < tableCount + cacheCount;
 };

.queue.processBatch:{[job]
    / take batch offset (inside partition) and its size
    offset:$[null job[`streamPosition];0j;job[`streamPosition]];

    / first take data from table on disk
    data:select from job[`tableName] where date = job[`partition], i within (offset,offset+job[`batchSize]-1);

    / if it's less then batch size, take the rest from memory cache
    /   it's took me a while to write this <select> so I have to explain
    /   we start at offset in cache which includes current stream <offset> + who many record we took from the disk table minus size of the table on disk
    /   we take as much as we can up to batch size
    if[job[`batchSize] > count data;data,:select[(offset+count data)-job[`tableCount],job[`batchSize]-count data] from .Q.dd[`.quarkCache;job[`tableName]] where date = job[`partition]];

    / finally, let's do some action...
    processedCount:@[value job[`handler];data];

    1 "Processed ",string[processedCount],"(",string[count data],") records at ",string[offset],"(",string[job[`tableCount]],"+",string[job[`cacheCount]],") position for ",string[job[`tableName]],"/",string[job[`partition]],"\n";
    
    `.queue.positions upsert (job[`tableName];job[`partition];job[`streamName];offset+processedCount);
    :1b;
 };

minuteBars:flip `date`minute`symbol`quoteCount`high`low`timestamp!"dusjfft"$\:();

.queue.streamHandler:{[data]
    x:select quoteCount:count i, high:max price, low:min price, timestamp:max timestamp by date, timestamp.minute, symbol from data;
    
    indexes:?[(keys x)#minuteBars;key x];
    updateIdx:where not indexes = count minuteBars;
    insertIdx:where indexes = count minuteBars;

    /minuteBars[indexes[updateIdx]]
    /(0!x)[updateIdx]
    
    @[`minuteBars;indexes[updateIdx];{[a;b] a[`quoteCount]+:b[`quoteCount]; a[`high]:max[(a[`high];b[`high])]; a[`low]:min[(a[`low];b[`low])]; a[`timestamp]:max[(a[`timestamp];b[`timestamp])]; :a;};(0!x)[updateIdx]]
    `minuteBars insert (0!x)[insertIdx];
    :count data;
 };

.queue.streamHandler1:{[data]
    x:select[1,4] quoteCount:count i, high:max price, low:min price by date, timestamp.minute, symbol from data;
    
    indexes:?[key x;(keys x)#minuteBars];
    index1:where not indexes = count x;
    index2:indexes[index1];

    /minuteBars[index1]
    /(0!x)[index2]
    
    x:@[0!x;index2;{[a;b] a[`quoteCount]+:b[`quoteCount]; a[`high]:max[(a[`high];b[`high])]; a[`low]:min[(a[`low];b[`low])]; :a;};minuteBars[index1]]
    `minuteBars upsert x;
    :count data;
 };

.bridge.streamHandler:{[data]
    :count data;
 };

/ test
/system "l quarkWrite.q";
/.quarkWrite.loadTableConfig[pathToConfigFile:`$":tablesTest.csv"];
/.queueTest.timerTick:{[]
/    n:rand 10; .quarkWrite.writeData[table:`quote;data:([]date:n#.z.D; channel:n#`inprocess; sequence:n#0; symbol:n?`$'.Q.a; timestamp:n#.z.T; price:50f+n?50f)];
/    .quarkWrite.timerTick[];
/ };
/.z.ts:{ .queueTest.timerTick[] };

.queue.init[server:`:localhost:9981;path:`$"/Users/nik/workspace/quark/dbTest"];

`.queue.streams insert (`quote;`queue;100;`.queue.streamHandler);
`.queue.streams insert (`quote;`bridge;200;`.bridge.streamHandler);
.queue.resetSources[];

jobs:select from (ej[`tableName;.queue.sources;.queue.streams] lj .queue.positions) where streamPosition < tableCount + cacheCount
/.queue.processBatch[job:jobs[0]]
/select streamPosition, totalCounts:tableCount+cacheCount, tableCount, cacheCount, inQueue:tableCount+cacheCount-streamPosition from (ej[`tableName;.queue.sources;.queue.streams] lj .queue.positions)


.z.ts:{};
.z.ts:{ .cache.reconnect[] };
.z.ts:{ .cache.reconnect[]; .queue.processBatches[] };

/.z.wo:{ set[`handle;x] };
/.z.ws:{ show x };

/.Q.l[`$"/Users/nik/workspace/quark/dbqueue"];
/while[.queue.processBatches[]];
