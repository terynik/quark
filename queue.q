system "l /Users/nik/workspace/quark/adb.q";

.cep.streams:([tableName:"s"$(); streamName:"s"$()] handler:"s"$());
.cep.sources:([tableName:"s"$(); partition:"d"$()] tableCounts:"j"$(); cacheCounts:"j"$());
.cep.positions:([tableName:"s"$(); partition:"d"$(); streamName:"s"$()] streamPosition:"j"$());
/ TODO: partition type
/.cep.queues:flip `tableName`partition`tableCounts`cacheCounts`streamName`handler`streamPosition!"sdjjssj"$\:();

.cep.init:{[server;path]
    .adb.init[server;path;1b;`.cep.writeHandler;`.cep.flushHandler];
 };

.cep.writeHandler:{[tableName;data]
    / insert the data into in-memory cache
    if[.adb.isLive[tableName];.Q.dd[`.quarkCache;tableName] insert data];

    / update counts
    `.cep.sources upsert `tableName`partition xkey select tableName, cacheCounts:count i by partition:date from .Q.dd[`.quarkCache;tableName]
 };

.cep.flushHandler:{[tableCounts]
    .cep.resetSources[];
 };

.cep.resetSources:{[]
    `.cep.sources set raze {[tableName] 
        :`tableName`partition xkey select tableName, tableCounts:count i, cacheCounts:0j by partition:date from tableName;
    } each exec distinct tableName from .cep.streams;
 };

/jobs:select from (ej[`tableName;.cep.sources;.cep.streams] lj .cep.positions) where streamPosition < tableCounts + cacheCounts
/.cep.processBatch[job:jobs[0]]
select streamPosition, totalCounts:tableCounts+cacheCounts, tableCounts, cacheCounts, inQueue:tableCounts+cacheCounts-streamPosition from (ej[`tableName;.cep.sources;.cep.streams] lj .cep.positions)

.cep.processBatches:{
    :max .cep.processBatch each select from (ej[`tableName;.cep.sources;.cep.streams] lj .cep.positions) where streamPosition < tableCounts + cacheCounts;
 };

.cep.processBatch:{[job]
    / take batch offset (inside partition) and its size
    offset:$[null job[`streamPosition];0j;job[`streamPosition]];
    size:100;

    / first take data from table on disk
    data:select from job[`tableName] where date = job[`partition], i within (offset,offset+size-1);

    / if it's less then batch size, take the rest from memory cache
    /   it's took me a while to write this <select> so I have to explain
    /   we start at offset in cache which includes current stream <offset> + who many record we took from the disk table minus size of the table on disk
    /   we take as much as we can up to batch size
    if[size > count data;data,:select[(offset+count data)-job[`tableCounts],size-count data] from .Q.dd[`.quarkCache;job[`tableName]] where date = job[`partition]];

    / finally, let's do some CEP action...
    processedCount:@[value job[`handler];data];

    1 "Processed ",string[processedCount],"(",string[count data],") records at ",string[offset],"(",string[job[`tableCounts]],"+",string[job[`cacheCounts]],") position for ",string[job[`tableName]],"/",string[job[`partition]],"\n";
    
    `.cep.positions upsert (job[`tableName];job[`partition];job[`streamName];offset+processedCount);
    :1b;
 };

minuteBars:flip `date`minute`symbol`quoteCount`high`low`timestamp!"dusjfft"$\:();

.cep.streamHandler:{[data]
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

.cep.streamHandler1:{[data]
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

.mon.timerTick:{[]
    if[() ~ key `handle;:(::)];
    neg[get `handle] .j.j[.cep.positions];
 };

/ test
/system "l quarkWrite.q";
/.quarkWrite.loadTableConfig[pathToConfigFile:`$":tablesTest.csv"];
/.cepTest.timerTick:{[]
/    n:rand 10; .quarkWrite.writeData[table:`quote;data:([]date:n#.z.D; channel:n#`inprocess; sequence:n#0; symbol:n?`$'.Q.a; timestamp:n#.z.T; price:50f+n?50f)];
/    .quarkWrite.timerTick[];
/ };
/.z.ts:{ .cepTest.timerTick[] };

.cep.init[server:`:localhost:9981;path:`$"/Users/nik/workspace/quark/dbTest"];

/`.cep.streams insert (`quote;`cep;`.cep.streamHandler);
`.cep.streams insert (`quote;`bridge;`.bridge.streamHandler);
.cep.resetSources[];

.z.ts:{};
.z.ts:{ .adb.reconnect[] };
.z.ts:{ .adb.reconnect[]; .cep.processBatches[] };

/.z.wo:{ set[`handle;x] };
/.z.ws:{ show x };

/.Q.l[`$"/Users/nik/workspace/quark/dbCep"];
/while[.cep.processBatches[]];
