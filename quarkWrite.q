system "l /Users/nik/workspace/quark/quarkPerf.q";

.quarkWrite.tables:([tableName:"s"$()] databasePath:"s"$(); columnNames:(); columnTypes:(); partitionColumn:"s"$(); sortColumns:(); flushTimeLimit:"t"$(); flushSizeLimit:"i"$(); lastFlushTimestamp:"t"$());
.quarkWrite.listeners:([handle:"j"$(); databasePath:"s"$()] writeHandler:"s"$(); flushHandler:"s"$());

.quarkWrite.loadTableConfig:{[pathToConfigFile]
    / read configuration file
    config:0:[("ss**s*ti";enlist csv);pathToConfigFile];

    / we do not need empty lines
    config:delete from config where tableName=`;

    / convert |-delimited strings into normal lists
    config:update columnNames:{`$ "|" vs x} each columnNames, sortColumns:{`$ "|" vs x} each sortColumns from config;

    / add real-time columns which are not stored in csv config
    config:update lastFlushTimestamp:.z.t from config;

    / create a in-memory cache table with name .quarkCache.<tableName> for each target table
    {[table] .Q.dd[`.quarkCache;table[`tableName]] set flip table[`columnNames]!{x$()} each table[`columnTypes]; } each config;

    / insert configuration into global config table
    `.quarkWrite.tables insert config;
 };

.quarkWrite.cleanUpTables:{[]
    / check if <.quarkCache> namespace exists and delete all in-memory cache tables
    if[0 < count key `.quarkCache;![`.quarkCache;();0b;exec tableName from `.quarkWrite.tables]];

    / remove all configured tables
    delete from `.quarkWrite.tables;
 };

.quarkWrite.subscribe:{[path;writeListener;flushListener]
    / if listener is already connected, we return empty list
    /   most likely this is a ping message and we should tell client no that they are already connected
    if[0 < count select from `.quarkWrite.listeners where handle=.z.w;:()];

    / make the world aware that we have received some important things to do
    1 "New listener with handle ",string[.z.w]," from ",string[.z.h]," to database ",string[path],"\n";

    / add the listener to internal table, we'll use it later to invoke callbacks
    `.quarkWrite.listeners upsert (.z.w;path;writeListener;flushListener);

    / we return tableNames and also table structure, so the listener can create a in-memory cache table
    /   we simply do <0#> to take only table structure, not the records from our own in-memeory cache
    tableNames:exec tableName from `.quarkWrite.tables where databasePath=path;
    :tableNames!({[table] 0#value .Q.dd[`.quarkCache;table]} each tableNames);
 };

.quarkWrite.cleanUpHandles:{[]
    / remove all listeners which do not have an active handle
    /   design is to call an idempotent method in .z.pc and before invoking callbacks on listeners
    /   we still have to call it in .z.pc (which I don't like) but kdb is re-using handles
    /   TODO: it's a good idea to write it the the log
    delete from `.quarkWrite.listeners where not handle in key .z.W;

    / remove slow subscribers
    /   TODO: make it smarter, e.g. remove those which consume more then their share of available heap in case of heap is over threshold
    showSubs:where 10000000 < sum each .z.W;
    if[0 ~ count showSubs;:(::)];
    /1 "----------------Disconnecting slow subscriber(s) with handle(s) ",sv[",";string each showSubs],"\n";
    /hclose each showSubs;
 };

/ TODO: consider to add <path> as parameter to <.quarkWrite.writeData>
.quarkWrite.writeData:{[name;data]
    .quarkPerf.start[`.quarkWrite.writeData];

    / check that table is in our records and get it's description, otherwise throw an exception
    table:$[name in key .quarkWrite.tables;.quarkWrite.tables[name];'"Unknown table ",string[name]];

    / funky debug output
    1 "Inserting data: ",sv[",";{(string count value .Q.dd[`.quarkCache;x]),"->",(string x)} each exec tableName from .quarkWrite.tables],"...\r";

    / add data to the memory cache table
    .Q.dd[`.quarkCache;name] insert data;
    .quarkPerf.check[`.quarkWrite.writeData;`insert;name];

    / remove inactive handlers
    .quarkWrite.cleanUpHandles[];

    / notify our listeners that they need to reload their partitions from the disk
    {[listener;name;data] neg[listener[`handle]](listener[`writeHandler];name;data); }[;name;data] each 0!select from `.quarkWrite.listeners where databasePath=table[`databasePath], not writeHandler = `;
    .quarkPerf.check[`.quarkWrite.writeData;`notify;name];
 };

.quarkWrite.flushTable:{[name] 
    / check that table is in our records and get it's description, otherwise throw an exception
    table:$[name in key .quarkWrite.tables;.quarkWrite.tables[name];'"Unknown table ",string[name]];

    / let's check if there is anything to do at all...
    if [0 ~ count value .Q.dd[`.quarkCache;name];:(::)];
    
    / get data from in-memory cache table and reenumerate it
    /   we have to preserve current sym file, as <.Q.en> will update it
    /   it's a design choice that <.quarkWrite> library doesn't require database to be loaded
    symCopy:$[`sym ~ key `sym;get `sym;(::)];
    data:.Q.en[hsym table[`databasePath];value .Q.dd[`.quarkCache;name]];
    if[not symCopy ~ (::);`sym set symCopy];

    / empty in-memory cache table
    delete from .Q.dd[`.quarkCache;name];

    / group data by partitions (<partition> -> list of indexes in <data>)
    partitions:group data[table[`partitionColumn]];
    
    / delete partColumn, we will not need it anymore as all required information is in <partitions>
    data:![data;();0b;enlist table[`partitionColumn]];
    
    / make people aware we are doing some important work here
    1 "Writing table '",string[name],"' to disk: ",sv[",";{[p;i] (string count i)," records into ", string p}'[key partitions;value partitions]],"\n";

    / this piece of q code might look threatening, but don't worry, I will explain:
    / first, we will take all partitions (<key partitions>) we need to write to and build a list of directories where partitions are
    /   note that we need to do <.Q.dd[x,`]> to add extra "/" at the end, so <set> operator later knows that we want to save a splayed table
    / second, we choose to use <insert> for existing partitions, or <set> if we need to create a new one
    /   let's say we have two partitons (2024.01.01;2024.01.02), one of them (2024.01.02) doesn't exists yet, table is <t1> and the <table[`databasePath]> is `/path/to/db
    /   then the output will be a list of two operators:
    /       {[t;d] t insert d}[`:/path/to/db/2024.01.01/t1/;]   / for the existing partition
    /       {[t;d] t set d}[`:/path/to/db/2024.01.02/t1/;]      / for the new partition
    operators:{[path] ?[() ~ key path;{[t;d] t set d};{[t;d] t insert d}][path;] } each .Q.dd[;`] each .Q.par[hsym table[`databasePath];;name] each key partitions;

    / finally, we simply apply <operators> (either <insert[path;]> or <set[path;]> from above) to the <data> indexed at <indexes>
    {[data;operator;indexes] operator[data[indexes]]}[data;;]'[operators;value partitions];
 };

.quarkWrite.flushDatabase:{[currentTime;path]
    .quarkPerf.start[`.quarkWrite.flushDatabase];

    / get all tables in this database which we will need to write to the disk
    tableNames:exec tableName from .quarkWrite.tables where databasePath=path;

    / write them
    t01:.z.T; .quarkWrite.flushTable each tableNames;
    .quarkPerf.check[`.quarkWrite.flushDatabase;`flushTables;path];

    / update time when we last flushed
    update lastFlushTimestamp:currentTime from `.quarkWrite.tables where databasePath=path;

    / a bit complicated q code, but it does a simple thing: it builds a dictionary of <table names> -> <table counts> 
    / another way to do it would be to load partitioned table with .Q.l[] and simply call <count> for each <tableNames> 
    /   we don't do it with .Q.l[] because it's a design choice to keep <.quarkWrite> module independent from loading database into memory
    /   for example, it can process multiple databases at the same time
    / one more design decision might be to keep counts in memory
    /   it's a reasonable option, but my choice was not to complicate the code and also avoid delta-based calculations
    /   this is an arguable decision and we might revisit it if scanning partitions on disk actually takes reasonable amount of time
    t02:.z.T; tableCounts:tableNames!{[d;p;t] sum {[d;p;t] path:.Q.par[d;p;t]; $[() ~ key path;0j;count get path]}[d;;t] each p}[hsym[path];(key hsym[path]) except `sym;] each tableNames;
    .quarkPerf.check[`.quarkWrite.flushDatabase;`countTables;path];

    / tell the world we have achieved something very important
    t99:.z.T; 1 "Flushing ",string[path]," complete; write time: ",string[0.001*(t02-t01)],"us, count time: ",string[0.001*(t99-t02)],"us\n";

    / remove inactive handlers
    .quarkWrite.cleanUpHandles[];

    / notify our listeners that they need to reload their partitions from the disk
    /   we send them table counts so they can compare them to their view of table counts after reload
    /   it's possible that listened was blocked by a long query and they have two or more reload messages in inbound network queue, while data on disk is already updated
    /   this might create duplication of records, when one copy comes from the disk, and another is delivered over network into in-memory cache (generated by <writeData>)
    /   listeners should use table counts to detect such scenario and block incoming in-memeory cache updates until counts are in sync
    {[listener;tableCounts] neg[listener[`handle]](listener[`flushHandler];tableCounts); }[;tableCounts] each 0!select from `.quarkWrite.listeners where databasePath=path, not flushHandler = `;
 };

.quarkWrite.flushAll:{[currentTime;force]
    / we are going to flush database when one of 3 conditions is met:
    /   - it's forced by the called (e.g. on shutdown)
    /   - time limit has passed since last flush
    /   - limit of in-memory cache is exceeded
    / such logic protects us from:
    /   - losing data when shutting down
    /   - having too stale data on disk when there are not many update (e.g. market status)
    /   - out of memory issues under load when there are too many updates within flush time limit
    / note that <max> accepts a list, hence we use <max[(...;...;...)]> syntax
     .quarkWrite.flushDatabase[currentTime;] each exec distinct databasePath from .quarkWrite.tables where max[(force;currentTime > lastFlushTimestamp+flushTimeLimit;flushSizeLimit < {[table] count value .Q.dd[`.quarkCache;table]} each tableName)];
 };

.quarkWrite.timerTick:{[]
    .quarkWrite.flushAll[.z.t;0b];
 };

.quarkWrite.onExit:{[]
    .quarkWrite.flushAll[.z.t;1b];
 };

.quarkWrite.onClose:{[]
    .quarkWrite.cleanUpHandles[];
 }; 