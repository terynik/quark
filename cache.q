system "l /Users/nik/workspace/quark/quarkUtils.q";

.adb.instance:(::);

/ TODO: tables
.adb.init:{[server;path;realtime;writeHandler;flushHandler]
    self:enlist[`]!enlist(::);
    self[`server]:server;
    self[`handle]:0Nj;
    self[`connectHandler]:`.adb.connectHandler;
    self[`pingHandler]:`.adb.connectHandler;
    self[`disconnectHandler]:`.adb.disconnectHandler;
    self[`databasePath]:path;
    self[`writeHandler]:writeHandler;
    self[`flushHandler]:flushHandler;
    / TODO: tables
    self[`tables]:`symbol$();
    self[`states]:()!();

    / try to load the database right now, fail fast policy
    .Q.l[self[`databasePath]];
    .Q.bv[];

    `.adb.instance set self;
 };

.adb.reconnect:{[]
    .quarkUtils.reconnect[.adb.instance];
 };

.adb.connectHandler:{[self]
    / subscribe to the database updates on the servers, the call will tell us what tables are in the database
    result:self[`handle](`.quarkWrite.subscribe;self[`databasePath];`.adb.writeHandler;`.adb.flushHandler);

    / if result is an empty list, then we are already connected, hence nothing to do
    if[() ~ result;:(::)];

    / make the world aware we have achieved some huge success
    1 "Subscribed for ",sv[",";string each key result]," tables in ",string[self[`databasePath]],"\n";

    / create empty in-memory cache tables
    set'[.Q.dd[`.quarkCache;] each key result;value result];

    / initial state of all tables is DISK, we will wait for the next flushHandler callback to change to LIVE
    self[`states]:(key result)!(count key result)#`DISK;
    self[`tables]:result;

    `.adb.instance set self; 
 };

.adb.disconnectHandler:{[self]
    / clean up in-memory cache
    {[table] delete from table;} each .Q.dd[`.quarkCache;] each key self[`tables];
   
    `.adb.instance set self;
 };

.adb.isLive:{[tableName]
    self:get `.adb.instance;
    :`LIVE = self[`states][tableName];
 };

.adb.writeHandler:{[tableName;data]
    self:get `.adb.instance;

    / validate the table
    if[not tableName in key self[`states];'tableName];

    1 "Received ",string[count data]," records into ",string[tableName]," (status ",string[self[`states][tableName]],", ",string[count value tableName]," on disk, ",string[count value .Q.dd[`.quarkCache;tableName]]," in cache)\n";

    / if table is not in LIVE state, we ignore the data
    /if[not `LIVE = self[`states][tableName];:(::)]; 

    / business logic callback
    .[self[`writeHandler];(tableName;data)];
 };

.adb.flushHandler:{[tableCounts]
    self:get `.adb.instance;

    1 "Received flush event with table counts ",sv[", ";{string[x],":",string[y]}'[key tableCounts;value tableCounts]],"\n";

    / TODO: we might not need keys in tableCounts
    if[not (key self[`states]) ~ (key tableCounts);show "something is wrong, keys don't match"];

    / clean up in-memory cache
    {[table] delete from table;} each .Q.dd[`.quarkCache;] each key self[`states];

    / reload database from the disk
    /   TODO: we need to do something with paths, mistical .Q.lo (https://code.kx.com/q/ref/dotq/#lo-load-without) is not defined 
    /   maybe it's time to learn <k> and implement .Q.lo ourselves
    t01:.z.p; .Q.l[self[`databasePath]]; 

    / re-create missing partitions just in memory
    /   TODO: this actually takes time and it's not required
    /   <.quarkWrite> should make sure that when a new partition is created for one table...
    /   ...then it creates an empty partition for all other tables 
    t02:.z.p; .Q.bv[];

    / count size, it will actually take a while as the disk has to be scanned
    /   in real deployment, it's assumed that number of partitions accessable via <.adb> is limited to one day or one week
    t03:.z.p; localCounts:{[table] count value table} each key self[`states];

    / counts must match, otherwise we are either behind (hence we must ignore new in-memory cache data)
    self[`states]:(key self[`states])!({$[x;`LIVE;`DISK]} each (localCounts = value tableCounts));

    / make the world aware that something dramatically important has happened
    /   TODO: it will be nice to add table counts (both local and remote in case of DISK status)
    t99:.z.p; 1 "Reloaded ",string[self[`databasePath]]," in ",string[0.001*(t02-t01)],"+",string[0.001*(t03-t02)],"+",string[0.001*(t99-t03)],"us, table status: ",sv[", ";{[t;s] sv[" is ";string each (t;s)]}'[key self[`states];value self[`states]]],"\n";

    `.adb.instance set self;

    / business logic callback
    .[self[`flushHandler];enlist tableCounts];
 };

.adb.interceptSelect:{[x]
    /`x set x; show x;
    if[not 10h = type x;:value x];
    tree:parse x;
    if[not 5 = count tree;:value x];
    if[not tree[0] = (parse "?[;;;]")[0];:value x];
    /set'[`op`t`c`b`a;tree]; show tree;
    :.adb.executeSelect[tree[1];tree[2];tree[3];tree[4]];
 };

.adb.select1:{[query]
    :.[.adb.select;1_parse query];
 };

.adb.select:{[table;c;b;a]
    /set'[`table`c`b`a;(table;c;b;a)];

    / check if the table has a cache, this function is supposed to be called only for "hybrid" tables,
    if[not table in key `.quarkCache;'"Unknown table ",string[table]];

    / this will be out cache part
    cache:.Q.dd[`.quarkCache;table];

    / if type of <b> is not a list, then it's a select without grouping
    /   now it's simple, we just do select to both <table> and <cache>, join then together with <,> and return
    if[not 99h = type b;:(?[table;c[0];0b;a] , ?[cache;c[0];0b;a])];

    / otherwise, it's grouping select:
    /   first, we use <raze/> to iterate into all nested lists to flatten 
    columns:distinct (value b) , aFlat[where -11h = type each aFlat:raze/[value a]];
    
    j:?[table;c[0];0b;columns!columns] , ?[cache;c[0];0b;columns!columns];

    :?[j;();b;a];
 };
