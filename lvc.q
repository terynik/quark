system "l /Users/nik/workspace/quark/adb.q";

.lvc.cache:();

.lvc.init:{[server;path]
    .adb.init[server;path;1b;`.lvc.writeHandler;`.lvc.flushHandler];
 };

.lvc.writeHandler:{[tableName;data]
    if[.adb.isLive[tableName];`.lvc.cache upsert select last price by symbol from data];
 };

.lvc.flushHandler:{[tableCounts]
    `.lvc.cache set select last price by symbol from quote;
 };

.lvc.init[server:`:localhost:9981;path:`$"/Users/nik/workspace/quark/dbTest"];

.z.ts:{
    .adb.reconnect[];
    show get `.lvc.cache;
 };
