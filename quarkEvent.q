system "l /Users/nik/workspace/quark/quarkUtils.q";

.quarkEvent.instance:`handle`server`connectHandler`disconnectHandler`databasePath!(0Nj;`:localhost:9981;`.quarkEvent.connectHandler;`.quarkEvent.disconnectHandler;`$"/Users/nik/workspace/quark/db");

.quarkEvent.connectHandler:{[self]
    self[`tables]:self[`handle](`.quarkWrite.subscribe;self[`databasePath];`.quarkEvent.writeHandler;`.quarkEvent.flushHandler);

    `.quarkEvent.instance set self;
 };

.quarkEvent.disconnectHandler:{[self]
    `.quarkEvent.instance set self;
 };

.quarkEvent.writeHandler:{[table;data]
    show table;
 };

.quarkEvent.flushHandler:{[tableCounts]
 };

.z.ts:{};
.z.ts:{.quarkUtils.reconnect[.quarkEvent.instance]};
