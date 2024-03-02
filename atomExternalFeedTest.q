system "l /Users/nik/workspace/quark/quarkUtils.q";

.Q.l[`$"/Users/nik/workspace/quark/db"];

sequences:select last sequence by channel from quote where date=last date;
seq3:$[`channel3 in key sequences;sequences[`channel3;`sequence];0j];

self:`handle`server`connectHandler`disconnectHandler!(0Nj;`:localhost:9981;`connectHandler;`disconnectHandler);

connectHandler:{[self]
    self[`handle](`.quarkWrite.subscribe;`$"/Users/nik/workspace/quark/db";`;`flushHandler);
    `self set self;
 };

disconnectHandler:{[self]
    `self set self;
 };

flushHandler:{[tableCounts]
    .Q.l[`$"/Users/nik/workspace/quark/db"];
    1 "Last stored sequence is ",string[(select last sequence by channel from quote where date=last date)[`channel3;`sequence]],"\n";
 };

.z.ts:{};
.z.ts:{
    if[not .quarkUtils.reconnect[self];:(::)];
    n:1+rand 9; seq:get `seq3; neg[self[`handle]](`.quarkWrite.writeData;table:`quote;data:([]date:n#.z.D; channel:n#`channel3; sequence:seq+til n; symbol:n?`$'.Q.a; timestamp:n#.z.T; price:50f+n?50f)); `seq3 set seq+n;
 };

/.z.exit:{.quarkUtils.disconnect[]};

