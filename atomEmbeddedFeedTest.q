system "l /Users/nik/workspace/quark/quarkWrite.q";

.Q.l[`$"/Users/nik/workspace/quark/db"];

sequences:0!select last sequence by channel from quote where date=last date;
sequences:sequences[`channel]!sequences[`sequence]

/sequences:()!();

.quarkWrite.cleanUpTables[];
.quarkWrite.loadTableConfig[pathToConfigFile:`$":../tables-test.csv"];
.quarkWrite.loadTableConfig[pathToConfigFile:`$":../tables-perf.csv"];

.quarkWrite.writeData[table:`status;data:([]date:1#.z.D; channel:1#`statusChannel; sequence:1#0; symbol:1#`x; timestamp:1#.z.T; status:1#`start)];
.quarkWrite.flushAll[currentTime:.z.t;force:1b];

writeQuoteData:{[channel;n]
    seq:$[null sequences[channel];0j;sequences[channel]];
    .quarkWrite.writeData[table:`quote;data:([]date:n#.z.D; channel:n#channel; sequence:seq+til n; symbol:n?`$'.Q.a; timestamp:n#.z.T; price:50f+n?50f)];
    sequences[channel]:seq+n;
 };

.z.ts:{
    writeQuoteData[channel:`channel1;n:1+rand 9];
    writeQuoteData[channel:`channel2;n:1+rand 9];
    .quarkWrite.writeData[table:`memory;data:(flip `date`time`host`port`file`name!enlist each (.z.D;.z.T;.z.h;system "p";.z.f;`test)) ^ (flip enlist each .Q.w[])];
    .quarkWrite.timerTick[];
 };
.z.ts:{};

.z.pc:{.quarkWrite.onClose[]};

.z.exit:{.quarkWrite.onExit[]};
