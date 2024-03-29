system "l write.q";
system "l monitor.q";

.write.cleanUpTables[];
.write.loadTableConfig[pathToConfigFile:`$":tablesTest.csv"];
.write.loadTableConfig[pathToConfigFile:`$":tablesPerf.csv"];

/ load database and change back to the original directory
/   ...where is mistical .Q.lo (https://code.kx.com/q/ref/dotq/#lo-load-without)?
.Q.l[`$"/Users/nik/workspace/quark/dbTest"]; 

if[.Q.qt[`quote];
    sequences:0!select last sequence by channel from quote where date=last date;
    `sequences set sequences[`channel]!sequences[`sequence]
 ];
if[not .Q.qt[`quote];
    `sequences set ()!()
 ];

.write.writeData[table:`status;data:([]date:1#.z.D; channel:1#`statusChannel; sequence:1#0; symbol:1#`x; timestamp:1#.z.T; status:1#`start)];
.write.flushAll[currentTime:.z.t;force:1b];

writeQuoteData:{[channel;n]
    seq:$[channel in key sequences;sequences[channel];0j];
    .write.writeData[table:`quote;data:([]date:n#.z.D; channel:n#channel; sequence:seq+til n; symbol:n?`$'.Q.a; timestamp:n#.z.T; price:50f+n?50f)];
    sequences[channel]:seq+n;
 };

/ no work
enableChannel1:0b;
enableChannel2:0b;
enableMonitor:0b;
.z.ts:{};

/ let's go
enableChannel1:1b;
enableChannel2:1b;
enableMonitor:1b;

.z.ts:{
    if[enableChannel1;
        writeQuoteData[channel:`channel1;n:rand 10]
    ];
    if[enableChannel2;
        writeQuoteData[channel:`channel2;n:rand 10]
    ];
    if[enableMonitor;
        .write.writeData[name:`memory;data:(flip `date`time`file`host`port`pid!enlist each (.z.D;.z.T;.z.f;.z.h;system "p";.z.i)) ^ (flip enlist each .Q.w[])];
        .write.writeData[name:`cputime;data:`date`time`file`host`port`pid xcols update date:.z.D, time:.z.T, file:.z.f, host:.z.h, port:system "p", pid:.z.i from .quarkPerf.reset[]];
    ];
    .write.timerTick[];
 };

.z.pc:{.write.onClose[]};

.z.exit:{.write.onExit[]};

