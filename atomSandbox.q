.Q.l[`$"/Users/nik/workspace/quark/dbPerf"]; 
tables[]

meta cputime 
select from memory
select from cputime

`date`time xdesc select date, time, used, heap, peak, 100.0 * used % peak from memory
`date`startTime xdesc select sum execTime, first sampleTime, 100.0 * ("j"$sum execTime) % ("j"$first sampleTime) by date,startTime,checkpoint from cputime

.z.i

.Q

\t 1000
\t 1
\t 0

.quarkCapture.initTargets[pathToConfigFile:`$":quark-tables.csv"];
.quarkCapture.tables
.quarkCapture.listeners

.quarkCapture.writeData[table:`t1;data:([]date:enlist 2024.01.01;s:enlist `a;t:enlist .z.T;p:enlist 1f)];
.quarkCapture.writeData[table:`t1;data:([]date:(2024.01.01;2024.01.02);s:`a`b;t:(.z.T;.z.T);p:(1f;1f))];
.quarkCapture.flushData[table:`t1];
.quarkCache.t1

.quarkCapture.writeData[table:`t2;data:([]date:enlist 2024.01.01;s:enlist `a)];
.quarkCapture.writeData[table:`t2;data:([]date:(2024.01.01;2024.01.02);s:`a`b)];
.quarkCapture.flushData[table:`t2];
.quarkCache.t2

.quark.flushDatabase[db:`:.];

/ merge two tables

