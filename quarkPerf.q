
.quarkPerf.segments:([segment:"s"$()] lastCheckpoint:"t"$());
.quarkPerf.checkpoints:([segment:"s"$(); checkpoint:"s"$(); parameter:"s"$()] execCount:"j"$(); execTime:"n"$());
.quarkPerf.resetTime:.z.t;

.quarkPerf.start:{[segment]

    upsert[`.quarkPerf.segments;(segment;.z.t)];
 };

.quarkPerf.check:{[segment;checkpoint;parameter]
    / get last checkpoing time for this segment, fail if it's null (meaning <.quarkPerf.start> was not called)
    if[not segment in key .quarkPerf.segments;1 "ERROR: missing start[`",string[segment],"] before calling check[",sv[",",string each (segment;checkpoint;parameter)],"]\n";:0Nn];
    s:.quarkPerf.segments[segment];

    if[not (segment;checkpoint;parameter) in key .quarkPerf.checkpoints;insert[`.quarkPerf.checkpoints;(segment;checkpoint;parameter;0j;00:00:00.000000000)]];
    c:.quarkPerf.checkpoints[(segment;checkpoint;parameter)];
    
    currentTime:.z.t; passedTime:currentTime-s[`lastCheckpoint];
    
    upsert[`.quarkPerf.segments;(segment;currentTime)];
    upsert[`.quarkPerf.checkpoints;(segment;checkpoint;parameter;c[`execCount]+1;c[`execTime]+passedTime)];
    
    :passedTime;    
 };

.quarkPerf.reset:{[]
    currentTime:.z.t; 
    result:`startTime`sampleTime xcols update startTime:get `.quarkPerf.resetTime, sampleTime:"n"$(currentTime-.quarkPerf.resetTime) from 0!select from .quarkPerf.checkpoints;
    if[00:00:30 > currentTime-.quarkPerf.resetTime;:0#result];
    delete from `.quarkPerf.checkpoints;
    set[`.quarkPerf.resetTime;currentTime];
    :result;
 };

/.quarkPerf.start[segment:`file]
/.quarkPerf.check[segment:`file;checkpoint:`point1;parameter:`p1]
/.quarkPerf.check[segment:`file;checkpoint:`point1;parameter:`p2]
/.quarkPerf.check[segment:`file;checkpoint:`point2]

/.quarkPerf.reset[]
