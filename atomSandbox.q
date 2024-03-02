.Q.l[`$"/Users/nik/workspace/quark/dbPerf/"];
tables[]

select from memory


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

t1:([]date:(2024.01.01;2024.01.02);s:`a`b;t:(.z.T;.z.T);p:(1f;1f))
c:(=;`s;enlist `a)
?[`t1;enlist c;0b;()!()]

i:where eval {(x[0];$[-11h = type x[1];(`t1;enlist x[1]);x[1]];x[2])} c
t1[i]


parse "t1[`s]=`a"

.[{[o;t;c;b;a] {(x[0];$[-11h = type x[1];(`t1;enlist x[1]);x[1]];x[2])} each c };parse "select a from t1 where s = `a"]


.quarkCache.t1
select count i by date from t1

td:`t1;tm:`.quarkCache.t1;c:enlist (<;`s;enlist `k);b:0b;a:();

condition:c[0]

?[`.quarkCache.t1;c;0b;(enlist `i)!(enlist `i)]

parse "(value `.quarkCache.t1)[`s] < `k"
eval (<;((value;enlist `.quarkCache.t1);enlist `s);enlist `k)
condition

`s xgroup ?[td;c;0b;()],?[tm;c;0b;()]

{[td;tm;c] t[c[2]] }[t;] each c

parse "select count where p > 55f by s from t1"
select count where p > 55f by s from t1
select count where p > 55f by s from .quark.access.data.t1

parse "select from t1 where s<`c"
tables[]
\pwd
\l db

?[t;c;b;a]

/ the rest

\l db
tables[]
select from t1

{[func] .Q.dd[`.aeroq;func] set `libaeroq 2:(`$("aeroq_",string[func]);1)} each `init`subscribe`poll;

.aeroq_init:`libaeroq 2:(`aeroq_init;1)
.aeroq_subscribe:`libaeroq 2:(`aeroq_subscribe;1)
.aeroq_poll:`libaeroq 2:(`aeroq_poll;1)

.aeroq.init[1]
.aeroq.subscribe[1]
.aeroq.poll[1]

.aeroq.path:`:db

`.quark.tables insert (`t1;`:db;`date;`date`s`t`p;"dstf";();0Nt;0Nt)
/`$":quark-tables.csv" 0: csv 0: tableslist

.quark.tables
.quarkData.t1

.quark.writeData[table:`t1;data:([]date:enlist 2024.01.01;s:enlist `a;t:enlist .z.T;p:enlist 1f)];
.quark.flushData[table:`t1];

/ table:`t1


0N!{"[memory] ",", " sv {": " sv string each x} each key[x] ,' value[x]}[.Q.w[]]

.z.po:{`h set .z.w;}

\ls


k){[t;c;b;a]
    if[-11h=@t;t:. t];
    if[~qe[a]&qe[b]|-1h=@b;'`nyi];
    d:pv;
    v:$[q:0>@b;0;~#b;0;-11h=@v:*. b;pf~*`\:v;0]
    if[$[~#c;0;@*c;0;-11h=@x:c[0]1;pf~*`\:x;0];d@:&-6!*c;c:1_c]
    if[$[#c;0;(g:(. a)~,pf)|(. a)~,(#:;`i)];f:!a;j:dt[d]t;if[q;:+f!,$[g;?d@&0<j;,+/j]];if[v&1=#b;:?[+(pf,f)!(d;j)[;&0<j];();b;f!,(sum;*f)]]]
    if[~#d;d:pv@&pv=*|pv;c:,()];f:$[q;0#`;!b];g:$[#a;qa@*a;0]
    $[(1=#d)|$[q;~g;u&pf~*. b];
        $[~q;.q.xkey[f];b;?:;::]foo[t;c;b;a;v]d;
        (?).(
            foo[t;c;$[q;()!();b];*a;v]d;
            ();
            $[q;0b;f!f];
            *|a:$[g;ua a;(a;$[#a;(,/;)'k!k:!a;()])]
            )
    ]
 }
t1:([]s:enlist `a;t:enlist .z.T;p:enlist 1f)
t1:.Q.en[`:db;t1]
.Q.dpt[`:db;2024.01.01;`t1]
load `/Users/nik/workspace/quark/db/
key `:/Users/nik/workspace/quark/db/
tables[]
select from t1
.Q.l `db
[`:db]
key `:db
load `:db
\ls
\cd ..