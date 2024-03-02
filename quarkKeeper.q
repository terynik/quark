


tableNames:`t1`t2;
path:`$"/Users/nik/workspace/quark/db"
x:tableNames!{[d;p;t] .Q.par[d;;t] each p}[hsym[path];(key hsym[path]) except `sym;] each tableNames

x:{x[where {not () ~ key x} each x]} each x

{{meta get x} each x} each x

sym:get `:/Users/nik/workspace/quark/db/sym
\ts:1 show "start ",string .z.t; `s`t xasc `:/Users/nik/workspace/quark/db/2024.01.01/t1; show "complete ",string .z.t; 

select first i, last i by s from `:/Users/nik/workspace/quark/db/2024.01.01/t1
exec c from (meta `:/Users/nik/workspace/quark/db/2024.01.01/t1) where a=`s

.Q.vt
.Q.vp
.Q.p1
.Q.ps