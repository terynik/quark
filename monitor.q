.monitor.clients:1!flip `handle`lastTime!"it"$\:();

.monitor.connectClient:{
    `.monitor.clients insert (.z.w;.z.t);
    .monitor.updateClient[.z.w];
 };

.monitor.disconnectClient:{
    delete from `.monitor.clients where not handle in key .z.W;
 };

.monitor.updateClient:{[handle]
    if[0 = sum .z.W[handle];neg[handle] .j.j[.monitor.clients]];
 };

.monitor.timerTick:{
    .monitor.updateClient each exec handle from .monitor.clients;
 };

.monitor.htmlResponse:{
    :.h.hy[`html;sv["\n";read0[`:monitor.html]]];
 };

.monitor.initRuntime:{
    `.z.ph set .monitor.htmlResponse;
    `.z.wo set .monitor.connectClient;
    `.z.wc set .monitor.disconnectClient;
 };

/ debug
/\cd ..
.monitor.initRuntime[];

.z.ts:{ .monitor.timerTick[] };
.z.ph:{ show 0; .monitor.htmlResponse[] };
.z.wo:{ show 1; .monitor.connectClient[] };
.z.wc:{ .monitor.disconnectClient[] };
