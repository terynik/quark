system "l quarkQuery.q";

.quarkQuery.init[server:`:localhost:9981;path:`$"/Users/nik/workspace/quark/dbTest";realtime:1b];

.z.ts:{.quarkUtils.reconnect[.quarkQuery.instance]};

/.z.pg:{.quarkQuery.interceptSelect[x]};

sleep:{t:.z.p;while[.z.p<t+x]};
/sleep 00:00:30

/\x .z.pg
/.z.ts:{};

/(select diskMax:max sequence by channel from quote) ^ (select cacheMin:min sequence, cacheMax:max sequence by channel from .Q.dd[`.quarkCache;`quote])

/select from quote
/select max sequence by channel from quote
/select count i by channel,sequence from quote
/select from quote where channel=`channel1;
/select from quote where channel=`channel1, price > 50.0;
/select max sequence, max price, count distinct symbol by channel from quote

.quarkQuery.select1[query:"select sum price from quote"]
/.quarkQuery.select1[query:"select from quote where channel=`channel1"]
/.quarkQuery.select1[query:"select from quote where channel=`channel1, price > 50.0"]
/.quarkQuery.select1[query:"select max price, count distinct symbol by channel from quote"]
/.quarkQuery.select1[query:"select max price, count i by channel, symbol from quote"]

/n:10; seq:(select max sequence from quote where channel=`channel1)[0;`sequence];
/`.quarkCache.quote insert data:([]date:n#.z.D; channel:n#`channel1; sequence:seq+til n; symbol:n?`$'.Q.a; timestamp:n#.z.T; price:50f+n?50f)

/delete from `.quarkCache.quote
/select from .quarkCache.quote where channel=`channel1
/select from quote where channel=`channel1

