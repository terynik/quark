/ TODO: change <self> to be a reference
/ TODO: add <wrap> to catch all errors

/ <client> is a dictionary which should define:
/   <client[`handle]> - handle to active connection to the server or 0Nj otherwise;
/   <client[`server]> - server which we want to connect to;
/   <client[`connectHandler]> - a rank 1 lambda to call if physical connection to the server has been established;
/   <client[`disconnectHandler]> - a rank 1 lambda to call if physical connection to the server was lost.
/ both <client[`connectHandler]> and <client[`disconnectHandler]> will be called with <client> as a parameter
/   it's responsibility of these handlers to update global state
.quarkUtils.reconnect:{[client]
    / check if we *were* connected and *are* still connected, then ping
    if [client[`handle] in key .z.W;
        @[value client[`pingHandler];client;{1 "Ping handler threw an error (",x,")\n"}];
        :1b
    ];

    / check if we *were* connected but *are* disconnected now 
    if [not null client[`handle];
        1 "Detected disconnect of handle ",string[client[`handle]]," to ",string[client[`server]],"\n";
        client[`handle]:0Nj;
        @[value client[`disconnectHandler];client;{1 "Disconnect handler threw an error (",x,")\n"}];
        :0b;
    ];

    / now, it looks we are not connected, and it's exactly what we want to do... so let's do it
    1 "Trying to connect to ",string[client[`server]],"...";
    client[`handle]:@[{h:hopen[x];1 " connected as ",string[h],"\n";:h};client[`server];{1 " failed with: ",x,"\n";:0Nj}];

    / ok, it failed - maybe next time...
    if[null client[`handle];:0b];

    / finally, we are connected, try to call connect handler...
    status:@[{x[y];:1b}[client[`connectHandler];];client;{1 "Connect handler threw an error (",x,"), connection aborted\n";:0b}];

    / nope... connection without succesful initialisation doesn't make much sence, let's drop the connection and fail
    if [not status;@[hclose;client[`handle];{}];:0b];

    / at last, everything worked... breath out...
    :1b;  
 };
