# esync_log
synchronize by log between servers or data centers

##HOW TO
start application:  
```
    application:start(esync_log).
    esync_log:set_sync_receiver().
```  
then, run in app:  
`esync_log:start_sync("$sync_host$", 8766).`  
bidirectional synchronization is supported, run upper command on the peer edis with corresponding sync_host arguments


##TODO
1.http link restart support
2.heart beat support for restful request
3.ack for response of restful http
