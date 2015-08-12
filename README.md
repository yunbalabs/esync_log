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
