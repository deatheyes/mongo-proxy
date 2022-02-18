# mongo-proxy
A multi-tenancy mongo-proxy consolidating multiple physical clusters

## Architecture

    client  ->   handleFuncChain [HandleCommand   ----> Transform]
                                   |                    |
        <--------------	    block and process      has response? ---> mongo
                                                        | Y              |
        <---------------------------------------     return              |
                                                                         |
        <---------------------------------------     revert     <--- response

* Authentication and handshake would be blocked and processed by mongo-proxy. 
* A meta server is employed to help mongo-proxy to route connections and requests. 
* Necessary transformations would be applyed to requests and responses to shield the topology of physical clusters.

## Modules
### Wire
Parser of mongodb wire protocal

### Topology
Connection and endpoint implements.

### Proxy
Transformations and router of request and response

### Meta
Topology of physical clusters. Only mongodb suppoerted currently.

### Operation
Commands that should be processed locally, such as ismaster, handshake.

### Auth
Sasl authenticator handler. Only SCRAMSHA256 supported currently.
