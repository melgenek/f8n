# TLS Demo

This demo illustrates a setup with mutual TLS (mTLS) connections between different components.

K3s, acting as a client, connects to F8N. F8N mimics an etcd server. 
Simultaneously, F8N acts as a client and connects to FoundationDB (FDB). 
Both of these connections are secured using mTLS.

```ascii
+-----+      mTLS      +-------------+      mTLS      +-----+
|     |                |             |                |     |
| K3S | <------------> |     F8N     | <------------> | FDB |
|     |                |             |                |     |
+-----+                +-------------+                +-----+
(client)               (etcd server)                  (server)
                       (fdb client)
```
