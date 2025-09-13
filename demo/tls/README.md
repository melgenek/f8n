# TLS Demo

This demo illustrates a setup with mutual TLS (mTLS) connections between different components.

K3s, acting as a client, connects to F8N over HTTPs. F8N mimics an etcd server. 
Simultaneously, F8N acts as a client and connects to FoundationDB (FDB) via the FDB protocol with mTLS.

```ascii
+-----+      TLS       +-------------+      mTLS      +-----+
|     |                |             |                |     |
| K3S | <------------> |     F8N     | <------------> | FDB |
|     |                |             |                |     |
+-----+                +-------------+                +-----+
(client)               (etcd server)                  (server)
                       (fdb client)
```
