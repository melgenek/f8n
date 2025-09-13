# Demo Setup

This demo sets up a FoundationDB cluster (`fdb-demo`), an F8N API service (`f8n-demo`), and a K3s Kubernetes server (`k3s-demo`) using Docker Compose.

By default, if the `--endpoint` flag is not specified, the F8N service expects the `fdb.cluster` configuration file to be located at `/etc/foundationdb/fdb.cluster` inside the container.
