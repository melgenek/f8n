package metrics

// FDBStatus represents the status of the FDB cluster.
//
// The original TypeScript definition can be found in the fdb-exporter project.
// This struct is a Go translation of that definition.
type FDBStatus struct {
	Client  *ClientStatus  `json:"client,omitempty"`
	Cluster *ClusterStatus `json:"cluster,omitempty"`
}

// ClientStatus represents the status of the FDB client.
type ClientStatus struct {
	Coordinators   *CoordinatorsStatus `json:"coordinators,omitempty"`
	DatabaseStatus *DatabaseStatus     `json:"database_status,omitempty"`
	Timestamp      int64               `json:"timestamp,omitempty"`
	Messages       []string            `json:"messages,omitempty"`
}

// CoordinatorsStatus represents the status of the FDB coordinators.
type CoordinatorsStatus struct {
	Coordinators    []*CoordinatorStatus `json:"coordinators,omitempty"`
	QuorumReachable bool                 `json:"quorum_reachable,omitempty"`
}

// CoordinatorStatus represents the status of a single FDB coordinator.
type CoordinatorStatus struct {
	Address   string `json:"address,omitempty"`
	Protocol  string `json:"protocol,omitempty"`
	Reachable bool   `json:"reachable,omitempty"`
}

// DatabaseStatus represents the status of the FDB database.
type DatabaseStatus struct {
	Available bool `json:"available,omitempty"`
	Healthy   bool `json:"healthy,omitempty"`
}

// ClusterStatus represents the status of the FDB cluster.
type ClusterStatus struct {
	DatabaseAvailable bool                `json:"database_available,omitempty"`
	DatabaseLockState *DatabaseLockState  `json:"database_lock_state,omitempty"`
	Clients           *Clients            `json:"clients,omitempty"`
	DatacenterLag     *DatacenterLag      `json:"datacenter_lag,omitempty"`
	LatencyProbe      *LatencyProbe       `json:"latency_probe,omitempty"`
	Workload          *Workload           `json:"workload,omitempty"`
	Data              *Data               `json:"data,omitempty"`
	QOS               *QOS                `json:"qos,omitempty"`
	RecoveryState     *RecoveryState      `json:"recovery_state,omitempty"`
	Processes         map[string]*Process `json:"processes,omitempty"`
}

// DatabaseLockState represents the lock state of the database.
type DatabaseLockState struct {
	Locked bool `json:"locked,omitempty"`
}

// Clients represents the client information.
type Clients struct {
	Count int `json:"count,omitempty"`
}

// DatacenterLag represents the lag of a datacenter.
type DatacenterLag struct {
	Seconds float64 `json:"seconds,omitempty"`
}

// LatencyProbe represents the latency probe metrics.
type LatencyProbe struct {
	ReadSeconds                              float64 `json:"read_seconds,omitempty"`
	CommitSeconds                            float64 `json:"commit_seconds,omitempty"`
	TransactionStartSeconds                  float64 `json:"transaction_start_seconds,omitempty"`
	ImmediatePriorityTransactionStartSeconds float64 `json:"immediate_priority_transaction_start_seconds,omitempty"`
	BatchPriorityTransactionStartSeconds     float64 `json:"batch_priority_transaction_start_seconds,omitempty"`
}

// Workload represents the workload metrics.
type Workload struct {
	Operations   *Operations   `json:"operations,omitempty"`
	Transactions *Transactions `json:"transactions,omitempty"`
	Keys         *Keys         `json:"keys,omitempty"`
	Bytes        *Bytes        `json:"bytes,omitempty"`
}

// Operations represents the workload operations.
type Operations struct {
	ReadRequests     *Counter `json:"read_requests,omitempty"`
	Reads            *Counter `json:"reads,omitempty"`
	Writes           *Counter `json:"writes,omitempty"`
	LocationRequests *Counter `json:"location_requests,omitempty"`
	LowPriorityReads *Counter `json:"low_priority_reads,omitempty"`
	MemoryErrors     *Counter `json:"memory_errors,omitempty"`
}

// Transactions represents the workload transactions.
type Transactions struct {
	Started                  *Counter `json:"started,omitempty"`
	Committed                *Counter `json:"committed,omitempty"`
	Conflicted               *Counter `json:"conflicted,omitempty"`
	RejectedForQueuedTooLong *Counter `json:"rejected_for_queued_too_long,omitempty"`
}

// Keys represents the workload keys.
type Keys struct {
	Read *Counter `json:"read,omitempty"`
}

// Bytes represents the workload bytes.
type Bytes struct {
	Read    *Counter `json:"read,omitempty"`
	Written *Counter `json:"written,omitempty"`
}

// Counter represents a counter metric.
type Counter struct {
	Counter float64 `json:"counter,omitempty"`
}

// Data represents the data metrics.
type Data struct {
	State                                 *State      `json:"state,omitempty"`
	LeastOperatingSpaceBytesLogServer     int64       `json:"least_operating_space_bytes_log_server,omitempty"`
	LeastOperatingSpaceBytesStorageServer int64       `json:"least_operating_space_bytes_storage_server,omitempty"`
	AveragePartitionSizeBytes             int64       `json:"average_partition_size_bytes,omitempty"`
	PartitionsCount                       int         `json:"partitions_count,omitempty"`
	TotalDiskUsedBytes                    int64       `json:"total_disk_used_bytes,omitempty"`
	TotalKVSizeBytes                      int64       `json:"total_kv_size_bytes,omitempty"`
	SystemKVSizeBytes                     int64       `json:"system_kv_size_bytes,omitempty"`
	MovingData                            *MovingData `json:"moving_data,omitempty"`
}

// State represents the state of the data.
type State struct {
	Name string `json:"name,omitempty"`
}

// MovingData represents the moving data metrics.
type MovingData struct {
	InFlightBytes     int64 `json:"in_flight_bytes,omitempty"`
	InQueueBytes      int64 `json:"in_queue_bytes,omitempty"`
	TotalWrittenBytes int64 `json:"total_written_bytes,omitempty"`
}

// QOS represents the quality of service metrics.
type QOS struct {
	LimitingDataLagStorageServer       *Seconds       `json:"limiting_data_lag_storage_server,omitempty"`
	LimitingDurabilityLagStorageServer *Seconds       `json:"limiting_durability_lag_storage_server,omitempty"`
	LimitingStorageServerQueueBytes    int64          `json:"limiting_storage_server_queue_bytes,omitempty"`
	LimitingVersionLagStorageServer    int64          `json:"limiting_version_lag_storage_server,omitempty"`
	WorstDataLagStorageServer          *Seconds       `json:"worst_data_lag_storage_server,omitempty"`
	WorstDurabilityLagStorageServer    *Seconds       `json:"worst_durability_lag_storage_server,omitempty"`
	WorstStorageServerQueueBytes       int64          `json:"worst_storage_server_queue_bytes,omitempty"`
	WorstLogServerQueueBytes           int64          `json:"worst_log_server_queue_bytes,omitempty"`
	WorstVersionLagStorageServer       int64          `json:"worst_version_lag_storage_server,omitempty"`
	ReleasedTransactionsPerSecond      float64        `json:"released_transactions_per_second,omitempty"`
	TransactionsPerSecondLimit         float64        `json:"transactions_per_second_limit,omitempty"`
	PerformanceLimitedBy               *LimitedBy     `json:"performance_limited_by,omitempty"`
	BatchReleasedTransactionsPerSecond float64        `json:"batch_released_transactions_per_second,omitempty"`
	BatchTransactionsPerSecondLimit    float64        `json:"batch_transactions_per_second_limit,omitempty"`
	BatchPerformanceLimitedBy          *LimitedBy     `json:"batch_performance_limited_by,omitempty"`
	ThrottledTags                      *ThrottledTags `json:"throttled_tags,omitempty"`
}

// Seconds represents a duration in seconds.
type Seconds struct {
	Seconds float64 `json:"seconds,omitempty"`
}

// LimitedBy represents the reason for limited performance.
type LimitedBy struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
}

// ThrottledTags represents the throttled tags metrics.
type ThrottledTags struct {
	Auto   *ThrottledTag `json:"auto,omitempty"`
	Manual *ThrottledTag `json:"manual,omitempty"`
}

// ThrottledTag represents a throttled tag.
type ThrottledTag struct {
	Count     int `json:"count,omitempty"`
	BusyRead  int `json:"busy_read,omitempty"`
	BusyWrite int `json:"busy_write,omitempty"`
}

// RecoveryState represents the recovery state metrics.
type RecoveryState struct {
	Name                      string  `json:"name,omitempty"`
	Description               string  `json:"description,omitempty"`
	ActiveGenerations         int     `json:"active_generations,omitempty"`
	SecondsSinceLastRecovered float64 `json:"seconds_since_last_recovered,omitempty"`
}

// Process represents a process in the FDB cluster.
type Process struct {
	Address       string   `json:"address,omitempty"`
	ClassType     string   `json:"class_type,omitempty"`
	UptimeSeconds float64  `json:"uptime_seconds,omitempty"`
	Degraded      bool     `json:"degraded,omitempty"`
	RunLoopBusy   float64  `json:"run_loop_busy,omitempty"`
	CPU           *CPU     `json:"cpu,omitempty"`
	Memory        *Memory  `json:"memory,omitempty"`
	Disk          *Disk    `json:"disk,omitempty"`
	Network       *Network `json:"network,omitempty"`
	Roles         []*Role  `json:"roles,omitempty"`
}

// CPU represents the CPU metrics of a process.
type CPU struct {
	UsageCores float64 `json:"usage_cores,omitempty"`
}

// Memory represents the memory metrics of a process.
type Memory struct {
	AvailableBytes        int64 `json:"available_bytes,omitempty"`
	UsedBytes             int64 `json:"used_bytes,omitempty"`
	LimitBytes            int64 `json:"limit_bytes,omitempty"`
	UnusedAllocatedMemory int64 `json:"unused_allocated_memory,omitempty"`
	RssBytes              int64 `json:"rss_bytes,omitempty"`
}

// Disk represents the disk metrics of a process.
type Disk struct {
	Busy       float64  `json:"busy,omitempty"`
	FreeBytes  int64    `json:"free_bytes,omitempty"`
	TotalBytes int64    `json:"total_bytes,omitempty"`
	Reads      *Counter `json:"reads,omitempty"`
	Writes     *Counter `json:"writes,omitempty"`
}

// Network represents the network metrics of a process.
type Network struct {
	CurrentConnections     int   `json:"current_connections,omitempty"`
	ConnectionErrors       *Rate `json:"connection_errors,omitempty"`
	ConnectionsClosed      *Rate `json:"connections_closed,omitempty"`
	ConnectionsEstablished *Rate `json:"connections_established,omitempty"`
	MegabitsReceived       *Rate `json:"megabits_received,omitempty"`
	MegabitsSent           *Rate `json:"megabits_sent,omitempty"`
}

// Rate represents a rate metric.
type Rate struct {
	Hz float64 `json:"hz,omitempty"`
}

// Role represents a role of a process.
type Role struct {
	Role string `json:"role,omitempty"`

	// Storage role
	DataLag               *Seconds      `json:"data_lag,omitempty"`
	DurabilityLag         *Seconds      `json:"durability_lag,omitempty"`
	InputBytes            *Counter      `json:"input_bytes,omitempty"`
	DurableBytes          *Counter      `json:"durable_bytes,omitempty"`
	StoredBytes           int64         `json:"stored_bytes,omitempty"`
	KvstoreAvailableBytes int64         `json:"kvstore_available_bytes,omitempty"`
	KvstoreFreeBytes      int64         `json:"kvstore_free_bytes,omitempty"`
	KvstoreTotalBytes     int64         `json:"kvstore_total_bytes,omitempty"`
	KvstoreUsedBytes      int64         `json:"kvstore_used_bytes,omitempty"`
	FetchedVersions       *Counter      `json:"fetched_versions,omitempty"`
	FetchesFromLogs       *Counter      `json:"fetches_from_logs,omitempty"`
	LowPriorityQueries    *Counter      `json:"low_priority_queries,omitempty"`
	ReadLatencyStatistics *LatencyStats `json:"read_latency_statistics,omitempty"`

	// Log role
	QueueDiskAvailableBytes int64 `json:"queue_disk_available_bytes,omitempty"`
	QueueDiskFreeBytes      int64 `json:"queue_disk_free_bytes,omitempty"`
	QueueDiskTotalBytes     int64 `json:"queue_disk_total_bytes,omitempty"`
	QueueDiskUsedBytes      int64 `json:"queue_disk_used_bytes,omitempty"`

	// Commit Proxy role
	CommitBatchingWindowSize *LatencyStats `json:"commit_batching_window_size,omitempty"`
	CommitLatencyStatistics  *LatencyStats `json:"commit_latency_statistics,omitempty"`

	// GRV Proxy role
	GrvLatencyStatistics *GrvLatencyStats `json:"grv_latency_statistics,omitempty"`
}

// LatencyStats represents latency statistics.
type LatencyStats struct {
	Mean   float64 `json:"mean,omitempty"`
	Median float64 `json:"median,omitempty"`
	P95    float64 `json:"p95,omitempty"`
	P99    float64 `json:"p99,omitempty"`
	Max    float64 `json:"max,omitempty"`
}

// GrvLatencyStats represents GRV latency statistics.
type GrvLatencyStats struct {
	Batch   *LatencyStats `json:"batch,omitempty"`
	Default *LatencyStats `json:"default,omitempty"`
}
