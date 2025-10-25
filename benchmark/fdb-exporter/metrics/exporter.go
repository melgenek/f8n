package metrics

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// Exporter collects FDB metrics and exports them to Prometheus.
//
// It implements the prometheus.Collector interface.
type Exporter struct {
	db fdb.Database

	mu sync.Mutex

	// FDB metrics
	fdbDatabaseAvailable                                    *prometheus.Desc
	fdbDatabaseLocked                                       *prometheus.Desc
	fdbClientsTotal                                         *prometheus.Desc
	fdbDatacenterLagSeconds                                 *prometheus.Desc
	fdbLatencyProbeReadSeconds                              *prometheus.Desc
	fdbLatencyProbeCommitSeconds                            *prometheus.Desc
	fdbLatencyProbeTransactionStartSeconds                  *prometheus.Desc
	fdbLatencyProbeImmediatePriorityTransactionStartSeconds *prometheus.Desc
	fdbLatencyProbeBatchPriorityTransactionStartSeconds     *prometheus.Desc
	fdbWorkloadOperationsReadRequestsTotal                  *prometheus.Desc
	fdbWorkloadOperationsReadsTotal                         *prometheus.Desc
	fdbWorkloadOperationsWritesTotal                        *prometheus.Desc
	fdbWorkloadOperationsLocationRequestsTotal              *prometheus.Desc
	fdbWorkloadOperationsLowPriorityReadsTotal              *prometheus.Desc
	fdbWorkloadOperationsMemoryErrorsTotal                  *prometheus.Desc
	fdbWorkloadTransactionsStartedTotal                     *prometheus.Desc
	fdbWorkloadTransactionsCommittedTotal                   *prometheus.Desc
	fdbWorkloadTransactionsConflictedTotal                  *prometheus.Desc
	fdbWorkloadTransactionsRejectedForQueuedTooLongTotal    *prometheus.Desc
	fdbWorkloadKeysReadTotal                                *prometheus.Desc
	fdbWorkloadBytesReadTotal                               *prometheus.Desc
	fdbWorkloadBytesWrittenTotal                            *prometheus.Desc
	fdbDataState                                            *prometheus.Desc
	fdbDataLeastOperatingSpaceLogServerBytes                *prometheus.Desc
	fdbDataLeastOperatingSpaceStorageServerBytes            *prometheus.Desc
	fdbDataAveragePartitionSizeBytes                        *prometheus.Desc
	fdbDataPartitionsTotal                                  *prometheus.Desc
	fdbDataTotalDiskUsedBytes                               *prometheus.Desc
	fdbDataTotalKVSizeBytes                                 *prometheus.Desc
	fdbDataSystemKVSizeBytes                                *prometheus.Desc
	fdbDataMovingInFlightBytes                              *prometheus.Desc
	fdbDataMovingInQueueBytes                               *prometheus.Desc
	fdbDataMovingTotalWrittenBytes                          *prometheus.Desc
	fdbQosLimitingStorageServerDataLagSeconds               *prometheus.Desc
	fdbQosLimitingDurabilityLagStorageServerSeconds         *prometheus.Desc
	fdbQosLimitingStorageServerQueueBytes                   *prometheus.Desc
	fdbQosLimitingVersionLagStorageServer                   *prometheus.Desc
	fdbQosWorstStorageServerDataLagSeconds                  *prometheus.Desc
	fdbQosWorstDurabilityLagStorageServerSeconds            *prometheus.Desc
	fdbQosWorstStorageServerQueueBytes                      *prometheus.Desc
	fdbQosWorstLogServerQueueBytes                          *prometheus.Desc
	fdbQosWorstVersionLagStorageServer                      *prometheus.Desc
	fdbQosReleasedTransactionsPerSecond                     *prometheus.Desc
	fdbQosTransactionsPerSecondLimit                        *prometheus.Desc
	fdbQosPerformanceLimitedBy                              *prometheus.Desc
	fdbQosBatchReleasedTransactionsPerSecond                *prometheus.Desc
	fdbQosBatchTransactionsPerSecondLimit                   *prometheus.Desc
	fdbQosBatchPerformanceLimitedBy                         *prometheus.Desc
	fdbQosThrottledTagsAutoCount                            *prometheus.Desc
	fdbQosThrottledTagsAutoBusyRead                         *prometheus.Desc
	fdbQosThrottledTagsAutoBusyWrite                        *prometheus.Desc
	fdbQosThrottledTagsManualCount                          *prometheus.Desc
	fdbRecoveryState                                        *prometheus.Desc
	fdbRecoveryStateActiveGenerations                       *prometheus.Desc
	fdbRecoveryStateSecondsSinceLastRecovered               *prometheus.Desc
	fdbProcessUptimeSeconds                                 *prometheus.Desc
	fdbProcessClass                                         *prometheus.Desc
	fdbProcessDegraded                                      *prometheus.Desc
	fdbProcessRunLoopBusyRatio                              *prometheus.Desc
	fdbProcessCPUUsageCores                                 *prometheus.Desc
	fdbProcessMemoryAvailableBytes                          *prometheus.Desc
	fdbProcessMemoryUsedBytes                               *prometheus.Desc
	fdbProcessMemoryLimitBytes                              *prometheus.Desc
	fdbProcessMemoryUnusedAllocatedMemory                   *prometheus.Desc
	fdbProcessMemoryRssBytes                                *prometheus.Desc
	fdbProcessDiskUsageRatio                                *prometheus.Desc
	fdbProcessDiskFreeBytes                                 *prometheus.Desc
	fdbProcessDiskTotalBytes                                *prometheus.Desc
	fdbProcessDiskReadsTotal                                *prometheus.Desc
	fdbProcessDiskWritesTotal                               *prometheus.Desc
	fdbProcessNetworkCurrentConnectionsTotal                *prometheus.Desc
	fdbProcessNetworkConnectionErrorsRate                   *prometheus.Desc
	fdbProcessNetworkConnectionsClosedRate                  *prometheus.Desc
	fdbProcessNetworkConnectionsEstablishedRate             *prometheus.Desc
	fdbProcessNetworkMegabitsReceivedRate                   *prometheus.Desc
	fdbProcessNetworkMegabitsSentRate                       *prometheus.Desc
	fdbProcessRole                                          *prometheus.Desc
	fdbProcessStorageDataLagSeconds                         *prometheus.Desc
	fdbProcessStorageDurabilityLagSeconds                   *prometheus.Desc
	fdbProcessStorageInputBytesTotal                        *prometheus.Desc
	fdbProcessStorageDurableBytesTotal                      *prometheus.Desc
	fdbProcessStorageStoredBytes                            *prometheus.Desc
	fdbProcessStorageKvstoreAvailableBytes                  *prometheus.Desc
	fdbProcessStorageKvstoreFreeBytes                       *prometheus.Desc
	fdbProcessStorageKvstoreTotalBytes                      *prometheus.Desc
	fdbProcessStorageKvstoreUsedBytes                       *prometheus.Desc
	fdbProcessStorageFetchedVersionsTotal                   *prometheus.Desc
	fdbProcessStorageFetchesFromLogsTotal                   *prometheus.Desc
	fdbProcessStorageLowPriorityQueriesTotal                *prometheus.Desc
	fdbProcessStorageReadLatencyMean                        *prometheus.Desc
	fdbProcessStorageReadLatencyMedian                      *prometheus.Desc
	fdbProcessStorageReadLatencyP95                         *prometheus.Desc
	fdbProcessStorageReadLatencyP99                         *prometheus.Desc
	fdbProcessStorageReadLatencyMax                         *prometheus.Desc
	fdbProcessLogQueueDiskAvailableBytes                    *prometheus.Desc
	fdbProcessLogQueueDiskFreeBytes                         *prometheus.Desc
	fdbProcessLogQueueDiskTotalBytes                        *prometheus.Desc
	fdbProcessLogQueueDiskUsedBytes                         *prometheus.Desc
	fdbProcessLogInputBytesTotal                            *prometheus.Desc
	fdbProcessLogDurableBytesTotal                          *prometheus.Desc
	fdbProcessCommitProxyBatchingWindowSizeMean             *prometheus.Desc
	fdbProcessCommitProxyBatchingWindowSizeMedian           *prometheus.Desc
	fdbProcessCommitProxyBatchingWindowSizeP95              *prometheus.Desc
	fdbProcessCommitProxyBatchingWindowSizeP99              *prometheus.Desc
	fdbProcessCommitProxyCommitLatencyMean                  *prometheus.Desc
	fdbProcessCommitProxyCommitLatencyMedian                *prometheus.Desc
	fdbProcessCommitProxyCommitLatencyP95                   *prometheus.Desc
	fdbProcessCommitProxyCommitLatencyP99                   *prometheus.Desc
	fdbProcessCommitProxyCommitLatencyMax                   *prometheus.Desc
	fdbProcessGrvProxyBatchLatencyMean                      *prometheus.Desc
	fdbProcessGrvProxyBatchLatencyMedian                    *prometheus.Desc
	fdbProcessGrvProxyBatchLatencyP95                       *prometheus.Desc
	fdbProcessGrvProxyBatchLatencyP99                       *prometheus.Desc
	fdbProcessGrvProxyDefaultLatencyMean                    *prometheus.Desc
	fdbProcessGrvProxyDefaultLatencyMedian                  *prometheus.Desc
	fdbProcessGrvProxyDefaultLatencyP95                     *prometheus.Desc
	fdbProcessGrvProxyDefaultLatencyP99                     *prometheus.Desc
	fdbProcessGrvProxyDefaultLatencyMax                     *prometheus.Desc
}

// NewExporter returns a new FDB exporter.
func NewExporter(db fdb.Database) *Exporter {
	return &Exporter{
		db: db,

		fdbDatabaseAvailable: prometheus.NewDesc(
			"fdb_database_available",
			"Whether or not database is available. 1 if is, 0 otherwise",
			nil, nil,
		),
		fdbDatabaseLocked: prometheus.NewDesc(
			"fdb_database_locked",
			"Whether or not database is locked. 1 if is, 0 otherwise",
			nil, nil,
		),
		fdbClientsTotal: prometheus.NewDesc(
			"fdb_clients_total",
			"Count of clients",
			nil, nil,
		),
		fdbDatacenterLagSeconds: prometheus.NewDesc(
			"fdb_datacenter_lag_seconds",
			"Datacenter lag in seconds",
			nil, nil,
		),
		fdbLatencyProbeReadSeconds: prometheus.NewDesc(
			"fdb_latency_probe_read_seconds",
			"Time to perform a single read",
			nil, nil,
		),
		fdbLatencyProbeCommitSeconds: prometheus.NewDesc(
			"fdb_latency_probe_commit_seconds",
			"Time to commit a sample transaction",
			nil, nil,
		),
		fdbLatencyProbeTransactionStartSeconds: prometheus.NewDesc(
			"fdb_latency_probe_transaction_start_seconds",
			"Time to start a sample transaction at normal priority",
			nil, nil,
		),
		fdbLatencyProbeImmediatePriorityTransactionStartSeconds: prometheus.NewDesc(
			"fdb_latency_probe_immediate_priority_transaction_start_seconds",
			"Time to start a sample transaction at system immediate priority",
			nil, nil,
		),
		fdbLatencyProbeBatchPriorityTransactionStartSeconds: prometheus.NewDesc(
			"fdb_latency_probe_batch_priority_transaction_start_seconds",
			"Time to start a sample transaction at batch priority",
			nil, nil,
		),
		fdbWorkloadOperationsReadRequestsTotal: prometheus.NewDesc(
			"fdb_workload_operations_read_requests_total",
			"Count of workload read request operations",
			nil, nil,
		),
		fdbWorkloadOperationsReadsTotal: prometheus.NewDesc(
			"fdb_workload_operations_reads_total",
			"Count of workload read operations",
			nil, nil,
		),
		fdbWorkloadOperationsWritesTotal: prometheus.NewDesc(
			"fdb_workload_operations_writes_total",
			"Count of workload write operations",
			nil, nil,
		),
		fdbWorkloadOperationsLocationRequestsTotal: prometheus.NewDesc(
			"fdb_workload_operations_location_requests_total",
			"Count of workload location request operations",
			nil, nil,
		),
		fdbWorkloadOperationsLowPriorityReadsTotal: prometheus.NewDesc(
			"fdb_workload_operations_low_priority_reads_total",
			"Count of workload low priority read operations",
			nil, nil,
		),
		fdbWorkloadOperationsMemoryErrorsTotal: prometheus.NewDesc(
			"fdb_workload_operations_memory_errors_total",
			"Count of workload memory errors",
			nil, nil,
		),
		fdbWorkloadTransactionsStartedTotal: prometheus.NewDesc(
			"fdb_workload_transactions_started_total",
			"Count of workload started transactions",
			nil, nil,
		),
		fdbWorkloadTransactionsCommittedTotal: prometheus.NewDesc(
			"fdb_workload_transactions_committed_total",
			"Count of workload committed transactions",
			nil, nil,
		),
		fdbWorkloadTransactionsConflictedTotal: prometheus.NewDesc(
			"fdb_workload_transactions_conflicted_total",
			"Count of workload conflicted transactions",
			nil, nil,
		),
		fdbWorkloadTransactionsRejectedForQueuedTooLongTotal: prometheus.NewDesc(
			"fdb_workload_transactions_rejected_for_queued_too_long_total",
			"Count of workload transactions rejected for being queued too long",
			nil, nil,
		),
		fdbWorkloadKeysReadTotal: prometheus.NewDesc(
			"fdb_workload_keys_read_total",
			"Count of workload keys read",
			nil, nil,
		),
		fdbWorkloadBytesReadTotal: prometheus.NewDesc(
			"fdb_workload_bytes_read_total",
			"Count of workload bytes read",
			nil, nil,
		),
		fdbWorkloadBytesWrittenTotal: prometheus.NewDesc(
			"fdb_workload_bytes_written_total",
			"Count of workload bytes written",
			nil, nil,
		),
		fdbDataState: prometheus.NewDesc(
			"fdb_data_state",
			"Indicates data state",
			[]string{"state"}, nil,
		),
		fdbDataLeastOperatingSpaceLogServerBytes: prometheus.NewDesc(
			"fdb_data_least_operating_space_log_server_bytes",
			"Operating space on most full log server",
			nil, nil,
		),
		fdbDataLeastOperatingSpaceStorageServerBytes: prometheus.NewDesc(
			"fdb_data_least_operating_space_storage_server_bytes",
			"Operating space on most full storage server",
			nil, nil,
		),
		fdbDataAveragePartitionSizeBytes: prometheus.NewDesc(
			"fdb_data_average_partition_size_bytes",
			"Average partition size",
			nil, nil,
		),
		fdbDataPartitionsTotal: prometheus.NewDesc(
			"fdb_data_partitions_total",
			"Partitions count",
			nil, nil,
		),
		fdbDataTotalDiskUsedBytes: prometheus.NewDesc(
			"fdb_data_total_disk_used_bytes",
			"Total disk used",
			nil, nil,
		),
		fdbDataTotalKVSizeBytes: prometheus.NewDesc(
			"fdb_data_total_kv_size_bytes",
			"Total KV size",
			nil, nil,
		),
		fdbDataSystemKVSizeBytes: prometheus.NewDesc(
			"fdb_data_system_kv_size_bytes",
			"System KV size",
			nil, nil,
		),
		fdbDataMovingInFlightBytes: prometheus.NewDesc(
			"fdb_data_moving_in_flight_bytes",
			"Moving data in-flight bytes",
			nil, nil,
		),
		fdbDataMovingInQueueBytes: prometheus.NewDesc(
			"fdb_data_moving_in_queue_bytes",
			"Moving data in-queue bytes",
			nil, nil,
		),
		fdbDataMovingTotalWrittenBytes: prometheus.NewDesc(
			"fdb_data_moving_total_written_bytes",
			"Moving data total written bytes",
			nil, nil,
		),
		fdbQosLimitingStorageServerDataLagSeconds: prometheus.NewDesc(
			"fdb_qos_limiting_storage_server_data_lag_seconds",
			"QoS limiting data lag among storage servers",
			nil, nil,
		),
		fdbQosLimitingDurabilityLagStorageServerSeconds: prometheus.NewDesc(
			"fdb_qos_limiting_storage_server_durability_lag_seconds",
			"QoS limiting durability lag among storage servers",
			nil, nil,
		),
		fdbQosLimitingStorageServerQueueBytes: prometheus.NewDesc(
			"fdb_qos_limiting_storage_server_queue_bytes",
			"QoS limiting queue bytes among storage servers",
			nil, nil,
		),
		fdbQosLimitingVersionLagStorageServer: prometheus.NewDesc(
			"fdb_qos_limiting_version_lag_storage_server",
			"QoS limiting version lag among storage servers",
			nil, nil,
		),
		fdbQosWorstStorageServerDataLagSeconds: prometheus.NewDesc(
			"fdb_qos_worst_storage_server_data_lag_seconds",
			"QoS worst data lag among storage servers",
			nil, nil,
		),
		fdbQosWorstDurabilityLagStorageServerSeconds: prometheus.NewDesc(
			"fdb_qos_worst_storage_server_durability_lag_seconds",
			"QoS worst durability lag among storage servers",
			nil, nil,
		),
		fdbQosWorstStorageServerQueueBytes: prometheus.NewDesc(
			"fdb_qos_worst_storage_server_queue_bytes",
			"QoS worst queue bytes among storage servers",
			nil, nil,
		),
		fdbQosWorstLogServerQueueBytes: prometheus.NewDesc(
			"fdb_qos_worst_log_server_queue_bytes",
			"QoS worst queue bytes among log servers",
			nil, nil,
		),
		fdbQosWorstVersionLagStorageServer: prometheus.NewDesc(
			"fdb_qos_worst_version_lag_storage_server",
			"QoS worst version lag among storage servers",
			nil, nil,
		),
		fdbQosReleasedTransactionsPerSecond: prometheus.NewDesc(
			"fdb_qos_released_transactions_per_second",
			"QoS released transactions per second",
			nil, nil,
		),
		fdbQosTransactionsPerSecondLimit: prometheus.NewDesc(
			"fdb_qos_transactions_per_second_limit",
			"QoS transactions per second limit",
			nil, nil,
		),
		fdbQosPerformanceLimitedBy: prometheus.NewDesc(
			"fdb_qos_performance_limited_by",
			"Indicates the reason for limited performance",
			[]string{"name", "description"}, nil,
		),
		fdbQosBatchReleasedTransactionsPerSecond: prometheus.NewDesc(
			"fdb_qos_batch_released_transactions_per_second",
			"QoS released transactions per second",
			nil, nil,
		),
		fdbQosBatchTransactionsPerSecondLimit: prometheus.NewDesc(
			"fdb_qos_batch_transactions_per_second_limit",
			"QoS transactions per second limit",
			nil, nil,
		),
		fdbQosBatchPerformanceLimitedBy: prometheus.NewDesc(
			"fdb_qos_batch_performance_limited_by",
			"Indicates the reason for limited performance",
			[]string{"name", "description"}, nil,
		),
		fdbQosThrottledTagsAutoCount: prometheus.NewDesc(
			"fdb_qos_throttled_tags_auto_count",
			"Number of automatically throttled tags",
			nil, nil,
		),
		fdbQosThrottledTagsAutoBusyRead: prometheus.NewDesc(
			"fdb_qos_throttled_tags_auto_busy_read",
			"Number of automatically throttled tags for busy reads",
			nil, nil,
		),
		fdbQosThrottledTagsAutoBusyWrite: prometheus.NewDesc(
			"fdb_qos_throttled_tags_auto_busy_write",
			"Number of automatically throttled tags for busy writes",
			nil, nil,
		),
		fdbQosThrottledTagsManualCount: prometheus.NewDesc(
			"fdb_qos_throttled_tags_manual_count",
			"Number of manually throttled tags",
			nil, nil,
		),
		fdbRecoveryState: prometheus.NewDesc(
			"fdb_recovery_state",
			"Recovery state info",
			[]string{"name", "description"}, nil,
		),
		fdbRecoveryStateActiveGenerations: prometheus.NewDesc(
			"fdb_recovery_state_active_generations",
			"Recovery state active generations count",
			nil, nil,
		),
		fdbRecoveryStateSecondsSinceLastRecovered: prometheus.NewDesc(
			"fdb_recovery_state_seconds_since_last_recovered",
			"Seconds since last recovery",
			nil, nil,
		),
		fdbProcessUptimeSeconds: prometheus.NewDesc(
			"fdb_process_uptime_seconds",
			"Process uptime",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessClass: prometheus.NewDesc(
			"fdb_process_class",
			"Indicates process class",
			[]string{"process_id", "address", "class_type"}, nil,
		),
		fdbProcessDegraded: prometheus.NewDesc(
			"fdb_process_degraded",
			"Whether or not process is degraded. 1 if is, 0 otherwise",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessRunLoopBusyRatio: prometheus.NewDesc(
			"fdb_process_run_loop_busy_ratio",
			"Fraction of time the run loop was busy",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCPUUsageCores: prometheus.NewDesc(
			"fdb_process_cpu_usage_cores",
			"Amount of CPU cores used by process",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessMemoryAvailableBytes: prometheus.NewDesc(
			"fdb_process_memory_available_bytes",
			"Memory in bytes available to process",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessMemoryUsedBytes: prometheus.NewDesc(
			"fdb_process_memory_used_bytes",
			"Memory in bytes used by process",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessMemoryLimitBytes: prometheus.NewDesc(
			"fdb_process_memory_limit_bytes",
			"Memory limit in bytes for process",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessMemoryUnusedAllocatedMemory: prometheus.NewDesc(
			"fdb_process_memory_unused_allocated_memory",
			"Unused memory in bytes allocated by process",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessMemoryRssBytes: prometheus.NewDesc(
			"fdb_process_memory_rss_bytes",
			"Resident set size (RSS) memory in bytes for process",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessDiskUsageRatio: prometheus.NewDesc(
			"fdb_process_disk_usage_ratio",
			"Disk usage from 0.0 (idle) to 1.0 (fully busy)",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessDiskFreeBytes: prometheus.NewDesc(
			"fdb_process_disk_free_bytes",
			"Amount of free disk space in bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessDiskTotalBytes: prometheus.NewDesc(
			"fdb_process_disk_total_bytes",
			"Total amount of disk space in bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessDiskReadsTotal: prometheus.NewDesc(
			"fdb_process_disk_reads_total",
			"Count of disk read operations",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessDiskWritesTotal: prometheus.NewDesc(
			"fdb_process_disk_writes_total",
			"Count of disk write operations",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessNetworkCurrentConnectionsTotal: prometheus.NewDesc(
			"fdb_process_network_current_connections_total",
			"Number of current connections to process",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessNetworkConnectionErrorsRate: prometheus.NewDesc(
			"fdb_process_network_connection_errors_rate",
			"Connection errors per second",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessNetworkConnectionsClosedRate: prometheus.NewDesc(
			"fdb_process_network_connections_closed_rate",
			"Connections closed per second",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessNetworkConnectionsEstablishedRate: prometheus.NewDesc(
			"fdb_process_network_connections_established_rate",
			"Connections established per second",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessNetworkMegabitsReceivedRate: prometheus.NewDesc(
			"fdb_process_network_megabits_received_rate",
			"Received data rate in megabits per second",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessNetworkMegabitsSentRate: prometheus.NewDesc(
			"fdb_process_network_megabits_sent_rate",
			"Sent data rate in megabits per second",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessRole: prometheus.NewDesc(
			"fdb_process_role",
			"Indicates process roles",
			[]string{"process_id", "address", "role"}, nil,
		),
		fdbProcessStorageDataLagSeconds: prometheus.NewDesc(
			"fdb_process_storage_data_lag_seconds",
			"Storage process data lag",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageDurabilityLagSeconds: prometheus.NewDesc(
			"fdb_process_storage_durability_lag_seconds",
			"Storage process durability lag",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageInputBytesTotal: prometheus.NewDesc(
			"fdb_process_storage_input_bytes_total",
			"Storage process input bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageDurableBytesTotal: prometheus.NewDesc(
			"fdb_process_storage_durable_bytes_total",
			"Storage process durable bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageStoredBytes: prometheus.NewDesc(
			"fdb_process_storage_stored_bytes",
			"Storage process stored bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageKvstoreAvailableBytes: prometheus.NewDesc(
			"fdb_process_storage_kvstore_available_bytes",
			"Storage process KV store available bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageKvstoreFreeBytes: prometheus.NewDesc(
			"fdb_process_storage_kvstore_free_bytes",
			"Storage process KV store free bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageKvstoreTotalBytes: prometheus.NewDesc(
			"fdb_process_storage_kvstore_total_bytes",
			"Storage process KV store total bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageKvstoreUsedBytes: prometheus.NewDesc(
			"fdb_process_storage_kvstore_used_bytes",
			"Storage process KV store used bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageFetchedVersionsTotal: prometheus.NewDesc(
			"fdb_process_storage_fetched_versions_total",
			"Storage process fetched versions count",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageFetchesFromLogsTotal: prometheus.NewDesc(
			"fdb_process_storage_fetches_from_logs_total",
			"Storage process fetches from logs count",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageLowPriorityQueriesTotal: prometheus.NewDesc(
			"fdb_process_storage_low_priority_queries_total",
			"Storage process low priority queries count",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageReadLatencyMean: prometheus.NewDesc(
			"fdb_process_storage_read_latency_mean",
			"Storage process mean read latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageReadLatencyMedian: prometheus.NewDesc(
			"fdb_process_storage_read_latency_median",
			"Storage process median read latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageReadLatencyP95: prometheus.NewDesc(
			"fdb_process_storage_read_latency_p95",
			"Storage process p95 read latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageReadLatencyP99: prometheus.NewDesc(
			"fdb_process_storage_read_latency_p99",
			"Storage process p99 read latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessStorageReadLatencyMax: prometheus.NewDesc(
			"fdb_process_storage_read_latency_max",
			"Storage process max read latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessLogQueueDiskAvailableBytes: prometheus.NewDesc(
			"fdb_process_log_queue_disk_available_bytes",
			"Log process queue disk available bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessLogQueueDiskFreeBytes: prometheus.NewDesc(
			"fdb_process_log_queue_disk_free_bytes",
			"Log process queue disk free bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessLogQueueDiskTotalBytes: prometheus.NewDesc(
			"fdb_process_log_queue_disk_total_bytes",
			"Log process queue disk total bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessLogQueueDiskUsedBytes: prometheus.NewDesc(
			"fdb_process_log_queue_disk_used_bytes",
			"Log process queue disk used bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessLogInputBytesTotal: prometheus.NewDesc(
			"fdb_process_log_input_bytes_total",
			"Log process input bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessLogDurableBytesTotal: prometheus.NewDesc(
			"fdb_process_log_durable_bytes_total",
			"Log process durable bytes",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyBatchingWindowSizeMean: prometheus.NewDesc(
			"fdb_process_commit_proxy_batching_window_size_mean",
			"Commit proxy mean batching window size in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyBatchingWindowSizeMedian: prometheus.NewDesc(
			"fdb_process_commit_proxy_batching_window_size_median",
			"Commit proxy median batching window size in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyBatchingWindowSizeP95: prometheus.NewDesc(
			"fdb_process_commit_proxy_batching_window_size_p95",
			"Commit proxy p95 batching window size in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyBatchingWindowSizeP99: prometheus.NewDesc(
			"fdb_process_commit_proxy_batching_window_size_p99",
			"Commit proxy p99 batching window size in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyCommitLatencyMean: prometheus.NewDesc(
			"fdb_process_commit_proxy_commit_latency_mean",
			"Commit proxy mean commit latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyCommitLatencyMedian: prometheus.NewDesc(
			"fdb_process_commit_proxy_commit_latency_median",
			"Commit proxy median commit latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyCommitLatencyP95: prometheus.NewDesc(
			"fdb_process_commit_proxy_commit_latency_p95",
			"Commit proxy p95 commit latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyCommitLatencyP99: prometheus.NewDesc(
			"fdb_process_commit_proxy_commit_latency_p99",
			"Commit proxy p99 commit latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessCommitProxyCommitLatencyMax: prometheus.NewDesc(
			"fdb_process_commit_proxy_commit_latency_max",
			"Commit proxy max commit latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyBatchLatencyMean: prometheus.NewDesc(
			"fdb_process_grv_proxy_batch_latency_mean",
			"GRV proxy mean batch priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyBatchLatencyMedian: prometheus.NewDesc(
			"fdb_process_grv_proxy_batch_latency_median",
			"GRV proxy median batch priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyBatchLatencyP95: prometheus.NewDesc(
			"fdb_process_grv_proxy_batch_latency_p95",
			"GRV proxy p95 batch priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyBatchLatencyP99: prometheus.NewDesc(
			"fdb_process_grv_proxy_batch_latency_p99",
			"GRV proxy p99 batch priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyDefaultLatencyMean: prometheus.NewDesc(
			"fdb_process_grv_proxy_default_latency_mean",
			"GRV proxy mean default priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyDefaultLatencyMedian: prometheus.NewDesc(
			"fdb_process_grv_proxy_default_latency_median",
			"GRV proxy median default priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyDefaultLatencyP95: prometheus.NewDesc(
			"fdb_process_grv_proxy_default_latency_p95",
			"GRV proxy p95 default priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyDefaultLatencyP99: prometheus.NewDesc(
			"fdb_process_grv_proxy_default_latency_p99",
			"GRV proxy p99 default priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
		fdbProcessGrvProxyDefaultLatencyMax: prometheus.NewDesc(
			"fdb_process_grv_proxy_default_latency_max",
			"GRV proxy max default priority latency in seconds",
			[]string{"process_id", "address"}, nil,
		),
	}
}

// Describe sends the super-set of all possible descriptors of metrics
// that can be produced by the Exporter to the provided channel.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.fdbDatabaseAvailable
	ch <- e.fdbDatabaseLocked
	ch <- e.fdbClientsTotal
	ch <- e.fdbDatacenterLagSeconds
	ch <- e.fdbLatencyProbeReadSeconds
	ch <- e.fdbLatencyProbeCommitSeconds
	ch <- e.fdbLatencyProbeTransactionStartSeconds
	ch <- e.fdbLatencyProbeImmediatePriorityTransactionStartSeconds
	ch <- e.fdbLatencyProbeBatchPriorityTransactionStartSeconds
	ch <- e.fdbWorkloadOperationsReadRequestsTotal
	ch <- e.fdbWorkloadOperationsReadsTotal
	ch <- e.fdbWorkloadOperationsWritesTotal
	ch <- e.fdbWorkloadOperationsLocationRequestsTotal
	ch <- e.fdbWorkloadOperationsLowPriorityReadsTotal
	ch <- e.fdbWorkloadOperationsMemoryErrorsTotal
	ch <- e.fdbWorkloadTransactionsStartedTotal
	ch <- e.fdbWorkloadTransactionsCommittedTotal
	ch <- e.fdbWorkloadTransactionsConflictedTotal
	ch <- e.fdbWorkloadTransactionsRejectedForQueuedTooLongTotal
	ch <- e.fdbWorkloadKeysReadTotal
	ch <- e.fdbWorkloadBytesReadTotal
	ch <- e.fdbWorkloadBytesWrittenTotal
	ch <- e.fdbDataState
	ch <- e.fdbDataLeastOperatingSpaceLogServerBytes
	ch <- e.fdbDataLeastOperatingSpaceStorageServerBytes
	ch <- e.fdbDataAveragePartitionSizeBytes
	ch <- e.fdbDataPartitionsTotal
	ch <- e.fdbDataTotalDiskUsedBytes
	ch <- e.fdbDataTotalKVSizeBytes
	ch <- e.fdbDataSystemKVSizeBytes
	ch <- e.fdbDataMovingInFlightBytes
	ch <- e.fdbDataMovingInQueueBytes
	ch <- e.fdbDataMovingTotalWrittenBytes
	ch <- e.fdbQosLimitingStorageServerDataLagSeconds
	ch <- e.fdbQosLimitingDurabilityLagStorageServerSeconds
	ch <- e.fdbQosLimitingStorageServerQueueBytes
	ch <- e.fdbQosLimitingVersionLagStorageServer
	ch <- e.fdbQosWorstStorageServerDataLagSeconds
	ch <- e.fdbQosWorstDurabilityLagStorageServerSeconds
	ch <- e.fdbQosWorstStorageServerQueueBytes
	ch <- e.fdbQosWorstLogServerQueueBytes
	ch <- e.fdbQosWorstVersionLagStorageServer
	ch <- e.fdbQosReleasedTransactionsPerSecond
	ch <- e.fdbQosTransactionsPerSecondLimit
	ch <- e.fdbQosPerformanceLimitedBy
	ch <- e.fdbQosBatchReleasedTransactionsPerSecond
	ch <- e.fdbQosBatchTransactionsPerSecondLimit
	ch <- e.fdbQosBatchPerformanceLimitedBy
	ch <- e.fdbQosThrottledTagsAutoCount
	ch <- e.fdbQosThrottledTagsAutoBusyRead
	ch <- e.fdbQosThrottledTagsAutoBusyWrite
	ch <- e.fdbQosThrottledTagsManualCount
	ch <- e.fdbRecoveryState
	ch <- e.fdbRecoveryStateActiveGenerations
	ch <- e.fdbRecoveryStateSecondsSinceLastRecovered
	ch <- e.fdbProcessUptimeSeconds
	ch <- e.fdbProcessClass
	ch <- e.fdbProcessDegraded
	ch <- e.fdbProcessRunLoopBusyRatio
	ch <- e.fdbProcessCPUUsageCores
	ch <- e.fdbProcessMemoryAvailableBytes
	ch <- e.fdbProcessMemoryUsedBytes
	ch <- e.fdbProcessMemoryLimitBytes
	ch <- e.fdbProcessMemoryUnusedAllocatedMemory
	ch <- e.fdbProcessMemoryRssBytes
	ch <- e.fdbProcessDiskUsageRatio
	ch <- e.fdbProcessDiskFreeBytes
	ch <- e.fdbProcessDiskTotalBytes
	ch <- e.fdbProcessDiskReadsTotal
	ch <- e.fdbProcessDiskWritesTotal
	ch <- e.fdbProcessNetworkCurrentConnectionsTotal
	ch <- e.fdbProcessNetworkConnectionErrorsRate
	ch <- e.fdbProcessNetworkConnectionsClosedRate
	ch <- e.fdbProcessNetworkConnectionsEstablishedRate
	ch <- e.fdbProcessNetworkMegabitsReceivedRate
	ch <- e.fdbProcessNetworkMegabitsSentRate
	ch <- e.fdbProcessRole
	ch <- e.fdbProcessStorageDataLagSeconds
	ch <- e.fdbProcessStorageDurabilityLagSeconds
	ch <- e.fdbProcessStorageInputBytesTotal
	ch <- e.fdbProcessStorageDurableBytesTotal
	ch <- e.fdbProcessStorageStoredBytes
	ch <- e.fdbProcessStorageKvstoreAvailableBytes
	ch <- e.fdbProcessStorageKvstoreFreeBytes
	ch <- e.fdbProcessStorageKvstoreTotalBytes
	ch <- e.fdbProcessStorageKvstoreUsedBytes
	ch <- e.fdbProcessStorageFetchedVersionsTotal
	ch <- e.fdbProcessStorageFetchesFromLogsTotal
	ch <- e.fdbProcessStorageLowPriorityQueriesTotal
	ch <- e.fdbProcessStorageReadLatencyMean
	ch <- e.fdbProcessStorageReadLatencyMedian
	ch <- e.fdbProcessStorageReadLatencyP95
	ch <- e.fdbProcessStorageReadLatencyP99
	ch <- e.fdbProcessStorageReadLatencyMax
	ch <- e.fdbProcessLogQueueDiskAvailableBytes
	ch <- e.fdbProcessLogQueueDiskFreeBytes
	ch <- e.fdbProcessLogQueueDiskTotalBytes
	ch <- e.fdbProcessLogQueueDiskUsedBytes
	ch <- e.fdbProcessLogInputBytesTotal
	ch <- e.fdbProcessLogDurableBytesTotal
	ch <- e.fdbProcessCommitProxyBatchingWindowSizeMean
	ch <- e.fdbProcessCommitProxyBatchingWindowSizeMedian
	ch <- e.fdbProcessCommitProxyBatchingWindowSizeP95
	ch <- e.fdbProcessCommitProxyBatchingWindowSizeP99
	ch <- e.fdbProcessCommitProxyCommitLatencyMean
	ch <- e.fdbProcessCommitProxyCommitLatencyMedian
	ch <- e.fdbProcessCommitProxyCommitLatencyP95
	ch <- e.fdbProcessCommitProxyCommitLatencyP99
	ch <- e.fdbProcessCommitProxyCommitLatencyMax
	ch <- e.fdbProcessGrvProxyBatchLatencyMean
	ch <- e.fdbProcessGrvProxyBatchLatencyMedian
	ch <- e.fdbProcessGrvProxyBatchLatencyP95
	ch <- e.fdbProcessGrvProxyBatchLatencyP99
	ch <- e.fdbProcessGrvProxyDefaultLatencyMean
	ch <- e.fdbProcessGrvProxyDefaultLatencyMedian
	ch <- e.fdbProcessGrvProxyDefaultLatencyP95
	ch <- e.fdbProcessGrvProxyDefaultLatencyP99
	ch <- e.fdbProcessGrvProxyDefaultLatencyMax
}

// Collect is called by the Prometheus registry when collecting metrics.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mu.Lock()
	defer e.mu.Unlock()

	status, err := e.getStatus()
	if err != nil {
		logrus.WithError(err).Error("Failed to get FDB status")
		return
	}

	if status.Cluster != nil {
		ch <- prometheus.MustNewConstMetric(
			e.fdbDatabaseAvailable,
			prometheus.GaugeValue,
			boolToFloat64(status.Cluster.DatabaseAvailable),
		)

		if status.Cluster.DatabaseLockState != nil {
			ch <- prometheus.MustNewConstMetric(
				e.fdbDatabaseLocked,
				prometheus.GaugeValue,
				boolToFloat64(status.Cluster.DatabaseLockState.Locked),
			)
		}

		if status.Cluster.Clients != nil {
			ch <- prometheus.MustNewConstMetric(
				e.fdbClientsTotal,
				prometheus.GaugeValue,
				float64(status.Cluster.Clients.Count),
			)
		}

		if status.Cluster.DatacenterLag != nil {
			ch <- prometheus.MustNewConstMetric(
				e.fdbDatacenterLagSeconds,
				prometheus.GaugeValue,
				status.Cluster.DatacenterLag.Seconds,
			)
		}

		if status.Cluster.LatencyProbe != nil {
			ch <- prometheus.MustNewConstMetric(e.fdbLatencyProbeReadSeconds, prometheus.GaugeValue, status.Cluster.LatencyProbe.ReadSeconds)
			ch <- prometheus.MustNewConstMetric(e.fdbLatencyProbeCommitSeconds, prometheus.GaugeValue, status.Cluster.LatencyProbe.CommitSeconds)
			ch <- prometheus.MustNewConstMetric(e.fdbLatencyProbeTransactionStartSeconds, prometheus.GaugeValue, status.Cluster.LatencyProbe.TransactionStartSeconds)
			ch <- prometheus.MustNewConstMetric(e.fdbLatencyProbeImmediatePriorityTransactionStartSeconds, prometheus.GaugeValue, status.Cluster.LatencyProbe.ImmediatePriorityTransactionStartSeconds)
			ch <- prometheus.MustNewConstMetric(e.fdbLatencyProbeBatchPriorityTransactionStartSeconds, prometheus.GaugeValue, status.Cluster.LatencyProbe.BatchPriorityTransactionStartSeconds)
		}

		if status.Cluster.Workload != nil {
			if status.Cluster.Workload.Operations != nil {
				if status.Cluster.Workload.Operations.ReadRequests != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadOperationsReadRequestsTotal, prometheus.CounterValue, status.Cluster.Workload.Operations.ReadRequests.Counter)
				}
				if status.Cluster.Workload.Operations.Reads != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadOperationsReadsTotal, prometheus.CounterValue, status.Cluster.Workload.Operations.Reads.Counter)
				}
				if status.Cluster.Workload.Operations.Writes != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadOperationsWritesTotal, prometheus.CounterValue, status.Cluster.Workload.Operations.Writes.Counter)
				}
				if status.Cluster.Workload.Operations.LocationRequests != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadOperationsLocationRequestsTotal, prometheus.CounterValue, status.Cluster.Workload.Operations.LocationRequests.Counter)
				}
				if status.Cluster.Workload.Operations.LowPriorityReads != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadOperationsLowPriorityReadsTotal, prometheus.CounterValue, status.Cluster.Workload.Operations.LowPriorityReads.Counter)
				}
				if status.Cluster.Workload.Operations.MemoryErrors != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadOperationsMemoryErrorsTotal, prometheus.CounterValue, status.Cluster.Workload.Operations.MemoryErrors.Counter)
				}
			}

			if status.Cluster.Workload.Transactions != nil {
				if status.Cluster.Workload.Transactions.Started != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadTransactionsStartedTotal, prometheus.CounterValue, status.Cluster.Workload.Transactions.Started.Counter)
				}
				if status.Cluster.Workload.Transactions.Committed != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadTransactionsCommittedTotal, prometheus.CounterValue, status.Cluster.Workload.Transactions.Committed.Counter)
				}
				if status.Cluster.Workload.Transactions.Conflicted != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadTransactionsConflictedTotal, prometheus.CounterValue, status.Cluster.Workload.Transactions.Conflicted.Counter)
				}
				if status.Cluster.Workload.Transactions.RejectedForQueuedTooLong != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadTransactionsRejectedForQueuedTooLongTotal, prometheus.CounterValue, status.Cluster.Workload.Transactions.RejectedForQueuedTooLong.Counter)
				}
			}

			if status.Cluster.Workload.Keys != nil {
				if status.Cluster.Workload.Keys.Read != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadKeysReadTotal, prometheus.CounterValue, status.Cluster.Workload.Keys.Read.Counter)
				}
			}

			if status.Cluster.Workload.Bytes != nil {
				if status.Cluster.Workload.Bytes.Read != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadBytesReadTotal, prometheus.CounterValue, status.Cluster.Workload.Bytes.Read.Counter)
				}
				if status.Cluster.Workload.Bytes.Written != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbWorkloadBytesWrittenTotal, prometheus.CounterValue, status.Cluster.Workload.Bytes.Written.Counter)
				}
			}
		}

		if status.Cluster.Data != nil {
			if status.Cluster.Data.State != nil {
				ch <- prometheus.MustNewConstMetric(e.fdbDataState, prometheus.GaugeValue, 1, status.Cluster.Data.State.Name)
			}
			ch <- prometheus.MustNewConstMetric(e.fdbDataLeastOperatingSpaceLogServerBytes, prometheus.GaugeValue, float64(status.Cluster.Data.LeastOperatingSpaceBytesLogServer))
			ch <- prometheus.MustNewConstMetric(e.fdbDataLeastOperatingSpaceStorageServerBytes, prometheus.GaugeValue, float64(status.Cluster.Data.LeastOperatingSpaceBytesStorageServer))
			ch <- prometheus.MustNewConstMetric(e.fdbDataAveragePartitionSizeBytes, prometheus.GaugeValue, float64(status.Cluster.Data.AveragePartitionSizeBytes))
			ch <- prometheus.MustNewConstMetric(e.fdbDataPartitionsTotal, prometheus.GaugeValue, float64(status.Cluster.Data.PartitionsCount))
			ch <- prometheus.MustNewConstMetric(e.fdbDataTotalDiskUsedBytes, prometheus.GaugeValue, float64(status.Cluster.Data.TotalDiskUsedBytes))
			ch <- prometheus.MustNewConstMetric(e.fdbDataTotalKVSizeBytes, prometheus.GaugeValue, float64(status.Cluster.Data.TotalKVSizeBytes))
			ch <- prometheus.MustNewConstMetric(e.fdbDataSystemKVSizeBytes, prometheus.GaugeValue, float64(status.Cluster.Data.SystemKVSizeBytes))

			if status.Cluster.Data.MovingData != nil {
				ch <- prometheus.MustNewConstMetric(e.fdbDataMovingInFlightBytes, prometheus.GaugeValue, float64(status.Cluster.Data.MovingData.InFlightBytes))
				ch <- prometheus.MustNewConstMetric(e.fdbDataMovingInQueueBytes, prometheus.GaugeValue, float64(status.Cluster.Data.MovingData.InQueueBytes))
				ch <- prometheus.MustNewConstMetric(e.fdbDataMovingTotalWrittenBytes, prometheus.CounterValue, float64(status.Cluster.Data.MovingData.TotalWrittenBytes))
			}
		}

		if status.Cluster.QOS != nil {
			if status.Cluster.QOS.LimitingDataLagStorageServer != nil {
				ch <- prometheus.MustNewConstMetric(e.fdbQosLimitingStorageServerDataLagSeconds, prometheus.GaugeValue, status.Cluster.QOS.LimitingDataLagStorageServer.Seconds)
			}
			if status.Cluster.QOS.LimitingDurabilityLagStorageServer != nil {
				ch <- prometheus.MustNewConstMetric(e.fdbQosLimitingDurabilityLagStorageServerSeconds, prometheus.GaugeValue, status.Cluster.QOS.LimitingDurabilityLagStorageServer.Seconds)
			}
			ch <- prometheus.MustNewConstMetric(e.fdbQosLimitingStorageServerQueueBytes, prometheus.GaugeValue, float64(status.Cluster.QOS.LimitingStorageServerQueueBytes))
			ch <- prometheus.MustNewConstMetric(e.fdbQosLimitingVersionLagStorageServer, prometheus.GaugeValue, float64(status.Cluster.QOS.LimitingVersionLagStorageServer))
			if status.Cluster.QOS.WorstDataLagStorageServer != nil {
				ch <- prometheus.MustNewConstMetric(e.fdbQosWorstStorageServerDataLagSeconds, prometheus.GaugeValue, status.Cluster.QOS.WorstDataLagStorageServer.Seconds)
			}
			if status.Cluster.QOS.WorstDurabilityLagStorageServer != nil {
				ch <- prometheus.MustNewConstMetric(e.fdbQosWorstDurabilityLagStorageServerSeconds, prometheus.GaugeValue, status.Cluster.QOS.WorstDurabilityLagStorageServer.Seconds)
			}
			ch <- prometheus.MustNewConstMetric(e.fdbQosWorstStorageServerQueueBytes, prometheus.GaugeValue, float64(status.Cluster.QOS.WorstStorageServerQueueBytes))
			ch <- prometheus.MustNewConstMetric(e.fdbQosWorstLogServerQueueBytes, prometheus.GaugeValue, float64(status.Cluster.QOS.WorstLogServerQueueBytes))
			ch <- prometheus.MustNewConstMetric(e.fdbQosWorstVersionLagStorageServer, prometheus.GaugeValue, float64(status.Cluster.QOS.WorstVersionLagStorageServer))
			ch <- prometheus.MustNewConstMetric(e.fdbQosReleasedTransactionsPerSecond, prometheus.GaugeValue, status.Cluster.QOS.ReleasedTransactionsPerSecond)
			ch <- prometheus.MustNewConstMetric(e.fdbQosTransactionsPerSecondLimit, prometheus.GaugeValue, status.Cluster.QOS.TransactionsPerSecondLimit)
			if status.Cluster.QOS.PerformanceLimitedBy != nil {
				ch <- prometheus.MustNewConstMetric(e.fdbQosPerformanceLimitedBy, prometheus.GaugeValue, 1, status.Cluster.QOS.PerformanceLimitedBy.Name, status.Cluster.QOS.PerformanceLimitedBy.Description)
			}
			ch <- prometheus.MustNewConstMetric(e.fdbQosBatchReleasedTransactionsPerSecond, prometheus.GaugeValue, status.Cluster.QOS.BatchReleasedTransactionsPerSecond)
			ch <- prometheus.MustNewConstMetric(e.fdbQosBatchTransactionsPerSecondLimit, prometheus.GaugeValue, status.Cluster.QOS.BatchTransactionsPerSecondLimit)
			if status.Cluster.QOS.BatchPerformanceLimitedBy != nil {
				ch <- prometheus.MustNewConstMetric(e.fdbQosBatchPerformanceLimitedBy, prometheus.GaugeValue, 1, status.Cluster.QOS.BatchPerformanceLimitedBy.Name, status.Cluster.QOS.BatchPerformanceLimitedBy.Description)
			}
			if status.Cluster.QOS.ThrottledTags != nil {
				if status.Cluster.QOS.ThrottledTags.Auto != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbQosThrottledTagsAutoCount, prometheus.GaugeValue, float64(status.Cluster.QOS.ThrottledTags.Auto.Count))
					ch <- prometheus.MustNewConstMetric(e.fdbQosThrottledTagsAutoBusyRead, prometheus.GaugeValue, float64(status.Cluster.QOS.ThrottledTags.Auto.BusyRead))
					ch <- prometheus.MustNewConstMetric(e.fdbQosThrottledTagsAutoBusyWrite, prometheus.GaugeValue, float64(status.Cluster.QOS.ThrottledTags.Auto.BusyWrite))
				}
				if status.Cluster.QOS.ThrottledTags.Manual != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbQosThrottledTagsManualCount, prometheus.GaugeValue, float64(status.Cluster.QOS.ThrottledTags.Manual.Count))
				}
			}
		}

		if status.Cluster.RecoveryState != nil {
			ch <- prometheus.MustNewConstMetric(e.fdbRecoveryState, prometheus.GaugeValue, 1, status.Cluster.RecoveryState.Name, status.Cluster.RecoveryState.Description)
			ch <- prometheus.MustNewConstMetric(e.fdbRecoveryStateActiveGenerations, prometheus.GaugeValue, float64(status.Cluster.RecoveryState.ActiveGenerations))
			ch <- prometheus.MustNewConstMetric(e.fdbRecoveryStateSecondsSinceLastRecovered, prometheus.GaugeValue, status.Cluster.RecoveryState.SecondsSinceLastRecovered)
		}

		if status.Cluster.Processes != nil {
			for processID, process := range status.Cluster.Processes {
				ch <- prometheus.MustNewConstMetric(e.fdbProcessUptimeSeconds, prometheus.GaugeValue, process.UptimeSeconds, processID, process.Address)
				ch <- prometheus.MustNewConstMetric(e.fdbProcessClass, prometheus.GaugeValue, 1, processID, process.Address, process.ClassType)
				ch <- prometheus.MustNewConstMetric(e.fdbProcessDegraded, prometheus.GaugeValue, boolToFloat64(process.Degraded), processID, process.Address)
				ch <- prometheus.MustNewConstMetric(e.fdbProcessRunLoopBusyRatio, prometheus.GaugeValue, process.RunLoopBusy, processID, process.Address)

				if process.CPU != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbProcessCPUUsageCores, prometheus.GaugeValue, process.CPU.UsageCores, processID, process.Address)
				}

				if process.Memory != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbProcessMemoryAvailableBytes, prometheus.GaugeValue, float64(process.Memory.AvailableBytes), processID, process.Address)
					ch <- prometheus.MustNewConstMetric(e.fdbProcessMemoryUsedBytes, prometheus.GaugeValue, float64(process.Memory.UsedBytes), processID, process.Address)
					ch <- prometheus.MustNewConstMetric(e.fdbProcessMemoryLimitBytes, prometheus.GaugeValue, float64(process.Memory.LimitBytes), processID, process.Address)
					ch <- prometheus.MustNewConstMetric(e.fdbProcessMemoryUnusedAllocatedMemory, prometheus.GaugeValue, float64(process.Memory.UnusedAllocatedMemory), processID, process.Address)
					ch <- prometheus.MustNewConstMetric(e.fdbProcessMemoryRssBytes, prometheus.GaugeValue, float64(process.Memory.RssBytes), processID, process.Address)
				}

				if process.Disk != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbProcessDiskUsageRatio, prometheus.GaugeValue, process.Disk.Busy, processID, process.Address)
					ch <- prometheus.MustNewConstMetric(e.fdbProcessDiskFreeBytes, prometheus.GaugeValue, float64(process.Disk.FreeBytes), processID, process.Address)
					ch <- prometheus.MustNewConstMetric(e.fdbProcessDiskTotalBytes, prometheus.GaugeValue, float64(process.Disk.TotalBytes), processID, process.Address)
					if process.Disk.Reads != nil {
						ch <- prometheus.MustNewConstMetric(e.fdbProcessDiskReadsTotal, prometheus.CounterValue, process.Disk.Reads.Counter, processID, process.Address)
					}
					if process.Disk.Writes != nil {
						ch <- prometheus.MustNewConstMetric(e.fdbProcessDiskWritesTotal, prometheus.CounterValue, process.Disk.Writes.Counter, processID, process.Address)
					}
				}

				if process.Network != nil {
					ch <- prometheus.MustNewConstMetric(e.fdbProcessNetworkCurrentConnectionsTotal, prometheus.GaugeValue, float64(process.Network.CurrentConnections), processID, process.Address)
					if process.Network.ConnectionErrors != nil {
						ch <- prometheus.MustNewConstMetric(e.fdbProcessNetworkConnectionErrorsRate, prometheus.GaugeValue, process.Network.ConnectionErrors.Hz, processID, process.Address)
					}
					if process.Network.ConnectionsClosed != nil {
						ch <- prometheus.MustNewConstMetric(e.fdbProcessNetworkConnectionsClosedRate, prometheus.GaugeValue, process.Network.ConnectionsClosed.Hz, processID, process.Address)
					}
					if process.Network.ConnectionsEstablished != nil {
						ch <- prometheus.MustNewConstMetric(e.fdbProcessNetworkConnectionsEstablishedRate, prometheus.GaugeValue, process.Network.ConnectionsEstablished.Hz, processID, process.Address)
					}
					if process.Network.MegabitsReceived != nil {
						ch <- prometheus.MustNewConstMetric(e.fdbProcessNetworkMegabitsReceivedRate, prometheus.GaugeValue, process.Network.MegabitsReceived.Hz, processID, process.Address)
					}
					if process.Network.MegabitsSent != nil {
						ch <- prometheus.MustNewConstMetric(e.fdbProcessNetworkMegabitsSentRate, prometheus.GaugeValue, process.Network.MegabitsSent.Hz, processID, process.Address)
					}
				}

				for _, role := range process.Roles {
					ch <- prometheus.MustNewConstMetric(e.fdbProcessRole, prometheus.GaugeValue, 1, processID, process.Address, role.Role)

					switch role.Role {
					case "storage":
						if role.DataLag != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageDataLagSeconds, prometheus.GaugeValue, role.DataLag.Seconds, processID, process.Address)
						}
						if role.DurabilityLag != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageDurabilityLagSeconds, prometheus.GaugeValue, role.DurabilityLag.Seconds, processID, process.Address)
						}
						if role.InputBytes != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageInputBytesTotal, prometheus.CounterValue, role.InputBytes.Counter, processID, process.Address)
						}
						if role.DurableBytes != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageDurableBytesTotal, prometheus.CounterValue, role.DurableBytes.Counter, processID, process.Address)
						}
						ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageStoredBytes, prometheus.GaugeValue, float64(role.StoredBytes), processID, process.Address)
						ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageKvstoreAvailableBytes, prometheus.GaugeValue, float64(role.KvstoreAvailableBytes), processID, process.Address)
						ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageKvstoreFreeBytes, prometheus.GaugeValue, float64(role.KvstoreFreeBytes), processID, process.Address)
						ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageKvstoreTotalBytes, prometheus.GaugeValue, float64(role.KvstoreTotalBytes), processID, process.Address)
						ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageKvstoreUsedBytes, prometheus.GaugeValue, float64(role.KvstoreUsedBytes), processID, process.Address)
						if role.FetchedVersions != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageFetchedVersionsTotal, prometheus.CounterValue, role.FetchedVersions.Counter, processID, process.Address)
						}
						if role.FetchesFromLogs != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageFetchesFromLogsTotal, prometheus.CounterValue, role.FetchesFromLogs.Counter, processID, process.Address)
						}
						if role.LowPriorityQueries != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageLowPriorityQueriesTotal, prometheus.CounterValue, role.LowPriorityQueries.Counter, processID, process.Address)
						}
						if role.ReadLatencyStatistics != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageReadLatencyMean, prometheus.GaugeValue, role.ReadLatencyStatistics.Mean, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageReadLatencyMedian, prometheus.GaugeValue, role.ReadLatencyStatistics.Median, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageReadLatencyP95, prometheus.GaugeValue, role.ReadLatencyStatistics.P95, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageReadLatencyP99, prometheus.GaugeValue, role.ReadLatencyStatistics.P99, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessStorageReadLatencyMax, prometheus.GaugeValue, role.ReadLatencyStatistics.Max, processID, process.Address)
						}
					case "log":
						ch <- prometheus.MustNewConstMetric(e.fdbProcessLogQueueDiskAvailableBytes, prometheus.GaugeValue, float64(role.QueueDiskAvailableBytes), processID, process.Address)
						ch <- prometheus.MustNewConstMetric(e.fdbProcessLogQueueDiskFreeBytes, prometheus.GaugeValue, float64(role.QueueDiskFreeBytes), processID, process.Address)
						ch <- prometheus.MustNewConstMetric(e.fdbProcessLogQueueDiskTotalBytes, prometheus.GaugeValue, float64(role.QueueDiskTotalBytes), processID, process.Address)
						ch <- prometheus.MustNewConstMetric(e.fdbProcessLogQueueDiskUsedBytes, prometheus.GaugeValue, float64(role.QueueDiskUsedBytes), processID, process.Address)
						if role.InputBytes != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessLogInputBytesTotal, prometheus.CounterValue, role.InputBytes.Counter, processID, process.Address)
						}
						if role.DurableBytes != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessLogDurableBytesTotal, prometheus.CounterValue, role.DurableBytes.Counter, processID, process.Address)
						}
					case "commit_proxy":
						if role.CommitBatchingWindowSize != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyBatchingWindowSizeMean, prometheus.GaugeValue, role.CommitBatchingWindowSize.Mean, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyBatchingWindowSizeMedian, prometheus.GaugeValue, role.CommitBatchingWindowSize.Median, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyBatchingWindowSizeP95, prometheus.GaugeValue, role.CommitBatchingWindowSize.P95, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyBatchingWindowSizeP99, prometheus.GaugeValue, role.CommitBatchingWindowSize.P99, processID, process.Address)
						}
						if role.CommitLatencyStatistics != nil {
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyCommitLatencyMean, prometheus.GaugeValue, role.CommitLatencyStatistics.Mean, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyCommitLatencyMedian, prometheus.GaugeValue, role.CommitLatencyStatistics.Median, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyCommitLatencyP95, prometheus.GaugeValue, role.CommitLatencyStatistics.P95, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyCommitLatencyP99, prometheus.GaugeValue, role.CommitLatencyStatistics.P99, processID, process.Address)
							ch <- prometheus.MustNewConstMetric(e.fdbProcessCommitProxyCommitLatencyMax, prometheus.GaugeValue, role.CommitLatencyStatistics.Max, processID, process.Address)
						}
					case "grv_proxy":
						if role.GrvLatencyStatistics != nil {
							if role.GrvLatencyStatistics.Batch != nil {
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyBatchLatencyMean, prometheus.GaugeValue, role.GrvLatencyStatistics.Batch.Mean, processID, process.Address)
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyBatchLatencyMedian, prometheus.GaugeValue, role.GrvLatencyStatistics.Batch.Median, processID, process.Address)
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyBatchLatencyP95, prometheus.GaugeValue, role.GrvLatencyStatistics.Batch.P95, processID, process.Address)
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyBatchLatencyP99, prometheus.GaugeValue, role.GrvLatencyStatistics.Batch.P99, processID, process.Address)
							}
							if role.GrvLatencyStatistics.Default != nil {
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyDefaultLatencyMean, prometheus.GaugeValue, role.GrvLatencyStatistics.Default.Mean, processID, process.Address)
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyDefaultLatencyMedian, prometheus.GaugeValue, role.GrvLatencyStatistics.Default.Median, processID, process.Address)
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyDefaultLatencyP95, prometheus.GaugeValue, role.GrvLatencyStatistics.Default.P95, processID, process.Address)
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyDefaultLatencyP99, prometheus.GaugeValue, role.GrvLatencyStatistics.Default.P99, processID, process.Address)
								ch <- prometheus.MustNewConstMetric(e.fdbProcessGrvProxyDefaultLatencyMax, prometheus.GaugeValue, role.GrvLatencyStatistics.Default.Max, processID, process.Address)
							}
						}
					}
				}
			}
		}
	}
}

func (e *Exporter) getStatus() (*FDBStatus, error) {
	statusJSON, err := e.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.Get(fdb.Key([]byte{0xff, 0xff, '/', 's', 't', 'a', 't', 'u', 's', '/', 'j', 's', 'o', 'n'})).Get()
	})
	if err != nil {
		return nil, err
	}

	fmt.Println(string(statusJSON.([]byte)))

	var status FDBStatus
	if err := json.Unmarshal(statusJSON.([]byte), &status); err != nil {
		return nil, err
	}

	return &status, nil
}

func boolToFloat64(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
