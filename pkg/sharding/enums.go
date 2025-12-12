package sharding

type ReplicationStrategy string

const (
	// ReplicateNone Disable shard data replication
	ReplicateNone ReplicationStrategy = "ReplicateNone"

	// ReplicateAll Replicates data to every shard replica synchronously
	// on every transaction, every replication operation must succeed for the transaction
	// to commit
	ReplicateAll ReplicationStrategy = "ReplicateAll"

	// ReplicateSome Replicates data to every shard replica synchronously
	// on every transaction, at least one replication operation must succeed
	// for the transaction to commit
	ReplicateSome ReplicationStrategy = "ReplicateSome" // TODO -> Unimplemented
)

// TODO -> Unused
type ReplicaMode string

const (
	// ReadWrite Enables the shard replica to be used on both Transact and Query calls
	ReadWrite ReplicaMode = "ReadWrite"

	// ReadOnly Enables the shard replica to be used only to read data on Query
	ReadOnly ReplicaMode = "ReadOnly"

	// WriteOnly Enables the shard replica to be used only to mutate data on Transact
	WriteOnly ReplicaMode = "WriteOnly"
)
