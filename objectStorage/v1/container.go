package objectStorageV1

// container represents a object container
type container struct {
	Count   uint     `json:"count"` // The number of objects in the container.
	Name    string   `json:"name"`  // The name of the container.
	Bytes   uint     `json:"bytes"` // The total number of bytes that are stored in Object Storage for the account.
	Objects []object // Objects in container
}
