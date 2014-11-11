package objectStorageV1

import (
	"github.com/Toorop/gopenstack"
)

// NewObjectStorageClient return an objectStorageClient
func NewClient(keyring *gopenstack.Keyring, region string) (*gopenstack.Client, error) {
	return gopenstack.NewClient(keyring, region, "object-store")
}
