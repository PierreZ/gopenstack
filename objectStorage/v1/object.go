package objectStorageV1

import (
	"github.com/Toorop/gopenstack"
)

// {"hash": "eda9a9889837ac4bc81d6387d92c1bec", "last_modified": "2014-10-27T16:35:40.140480", "bytes": 204800000, "name": "453410c1-dab8-4884-8dc2-af57b33b4a29-00400", "content_type": "application/octet-stream"}

// object represents an openstack object
type object struct {
	Name         string                `json:"name"`          // The name of the object
	Hash         string                `json:"hash"`          // The MD5 checksum value of the object content
	Bytes        uint64                `json:"bytes"`         // The total number of bytes that are stored for this Object
	ContentType  string                `json:"content_type"`  // The content type of the object
	LastModified gopenstack.DateTimeOs `json:"last_modified"` // The date and time when the object was last modified
}
