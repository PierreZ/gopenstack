package gopenstack

import (
	"errors"
)

var (
	ErrEndpointNotFound = errors.New("No endpoint found for this region & type")
	// Object storage
	ErrContainerNotFound          = errors.New("Container not found")
	ErrCopyLocalToLocalNotAllowed = errors.New("Local copies ares not allowed")
	ErrNoContainerSpecified       = errors.New("You must specify a container")
)

func ErrPathNotFound(path string) error {
	return errors.New(path + ": No such file or directory ")
}

func ErrUnsuportedPathType(pathType string) error {
	return errors.New(pathType + ": Unsuported path type")
}
