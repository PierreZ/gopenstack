package objectStorageV1

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Toorop/gopenstack"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// A swift is a high-level representation of the openstack object storage service
type swift struct {
	client *gopenstack.Client
}

// NewObjectStoragesPath return an osPath
func NewSwift(client *gopenstack.Client) *swift {
	return &swift{client}
}

// CreateContainer create a container if it doesn't exists
func (s *swift) AddContainer(container string) (err error) {
	resp, err := s.client.Call(&gopenstack.CallOptions{
		Method:    "HEAD",
		Ressource: url.QueryEscape(container),
	})

	if err = resp.HandleErr(err, []int{200, 204, 404}); err != nil {
		return
	}
	// 404 not present
	if resp.StatusCode == 404 {
		_, err = s.client.Call(&gopenstack.CallOptions{
			Method:    "PUT",
			Ressource: url.QueryEscape(container),
		})
	}
	return
}

// ListContainers returns containers
func (s *swift) ListContainers() (containers []container, err error) {
	resp, err := s.client.Call(&gopenstack.CallOptions{
		Method:    "GET",
		Ressource: "?format=json",
	})
	if err = resp.HandleErr(err, []int{200, 203}); err != nil {
		return
	}
	err = json.Unmarshal(resp.Body, &containers)
	return
}

// DownloadObject download and save to dest, src object
func (s *swift) DownloadObject(src, dest string) error {
	// Create local folder if needed
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		return err
	}

	src = escapePath(src)
	resp, err := s.client.Call(&gopenstack.CallOptions{
		Method:             "GET",
		Ressource:          src,
		ReturnBodyAsReader: true,
	})
	if err = resp.HandleErr(err, []int{200}); err != nil {
		return err
	}

	i := resp.BodyReader
	defer i.Close()
	o, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer o.Close()
	_, err = io.Copy(o, i)
	return err
}

// GetAndStore recursively gets objects from srcPath and write them under destPath
func (s *swift) DownloadPath(srcPath, destPath string) error {

	// we must have a container specified
	if srcPath == "" || srcPath == "/" {
		return gopenstack.ErrNoContainerSpecified
	}

	// check if local path exist
	if _, err := os.Stat(destPath); err != nil {
		return gopenstack.ErrPathNotFound(destPath)
	}
	// Is RA path exists ?
	dPath := NewOsPath(s.client, srcPath)
	pathType, err := dPath.GetType()
	if err != nil {
		return err
	}

	hasTrailingSlash := false
	container := dPath.GetContainer()

	// clean paths
	if strings.HasSuffix(srcPath, "/") {
		srcPath = srcPath[0 : len(srcPath)-1]
		hasTrailingSlash = true
	}
	if strings.HasSuffix(destPath, "/") {
		destPath = destPath[:len(destPath)-1]
	}

	// is container
	isContainer := pathType == "container"
	prefix := ""
	if !isContainer {
		prefix = srcPath[len(container):]
	}
	pPrefix := strings.Split(prefix, "/")

	objectsToDownload, err := dPath.GetChildrenObjects()
	if err != nil {
		return err
	}

	// Upload files
	chanDone := make(chan bool)
	chanAJobIsDone := make(chan bool)
	chanThreadCount := make(chan int)
	threadsCount := 0
	remainingJobs := len(objectsToDownload)

	// Count concurrent threads
	go func() {
		for {
			threadsCount += <-chanThreadCount
		}
	}()

	// Update remainingJobs and send "all jobs are done" signal
	go func() {
		for {
			<-chanAJobIsDone
			remainingJobs--
			if remainingJobs < 1 {
				chanDone <- true
				break
			}
		}
	}()
	exitAsap := false
	for _, o := range objectsToDownload {

		for {
			if threadsCount < 5 {
				threadsCount++
				break
			}
			time.Sleep(1 * time.Second)
		}
		if exitAsap {
			// Wait for running process to finish their task
			for {
				if threadsCount == 1 {
					break
				}
				time.Sleep(1 * time.Second)
			}
			break
		}

		go func(o object) {
			src := container + "/" + o.Name
			dest := destPath + "/"
			if !hasTrailingSlash {
				if isContainer {
					dest += container + "/" + o.Name
				} else {
					dest += pPrefix[len(pPrefix)-1] + "/" + o.Name[len(prefix):]
				}
			} else {
				if isContainer {
					dest += o.Name
				} else {
					dest += o.Name[len(prefix):]
				}
			}

			//fmt.Println(o)
			//fmt.Println(o.Name, src+" -> "+dest)
			err = s.DownloadObject(src, dest)
			if err != nil {
				threadsCount--
				exitAsap = true
				chanDone <- true
				return
			}
			threadsCount--
			chanAJobIsDone <- true
		}(o)
	}
	// Waiting for all jobs to finish
	<-chanDone
	return nil
}

// Put upload a file to storage
// If the file exists (with the same etag) PutFile does not reupload it
func (s *swift) PutFile(src, dest string) (err error) {
	//fmt.Println(src + "->" + dest)

	// we must have a conatainer specified
	// -> at least 2 /
	if strings.Count(dest, "/") < 2 {
		err = gopenstack.ErrNoContainerSpecified
		return
	}

	// Create container if needed
	dPath := NewOsPath(s.client, dest)
	if err = s.AddContainer(dPath.GetContainer()); err != nil {
		return err
	}

	bodyReader, err := os.Open(src)
	if err != nil {
		return
	}
	defer bodyReader.Close()

	// Content-Length
	stats, err := bodyReader.Stat()
	if err != nil {
		return
	}
	contentLenght := strconv.FormatInt(stats.Size(), 10)

	// ETag (md5 sum)
	md5Reader, _ := os.Open(src)
	h := md5.New()
	io.Copy(h, md5Reader)
	etag := fmt.Sprintf("%x", h.Sum(nil))
	md5Reader.Close()

	// Do a Head request to see if the object already exists
	resp, err := s.client.Call(&gopenstack.CallOptions{
		Method:    "HEAD",
		Ressource: escapePath(dest),
	})

	err = resp.HandleErr(err, []int{200, 204, 404})

	if resp.StatusCode != 404 && resp.Headers["Etag"][0] == etag {
		return
	}

	// Headers
	headers := make(map[string]string)
	headers["Content-Length"] = contentLenght
	headers["Etag"] = etag

	resp, err = s.client.Call(&gopenstack.CallOptions{
		Method:    "PUT",
		Ressource: dest + "?format=json",
		Payload:   bodyReader,
		Headers:   headers,
	})
	err = resp.HandleErr(err, []int{200, 201})
	return
}

// Put recursively upload files under srcPath to destPath
func (s *swift) Put(srcPath, destPath string) error {
	srcPath, err := filepath.Abs(filepath.Clean(srcPath))
	if err != nil {
		return err
	}
	//fmt.Println(srcPath + " -> " + destPath)
	if strings.HasSuffix(destPath, "/") {
		destPath = destPath[:len(destPath)-1]
	}

	// we must have a container specified
	if destPath == "" || destPath == "/" {
		return gopenstack.ErrNoContainerSpecified
	}

	childrenPaths := []string{}
	if _, err = os.Stat(srcPath); err != nil {
		return gopenstack.ErrPathNotFound(srcPath)
	}

	err = filepath.Walk(srcPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			childrenPaths = append(childrenPaths, path)
		}
		return err
	})
	if err != nil {
		return err
	}

	// Upload files
	chanDone := make(chan bool)
	chanAJobIsDone := make(chan bool)
	chanThreadCount := make(chan int)
	threadsCount := 0
	remainingJobs := len(childrenPaths)

	// Count concurrent threads
	go func() {
		for {
			threadsCount += <-chanThreadCount
		}
	}()

	// Update remainingJobs and send "all jobs are done" signal
	go func() {
		for {
			<-chanAJobIsDone
			remainingJobs--
			if remainingJobs < 1 {
				chanDone <- true
				break
			}
		}
	}()

	// PUT
	ps := strings.Split(srcPath, "/")
	exitAsap := false
	for _, p := range childrenPaths {
		for {
			if threadsCount < 5 {
				threadsCount++
				break
			}
			time.Sleep(1 * time.Second)
		}
		if exitAsap {
			// Wait for running process to finish their task
			for {
				if threadsCount == 1 {
					break
				}
				time.Sleep(1 * time.Second)
			}
			break
		}
		go func(p string) {
			destination := destPath + "/"
			if !strings.HasSuffix(srcPath, "/") {
				destination += ps[len(ps)-1]
			}
			destination += p[len(srcPath):]
			err = s.PutFile(p, destination)
			if err != nil {
				threadsCount--
				exitAsap = true
				chanDone <- true
				return
			}
			threadsCount--
			chanAJobIsDone <- true
		}(p)
	}
	// Waiting for all jobs to finish
	<-chanDone
	return err
}

// Copy recursively copies srcPath to destPath
// You can copy
// local path to remote path
// remote path to local path
// remote path to remote path (not yet)
func (s *swift) Copy(srcPath, destPath string) error {
	srcIsLocal := true
	destIsLocal := true
	if _, err := os.Stat(srcPath); err != nil {
		srcIsLocal = false
	}
	if _, err := os.Stat(destPath); err != nil {
		destIsLocal = false
	}
	// Do copy
	if srcIsLocal && !destIsLocal {
		//fmt.Println("src is local, dest is remote")
		return s.Put(srcPath, destPath)
	} else if !srcIsLocal && destIsLocal {
		//fmt.Println("src is remote, dest is local")
		return s.DownloadPath(srcPath, destPath)
	} else if !srcIsLocal && !destIsLocal {
		return errors.New("Not implemented yet")
	} else {
		return errors.New("It seems that you do not set container on your remote path (or you are trying to make locals copies, if it's the case, use cp ;) )")
	}
	return nil
}

// DeleteObject delete object with path path
func (s *swift) DeleteObject(path string) error {
	resp, err := s.client.Call(&gopenstack.CallOptions{
		Method:    "DELETE",
		Ressource: escapePath(path),
	})
	return resp.HandleErr(err, []int{204})
}

// DeletePath delete path & his children (helper)
func (s *swift) DeletePath(path string) error {
	var err error
	hasTrailingSlash := false
	objectToremovePaths := []string{}
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
		hasTrailingSlash = true
	}

	// Container to remove (last job)
	containerToRemove := ""

	// get path type (container, object, vpath)
	dPath := NewOsPath(s.client, path)
	pathType, err := dPath.GetType()
	if err != nil {
		return err
	}

	switch pathType {
	case "object":
		objectToremovePaths = append(objectToremovePaths, path)
	case "container", "vfolder":
		objectsToRemove, err := dPath.GetChildrenObjects()
		if err != nil {
			return err
		}
		if pathType == "container" {
			for _, o := range objectsToRemove {
				objectToremovePaths = append(objectToremovePaths, path+"/"+o.Name)
			}
			if !hasTrailingSlash {
				containerToRemove = path
			}
		} else {
			if len(objectsToRemove) == 0 {
				return gopenstack.ErrPathNotFound(path)
			}
			container := dPath.GetContainer()
			for _, o := range objectsToRemove {
				objectToremovePaths = append(objectToremovePaths, container+"/"+o.Name)
			}
		}
	default:
		err = gopenstack.ErrUnsuportedPathType(pathType)
		return err
	}

	// Remove objects
	chanDone := make(chan bool)
	chanAJobIsDone := make(chan bool)
	chanThreadCount := make(chan int)
	threadsCount := 0
	remainingJobs := len(objectToremovePaths)

	// Count concurrent threads
	go func() {
		for {
			threadsCount += <-chanThreadCount
		}
	}()

	// Update remainingJobs and send "all jobs are done"  signal
	go func() {
		for {
			<-chanAJobIsDone
			remainingJobs--
			if remainingJobs < 1 {
				chanDone <- true
				break
			}
		}
	}()

	// Delete
	if len(objectToremovePaths) > 0 {
		for _, p := range objectToremovePaths {
			for {
				if threadsCount < 10 {
					threadsCount++
					break
				}
				time.Sleep(1 * time.Second)
			}
			go func(path string) {
				err = s.DeleteObject(path)
				if err != nil {
					chanDone <- true
				}
				threadsCount--
				chanAJobIsDone <- true
			}(p)
		}
		// Waiting for all jobs
		<-chanDone
	}
	// remove container if needed
	if len(containerToRemove) != 0 {
		err = s.DeleteObject(containerToRemove)
	}
	return err
}

// Helpers
// escapePath returns url.escaped path
func escapePath(path string) string {
	p := strings.Split(path, "/")
	for k, v := range p {
		p[k] = url.QueryEscape(v)
	}
	np := strings.Join(p, "/")
	if strings.HasSuffix(path, "/") {
		np += "/"
	}
	return np
}
