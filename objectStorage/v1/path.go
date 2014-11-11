package objectStorageV1

import (
	"encoding/json"
	"github.com/Toorop/gopenstack"
	"path"
	"strconv"
	"strings"
	"time"
)

// An osPath is a representation of an openstack object storage path
type osPath struct {
	client       *gopenstack.Client
	Name         string
	Ptype        string                // root, container, object, vfolder
	Etag         string                `json:"hash"`
	ContentType  string                `json:"content_type"`
	Bytes        uint64                `json:"bytes"`
	Count        uint                  `json:"count"`
	LastModified gopenstack.DateTimeOs `json:"last_modified"`
}

// NewObjectStoragesPath return an osPath
func NewOsPath(client *gopenstack.Client, rawPath string) *osPath {
	p := new(osPath)
	p.client = client
	// clean path
	if strings.HasSuffix(rawPath, "/") {
		rawPath = rawPath[0 : len(rawPath)-1]
	}
	if !strings.HasPrefix(rawPath, "/") {
		rawPath = "/" + rawPath
	}
	p.Name = path.Clean(rawPath)
	return p
}

// GetType returns the type of the "object" behind the path
// It can be : container, object, vfolder, (more)
func (p *osPath) GetType() (string, error) {
	if p.Ptype != "" {
		return p.Ptype, nil
	}
	resp, err := p.client.Call(&gopenstack.CallOptions{
		Method:    "HEAD",
		Ressource: p.Name,
	})

	err = resp.HandleErr(err, []int{200, 204, 404})
	if err != nil {
		return "", err
	}

	// If 404 it must be a vfolder or nothing
	if resp.StatusCode == 404 {
		// Search vpath in container
		resp, err := p.client.Call(&gopenstack.CallOptions{
			Method:    "GET",
			Ressource: p.GetContainer() + "?format=json",
		})

		err = resp.HandleErr(err, []int{200, 404})

		if resp.StatusCode == 404 {
			return "", gopenstack.ErrPathNotFound(p.Name)
		}
		var objects []object
		if err = json.Unmarshal(resp.Body, &objects); err != nil {
			return "", err
		}
		prefix := p.GetPrefix()
		for _, o := range objects {
			if strings.HasPrefix(o.Name, prefix) {
				p.Ptype = "vfolder"
				return p.Ptype, nil
			}
		}
		return "", gopenstack.ErrPathNotFound(p.Name)
	}

	// Region root
	if resp.Headers["X-Account-Container-Count"] != nil {
		p.Ptype = "root"
		return p.Ptype, nil
	}

	// Container
	if resp.Headers["X-Container-Bytes-Used"] != nil || resp.Headers["X-Container-Object-Count"] != nil {
		p.Ptype = "container"
		return p.Ptype, nil
	}
	// Object
	if resp.Headers["Etag"] != nil {
		p.Ptype = "object"
		return p.Ptype, nil
	}
	return "", gopenstack.ErrPathNotFound(p.Name)
}

// ListChidren returns children of a givent path
func (p *osPath) ListChildren() (children []osPath, err error) {
	pathType, err := p.GetType()
	if err != nil {
		return children, err
	}

	switch pathType {
	case "root":
		// List container
		resp, err := p.client.Call(&gopenstack.CallOptions{
			Method:    "GET",
			Ressource: "?format=json",
		})
		if err = resp.HandleErr(err, []int{200, 204}); err != nil {
			return children, err
		}
		if err = json.Unmarshal(resp.Body, &children); err != nil {
			return children, err
		}
		// add ptype
		for k, _ := range children {
			children[k].Ptype = "container"
		}

	case "container", "vfolder":
		resp, err := p.client.Call(&gopenstack.CallOptions{
			Method:    "GET",
			Ressource: p.GetContainer() + "?format=json&prefix=" + p.GetPrefix(),
			//Ressource: p.GetContainer() + "?format=json&path=dev",
		})
		if err = resp.HandleErr(err, []int{200, 203}); err != nil {
			return children, err
		}
		var tc []osPath
		err = json.Unmarshal(resp.Body, &tc)

		// Remove prefix
		prefix := p.GetPrefix()
		for k, c := range tc {
			if len(prefix) != 0 && strings.Count(c.Name, "/") > 0 {
				tc[k].Name = c.Name[strings.LastIndex(prefix, "/"):]
				posToCut := strings.Index(tc[k].Name, "/")
				if posToCut != -1 {
					tc[k].Name = tc[k].Name[posToCut+1:]
				}
			}
		}
		// Remove suffix
		for k, c := range tc {
			if i := strings.Index(c.Name, "/"); i != -1 {
				tc[k].Name = c.Name[:i]
			}
		}
	L1:
		for _, c := range tc {
			// vfolder && already in
			for k, v := range children {
				if c.Name == v.Name {
					//children[k].Count++
					children[k].Bytes += c.Bytes
					children[k].ContentType = "vfolder"
					children[k].Etag = ""
					continue L1
				}
			}
			if c.Bytes == 0 && c.ContentType == "application/octet-stream" {
				c.ContentType = "vfolder"
				c.Etag = ""
			}
			if c.Name != "" {
				children = append(children, c)
			}
		}

	case "object":
		//fmt.Println("object")
		resp, err := p.client.Call(&gopenstack.CallOptions{
			Method:    "HEAD",
			Ressource: p.Name,
		})
		if err = resp.HandleErr(err, []int{200, 203}); err != nil {
			return children, err
		}
		cp := osPath{}
		cp.Name = p.Name[strings.Index(p.Name, "/"):]
		cp.LastModified.Time, err = time.Parse(time.RFC1123, resp.Headers["Last-Modified"][0])
		if err != nil {
			cp.LastModified.Time = *new(time.Time)
		}
		cp.Etag = resp.Headers["Etag"][0]
		cp.Bytes, _ = strconv.ParseUint(resp.Headers["Content-Length"][0], 10, 64)
		cp.ContentType = resp.Headers["Content-Type"][0]
		children = append(children, cp)
	default:
		err = gopenstack.ErrUnsuportedPathType(pathType)
		return

	}
	return
}

// GetChildrenObjects return children object of a given path
func (p *osPath) GetChildrenObjects() (children []object, err error) {
	resp, err := p.client.Call(&gopenstack.CallOptions{
		Method:    "GET",
		Ressource: p.GetContainer() + "?format=json",
	})
	err = resp.HandleErr(err, []int{200, 404})
	if resp.StatusCode == 404 {
		return children, gopenstack.ErrPathNotFound(p.Name)
	}
	var tObjects []object
	if err = json.Unmarshal(resp.Body, &tObjects); err != nil {
		return children, err
	}
	prefix := p.GetPrefix()
	for _, o := range tObjects {
		if strings.HasPrefix(o.Name, prefix) {
			children = append(children, o)
		}
	}
	return
}

// GetContainer return the container correspondig to a given path
// If path is rooot return a empty syting
func (p *osPath) GetContainer() string {
	return strings.Split(p.Name, "/")[1]
}

// GetPrefix return prfix for API query
// PATH = /container/prefix...
func (p *osPath) GetPrefix() string {
	container := p.GetContainer()
	if len(p.Name) > len(container)+1 {
		return p.Name[len(p.GetContainer())+2:] + "/"
	}
	return ""
}
