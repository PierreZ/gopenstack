package gopenstack

type Endpoint struct {
	Id               string `json:"id"`
	Interface        string `json:"interface"`
	LegacyEndpointId string `json:"legacy_endpoint_id"`
	Region           string `json:"region"`
	Url              string `json:"url"`
}

type Catalog struct {
	Id        string     `json:"id"`
	Endpoints []Endpoint `json:"endpoints"`
	Type      string     `json:"type"`
}

type Project struct {
	Id     string `json:"id"`
	Domain struct {
		Name string `json:"name"`
	} `json:"domain"`
	Name string `json:"name"`
}

type Role struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type ProjectKeystone struct {
	Id string `json:"id"`
}

type ScopeKeystone struct {
	Project ProjectKeystone `json:"project"`
}

type UserKeystone struct {
	Id     string `json:"id"`
	Domain struct {
		Name string `json:"name"`
	} `json:"domain"`
	Name     string `json:"name"`
	Password string `json:"password"`
}

type Token struct {
	Catalog  []Catalog    `json:"catalog"`
	ExpireAt DateTime     `json:"expires_at"`
	IssuedAt DateTime     `json:"issued_at"`
	Methods  []string     `json:"methods"`
	Project  Project      `json:"project"`
	Roles    []Role       `json:"roles"`
	User     UserKeystone `json:"user"`
}

type Keyring struct {
	Token            Token  `json:"token"`
	XAuthHeaderToken string `json:"X-Auth-Token"`
}

// GetEndpointUrl returns the URL of the endoint
func (k *Keyring) GetEndpointUrl(iType, region string) (url string, err error) {
L:
	for _, item := range k.Token.Catalog {
		if item.Type == iType {
			for _, endpoint := range item.Endpoints {
				if endpoint.Region == region {
					url = endpoint.Url
					break L
				}
			}
		}
	}
	if len(url) == 0 {
		err = ErrEndpointNotFound
	}
	return
}
