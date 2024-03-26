package config

type PxfServiceGroup struct {
	Ports []int
	IsSsl bool
}

type PxfCluster struct {
	Name       string
	Collocated bool
	Endpoint   string
	Groups     map[string]PxfServiceGroup
}
