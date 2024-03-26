package config

type PxfServiceGroup struct {
	Name  string
	Ports []int
	IsSsl bool
}

type PxfCluster struct {
	Name       string
	Collocated bool
	Hostnames  []string
	Endpoint   string
	Groups     []PxfServiceGroup
}

type PxfDeployment struct {
	Name     string
	Clusters []PxfCluster
}
