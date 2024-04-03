package config

type PxfServiceGroup struct {
	Name  string
	Ports []int
	IsSsl bool
}
type pxfServiceGroupValidator func(p PxfServiceGroup) error

type PxfCluster struct {
	Name       string
	Collocated bool
	Hosts      []*PxfHost
	Endpoint   string
	Groups     map[string]*PxfServiceGroup
}
type pxfClusterValidator func(p PxfCluster) error

type PxfHost struct {
	Hostname string
}

type PxfDeployment struct {
	Name     string
	Clusters map[string]*PxfCluster
}
type pxfDeploymentValidator func(p PxfDeployment) error

func (p PxfDeployment) Validate() error {
	var validators = []pxfDeploymentValidator{
		pxfDeploymentNameNotEmpty,
		pxfDeploymentAtLeastOneCluster,
		pxfDeploymentHostnameUnique,
		pxfDeploymentNoMoreThanOneCollocated,
		pxfDeploymentGroupNameUnique,
	}
	for _, validator := range validators {
		if err := validator(p); err != nil {
			return err
		}
	}
	for _, cluster := range p.Clusters {
		if err := cluster.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (p PxfCluster) validate() error {
	var validators = []pxfClusterValidator{
		pxfClusterNameNotEmpty,
		pxfClusterIgnoreHostsWhenCollocatedWarning,
		pxfClusterIgnoreEndpointWhenCollocatedWarning,
		pxfClusterEndpointOrHostsMustExistWhenExternal,
		pxfClusterValidEndpoint,
		pxfClusterAtLeastOneServiceGroup,
	}
	for _, validator := range validators {
		if err := validator(p); err != nil {
			return err
		}
	}
	for _, group := range p.Groups {
		if err := group.validate(); err != nil {
			return err
		}
	}
	for _, host := range p.Hosts {
		if err := host.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (p PxfHost) validate() error {
	return pxfHostValidHostname(p)
}

func (p PxfServiceGroup) validate() error {
	validators := []pxfServiceGroupValidator{
		pxfServiceGroupNameNotEmpty,
		pxfServiceGroupAtLeastOnePort,
		pxfServiceGroupValidPorts,
		pxfServiceGroupPortsWarning,
	}
	for _, validator := range validators {
		if err := validator(p); err != nil {
			return err
		}
	}
	return nil
}
