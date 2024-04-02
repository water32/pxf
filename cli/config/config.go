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

// Validate on deployment level
// (DONE by map) Cluster::name MUST be unique.
// (DONE) Deployment::name MUST NOT be null / empty.
// (DONE) Host::hostname MUST be unique (a single host must belong to only one cluster)
// (DONE) There MUST be at least one Cluster
// (DONE) There MUST be no more than one Cluster::collocated==true
// (DONE) ServiceGroup::name MUST be unique across all Clusters
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
		if err := cluster.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate on cluster level:
// (DONE by bool) Cluster::collocated MUST not be NULL.
// (DONE) Cluster::name MUST NOT be null / empty.
// (DONE) Cluster::collocated==true SHOULD NOT have a list of hosts, they WILL be ignored.
// (DONE) Cluster::collocated==true SHOULD NOT have a Cluster::endpoint, it WILL be ignored.
// (DONE) Cluster::collocated==false AND Cluster::endpoint==NULL MUST have a non-empty list of hosts.
// (DONE) Cluster::endpoint != NULL MUST have a value of a valid IPv4, IPv6 of FQDN compliant syntax.
// (DONE) There MUST be at least one ServiceGroup per Cluster
func (p PxfCluster) Validate() error {
	var validators = []pxfClusterValidator{
		pxfClusterNameNotEmpty,
		pxfClusterIgnoreHostsWhenCollocated,
		pxfClusterIgnoreEndpointWhenCollocated,
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
		if err := group.Validate(); err != nil {
			return err
		}
	}
	for _, host := range p.Hosts {
		if err := host.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate on PxfHost level:
// (DONE) Host::hostname MUST have a value with either IPv4, IPv6 of FQDN compliant syntax.
func (p PxfHost) Validate() error {
	return pxfHostValidHostname(p)
}

// Validate on service group level:
// (DONE) ServiceGroup::name MUST NOT be null / empty.
// (DONE) ServiceGroup::ports MUST be a non-empty array with at least 1 element
// (DONE) ServiceGroup::ports elements MUST be an integer value between 1-65535
// (DONE) ServiceGroup::ports elements SHOULD be larger than 1023 and smaller than 32768 (between privileged and ephemeral ranges)
func (p PxfServiceGroup) Validate() error {
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
