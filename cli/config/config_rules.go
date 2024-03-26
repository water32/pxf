package config

import (
	"errors"
	"fmt"
	"net"
	"regexp"
)

// PxfDeploymentNameNotEmpty Deployment::name MUST NOT be null / empty.
func PxfDeploymentNameNotEmpty(p PxfDeployment) error {
	if p.Name == "" {
		return errors.New("the name of the deployment cannot be empty string")
	} else {
		return nil
	}
}

// PxfDeploymentAtLeastOneCluster There MUST be at least one Cluster
func PxfDeploymentAtLeastOneCluster(p PxfDeployment) error {
	if len(p.Clusters) == 0 {
		return fmt.Errorf("there should be at least one PXF cluster in PXF deployment `%s`", p.Name)
	} else {
		return nil
	}
}

// PxfDeploymentHostnameUnique Host::hostname MUST be unique (a single host must belong to only one cluster)
func PxfDeploymentHostnameUnique(p PxfDeployment) error {
	hostnameMap := make(map[string]bool)
	for _, cluster := range p.Clusters {
		for _, host := range cluster.Hosts {
			if _, ok := hostnameMap[host.Hostname]; ok {
				return fmt.Errorf("host `%s` must belong to only one pxf cluster across the pxf deplopyment",
					host.Hostname)
			}
			hostnameMap[host.Hostname] = true
		}
	}
	return nil
}

// PxfDeploymentGroupNameUnique ServiceGroup::name MUST be unique across all Clusters
func PxfDeploymentGroupNameUnique(p PxfDeployment) error {
	groupNameMap := make(map[string]bool)
	for _, cluster := range p.Clusters {
		for _, group := range cluster.Groups {
			if _, ok := groupNameMap[group.Name]; ok {
				return fmt.Errorf("PXF group name `%s` must be unique across the pxf deplopyment",
					group.Name)
			}
			groupNameMap[group.Name] = true
		}
	}
	return nil
}

// PxfDeploymentNoMoreThanOneCollocated There MUST be no more than one Cluster::collocated==true
func PxfDeploymentNoMoreThanOneCollocated(p PxfDeployment) error {
	found := false
	for _, cluster := range p.Clusters {
		if cluster.Collocated {
			if found {
				return fmt.Errorf("there must be no more than one collocated PXF cluster")
			} else {
				found = true
			}
		}
	}
	return nil
}

// PxfClusterNameNotEmpty Cluster::name MUST NOT be null / empty.
func PxfClusterNameNotEmpty(p PxfCluster) error {
	if p.Name == "" {
		return errors.New("the name of the cluster cannot be empty string")
	} else {
		return nil
	}
}

// PxfClusterIgnoreHostsWhenCollocated Cluster::collocated==true SHOULD NOT have a list of hosts, they WILL be ignored.
func PxfClusterIgnoreHostsWhenCollocated(p PxfCluster) error {
	if p.Collocated && len(p.Hosts) != 0 {
		fmt.Printf("[Warning] PXF host list of cluster `%s` will be ignored, "+
			"because cluster `%s` is marked as collocated", p.Name, p.Name)
	}
	return nil
}

// PxfClusterIgnoreEndpointWhenCollocated Cluster::collocated==true SHOULD NOT have a Cluster::endpoint, it WILL be ignored.
func PxfClusterIgnoreEndpointWhenCollocated(p PxfCluster) error {
	if p.Collocated && p.Endpoint != "" {
		fmt.Printf("[Warning] PXF endpoint `%s` of cluster `%s` will be ignored, "+
			"because cluster `%s` is marked as collocated", p.Endpoint, p.Name, p.Name)
	}
	return nil
}

// PxfClusterEndpointOrHostsMustExistWhenExternal
// Cluster::collocated==false AND Cluster::endpoint==NULL MUST have a non-empty list of hosts.
func PxfClusterEndpointOrHostsMustExistWhenExternal(p PxfCluster) error {
	if !p.Collocated && p.Endpoint == "" && len(p.Hosts) == 0 {
		return fmt.Errorf("the host list and endpoint cannot be both empty for the external PXF cluster `%s`",
			p.Name)
	}
	return nil
}

// PxfClusterValidEndpoint
// TODO: Cluster::endpoint != NULL MUST have a value of a valid IPv4, IPv6 of FQDN compliant syntax.
func PxfClusterValidEndpoint(p PxfCluster) error {
	return nil
}

// PxfClusterAtLeastOneServiceGroup There MUST be at least one ServiceGroup per Cluster
func PxfClusterAtLeastOneServiceGroup(p PxfCluster) error {
	if len(p.Groups) == 0 {
		return fmt.Errorf("there should be at least one Service Group in PXF cluster `%s`", p.Name)
	}
	return nil
}

// IsValidAddress Helper function to check if it is a valid IPv4 or FQDN address.
func IsValidAddress(address string) bool {
	// Check if it is a parsable IPv4 address
	if parsedIP := net.ParseIP(address); parsedIP != nil && parsedIP.To4() != nil {
		return true
	}

	// Check if it is an FQDN address
	// Keep it permissive to avoid false negatives during validation. Additional explanations:
	// 1. (?i): Set as case-insensitive regex
	// 2. ([a-z0-9]+(-[a-z0-9]+)*\.)+: Matches the hostname with one or more groups of alphanumeric
	// characters followed by an optional hyphen and more alphanumeric characters, separated by dots.
	// E.g. "ab-info.foo.test"
	// 3. [a-z]{2,}: Matches the TLD which is at least two lowercase alphabetical characters. E.g. "com", "gov"
	fqdnRegex := `^(?i)([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$`
	match, _ := regexp.MatchString(fqdnRegex, address)
	return match
}

func PxfClusterEndpointValidFqdn(p PxfCluster) error {
	if !IsValidAddress(p.Endpoint) {
		return fmt.Errorf("the endpoint `%s` of the PXF cluster `%s` is not a valid IPv4 address, "+
			"IPv6 address or FQDN", p.Endpoint, p.Name)
	}
	return nil
}

// PxfServiceGroupNameNotEmpty ServiceGroup::name MUST NOT be null / empty.
func PxfServiceGroupNameNotEmpty(p PxfServiceGroup) error {
	if p.Name == "" {
		return errors.New("the name of the service group cannot be empty string")
	} else {
		return nil
	}
}

// PxfServiceGroupAtLeastOnePort ServiceGroup::ports MUST be a non-empty array with at least 1 element
func PxfServiceGroupAtLeastOnePort(p PxfServiceGroup) error {
	if len(p.Ports) == 0 {
		return fmt.Errorf("there should be at least one port in PXF service group `%s`", p.Name)
	}
	return nil
}

// PxfServiceGroupValidPorts ServiceGroup::ports elements MUST be an integer value between 1-65535
func PxfServiceGroupValidPorts(p PxfServiceGroup) error {
	MinPortNumber := 1
	MaxPortNumber := 65535
	for _, port := range p.Ports {
		if port < MinPortNumber || port > MaxPortNumber {
			return fmt.Errorf("invalid port number `%d` under service group `%s`, "+
				"a port number must be between `%d` - `%d`", port, p.Name, MinPortNumber, MaxPortNumber)
		}
	}
	return nil
}

// PxfServiceGroupPortsWarning
// ServiceGroup::ports elements SHOULD be larger than 1023 and smaller than 32768
// (between privileged and ephemeral ranges)
func PxfServiceGroupPortsWarning(p PxfServiceGroup) error {
	MinPortNumber := 1024
	MaxPortNumber := 32767
	for _, port := range p.Ports {
		if port < MinPortNumber || port > MaxPortNumber {
			fmt.Printf("[Warning] recommend using port numbers bewteen `%d` - `%d`, "+
				"rather than `%d` under the service group `%s`", MinPortNumber, MaxPortNumber, port, p.Name,
			)
		}
	}
	return nil
}
