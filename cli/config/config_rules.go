package config

import (
	"errors"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"net"
	"regexp"
)

// pxfDeploymentNameNotEmpty
// Deployment::name MUST NOT be null / empty.
func pxfDeploymentNameNotEmpty(p PxfDeployment) error {
	if p.Name == "" {
		return errors.New("the name of the deployment cannot be empty string")
	} else {
		return nil
	}
}

// pxfDeploymentAtLeastOneCluster
// There MUST be at least one Cluster
func pxfDeploymentAtLeastOneCluster(p PxfDeployment) error {
	if len(p.Clusters) == 0 {
		return fmt.Errorf("there should be at least one PXF cluster in the PXF deployment `%s`", p.Name)
	} else {
		return nil
	}
}

// pxfDeploymentHostnameUnique
// Host::hostname MUST be unique (a single host must belong to only one cluster)
func pxfDeploymentHostnameUnique(p PxfDeployment) error {
	hostnameMap := make(map[string]bool)
	for _, cluster := range p.Clusters {
		for _, host := range cluster.Hosts {
			if _, ok := hostnameMap[host.Hostname]; ok {
				return fmt.Errorf("host `%s` must belong to only one pxf cluster across the pxf deployment",
					host.Hostname)
			}
			hostnameMap[host.Hostname] = true
		}
	}
	return nil
}

// pxfDeploymentGroupNameUnique
// ServiceGroup::name MUST be unique across all Clusters
func pxfDeploymentGroupNameUnique(p PxfDeployment) error {
	groupNameMap := make(map[string]bool)
	for _, cluster := range p.Clusters {
		for _, group := range cluster.Groups {
			if _, ok := groupNameMap[group.Name]; ok {
				return fmt.Errorf("PXF group name `%s` must be unique across the pxf deployment",
					group.Name)
			}
			groupNameMap[group.Name] = true
		}
	}
	return nil
}

// pxfDeploymentNoMoreThanOneCollocated
// There MUST be no more than one Cluster::collocated==true
func pxfDeploymentNoMoreThanOneCollocated(p PxfDeployment) error {
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

// pxfClusterNameNotEmpty Cluster::name MUST NOT be null / empty.
func pxfClusterNameNotEmpty(p PxfCluster) error {
	if p.Name == "" {
		return errors.New("the name of the PXF cluster cannot be empty string")
	} else {
		return nil
	}
}

// pxfClusterIgnoreHostsWhenCollocatedWarning
// Cluster::collocated==true SHOULD NOT have a list of hosts, they WILL be ignored.
func pxfClusterIgnoreHostsWhenCollocatedWarning(p PxfCluster) error {
	if p.Collocated && len(p.Hosts) != 0 {
		gplog.Warn("PXF host list of cluster `%s` will be ignored, "+
			"because cluster `%s` is marked as collocated", p.Name, p.Name)
	}
	return nil
}

// pxfClusterIgnoreEndpointWhenCollocatedWarning
// Cluster::collocated==true SHOULD NOT have a Cluster::endpoint, it WILL be ignored.
func pxfClusterIgnoreEndpointWhenCollocatedWarning(p PxfCluster) error {
	if p.Collocated && p.Endpoint != "" {
		gplog.Warn("PXF endpoint `%s` of cluster `%s` will be ignored, "+
			"because cluster `%s` is marked as collocated", p.Endpoint, p.Name, p.Name)
	}
	return nil
}

// pxfClusterEndpointOrHostsMustExistWhenExternal
// Cluster::collocated==false AND Cluster::endpoint==NULL MUST have a non-empty list of hosts.
func pxfClusterEndpointOrHostsMustExistWhenExternal(p PxfCluster) error {
	if !p.Collocated && p.Endpoint == "" && len(p.Hosts) == 0 {
		return fmt.Errorf("the host list and endpoint cannot be both empty for the external PXF cluster `%s`",
			p.Name)
	}
	return nil
}

// pxfClusterValidEndpoint
// Cluster::endpoint != NULL MUST have a value of a valid IPv4, IPv6 of FQDN compliant syntax.
func pxfClusterValidEndpoint(p PxfCluster) error {
	if p.Endpoint != "" && !IsValidAddress(p.Endpoint) {
		return fmt.Errorf("the endpoint `%s` of the PXF cluster `%s` is not a valid IPv4 address "+
			"or FQDN", p.Endpoint, p.Name)
	}
	return nil
}

// pxfClusterAtLeastOneServiceGroup
// There MUST be at least one ServiceGroup per Cluster
func pxfClusterAtLeastOneServiceGroup(p PxfCluster) error {
	if len(p.Groups) == 0 {
		return fmt.Errorf("there should be at least one service group in PXF cluster `%s`", p.Name)
	}
	return nil
}

// pxfServiceGroupNameNotEmpty
// ServiceGroup::name MUST NOT be null / empty.
func pxfServiceGroupNameNotEmpty(p PxfServiceGroup) error {
	if p.Name == "" {
		return errors.New("the name of the service group cannot be empty string")
	} else {
		return nil
	}
}

// pxfServiceGroupAtLeastOnePort
// ServiceGroup::ports MUST be a non-empty array with at least 1 element
func pxfServiceGroupAtLeastOnePort(p PxfServiceGroup) error {
	if len(p.Ports) == 0 {
		return fmt.Errorf("there should be at least one port in PXF service group `%s`", p.Name)
	}
	return nil
}

// pxfServiceGroupValidPorts
// ServiceGroup::ports elements MUST be an integer value between 1-65535
func pxfServiceGroupValidPorts(p PxfServiceGroup) error {
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

// pxfServiceGroupPortsWarning
// ServiceGroup::ports elements SHOULD be larger than 1023 and smaller than 32768
// (between privileged and ephemeral ranges)
func pxfServiceGroupPortsWarning(p PxfServiceGroup) error {
	MinPortNumber := 1024
	MaxPortNumber := 32767
	for _, port := range p.Ports {
		if port < MinPortNumber || port > MaxPortNumber {
			gplog.Warn("recommend using port numbers between `%d` - `%d`, "+
				"rather than `%d` under the service group `%s`", MinPortNumber, MaxPortNumber, port, p.Name,
			)
		}
	}
	return nil
}

// pxfHostValidHostname
// Host::hostname MUST have a value with either IPv4, IPv6 of FQDN compliant syntax.
func pxfHostValidHostname(p PxfHost) error {
	if p.Hostname != "" && !IsValidAddress(p.Hostname) {
		return fmt.Errorf("the hostname `%s` is not a valid IPv4 address "+
			"or FQDN", p.Hostname)
	}
	return nil
}

// IsValidAddress
// A utility function to check if it is a valid IPv4 or FQDN address.
func IsValidAddress(address string) bool {
	// Check if it is a parsable IPv4 address
	if parsedIP := net.ParseIP(address); parsedIP != nil && parsedIP.To4() != nil {
		return true
	}

	// Check if it is an FQDN address
	// Keep it permissive to avoid false negatives during validation. Additional explanations:
	// 1. (?i): Set as case-insensitive regex
	// 2. ([a-z0-9]+(-[a-z0-9]+)*\.)+: Matches the hostname with one or more groups of alphanumeric
	// characters followed by an optional hyphen and more alphanumeric characters, separated by dots
	// E.g. "ab-info.foo.test"
	// 3. [a-z]{2,}: Matches the TLD which is at least two alphabetical characters. E.g. "com", "gov"
	fqdnRegex := `^(?i)([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$`
	match, _ := regexp.MatchString(fqdnRegex, address)
	return match
}
