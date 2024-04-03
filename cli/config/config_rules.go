package config

import (
	"errors"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"net"
	"regexp"
)

// pxfDeploymentNameNotEmpty
// PXF deployment name MUST NOT be empty
func pxfDeploymentNameNotEmpty(p PxfDeployment) error {
	if p.Name == "" {
		return errors.New("the name of the deployment cannot be empty string")
	} else {
		return nil
	}
}

// pxfDeploymentAtLeastOneCluster
// PXF deployment MUST have at least one cluster
func pxfDeploymentAtLeastOneCluster(p PxfDeployment) error {
	if len(p.Clusters) == 0 {
		return fmt.Errorf("there should be at least one PXF cluster in the PXF deployment `%s`", p.Name)
	} else {
		return nil
	}
}

// pxfDeploymentHostnameUnique
// PXF hosts MUST have unique hostnames (a single host must belong to only one PXF cluster)
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
// PXF service group MUST have unique names across all PXF clusters
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
// There MUST be no more than one collocated PXF cluster
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

// pxfClusterNameNotEmpty
// PXF cluster name MUST NOT be empty
func pxfClusterNameNotEmpty(p PxfCluster) error {
	if p.Name == "" {
		return errors.New("the name of the PXF cluster cannot be empty string")
	} else {
		return nil
	}
}

// pxfClusterIgnoreHostsWhenCollocatedWarning
// A collocated PXF cluster SHOULD NOT have a list of hosts, and they will be ignored
// A warning will be generated
func pxfClusterIgnoreHostsWhenCollocatedWarning(p PxfCluster) error {
	if p.Collocated && len(p.Hosts) != 0 {
		gplog.Warn("PXF host list of cluster `%s` will be ignored, "+
			"because cluster `%s` is marked as collocated", p.Name, p.Name)
	}
	return nil
}

// pxfClusterIgnoreEndpointWhenCollocatedWarning
// A collocated PXF cluster SHOULD NOT have an endpoint, and it will be ignored
// A warning will be generated
func pxfClusterIgnoreEndpointWhenCollocatedWarning(p PxfCluster) error {
	if p.Collocated && p.Endpoint != "" {
		gplog.Warn("PXF endpoint `%s` of cluster `%s` will be ignored, "+
			"because cluster `%s` is marked as collocated", p.Endpoint, p.Name, p.Name)
	}
	return nil
}

// pxfClusterEndpointOrHostsMustExistWhenExternal
// An external cluster MUST have either a non-empty list of hosts or an endpoint or both
func pxfClusterEndpointOrHostsMustExistWhenExternal(p PxfCluster) error {
	if !p.Collocated && p.Endpoint == "" && len(p.Hosts) == 0 {
		return fmt.Errorf("the host list and endpoint cannot be both empty for the external PXF cluster `%s`",
			p.Name)
	}
	return nil
}

// pxfClusterValidEndpoint
// A non-empty endpoint of a PXF cluster MUST have a value of either a valid IPv4 address or FQDN
func pxfClusterValidEndpoint(p PxfCluster) error {
	if p.Endpoint != "" && !IsValidAddress(p.Endpoint) {
		return fmt.Errorf("the endpoint `%s` of the PXF cluster `%s` is not a valid IPv4 address "+
			"or FQDN", p.Endpoint, p.Name)
	}
	return nil
}

// pxfClusterAtLeastOneServiceGroup
// A PXF cluster MUST have at least one PXF service group
func pxfClusterAtLeastOneServiceGroup(p PxfCluster) error {
	if len(p.Groups) == 0 {
		return fmt.Errorf("there should be at least one service group in PXF cluster `%s`", p.Name)
	}
	return nil
}

// pxfServiceGroupNameNotEmpty
// PXF service group name MUST NOT be empty
func pxfServiceGroupNameNotEmpty(p PxfServiceGroup) error {
	if p.Name == "" {
		return errors.New("the name of the service group cannot be empty string")
	} else {
		return nil
	}
}

// pxfServiceGroupAtLeastOnePort
// PXF service group MUST have more than one port
func pxfServiceGroupAtLeastOnePort(p PxfServiceGroup) error {
	if len(p.Ports) == 0 {
		return fmt.Errorf("there should be at least one port in PXF service group `%s`", p.Name)
	}
	return nil
}

// pxfServiceGroupValidPorts
// PXF service group MUST have ports which are integers between 1-65535
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
// PXF service group SHOULD have ports which are integers between 1023 and 32768
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
// A PXF host MUST have a hostname which is either a valid IPv4 address or FQDN
func pxfHostValidHostname(p PxfHost) error {
	if p.Hostname != "" && !IsValidAddress(p.Hostname) {
		return fmt.Errorf("the hostname `%s` is not a valid IPv4 address "+
			"or FQDN", p.Hostname)
	}
	return nil
}

// IsValidAddress
// A utility function to check if it is a valid IPv4 address or FQDN
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
