package org.greenplum.pxf.api.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.configuration.ProtocolSettings;
import org.greenplum.pxf.api.utilities.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Common configuration available to all PXF plugins. Represents input data
 * coming from client applications, such as Greenplum.
 */
@Data
public class RequestContext {

    /**
     * The request type can be used to later determine whether we
     * are in a read, write or fragmenter call.
     */
    private RequestType requestType;

    public enum RequestType {
        FRAGMENTER,
        READ_BRIDGE,
        WRITE_BRIDGE,
        READ_CONTROLLER,
        WRITE_CONTROLLER
    }

    // ----- NAMED PROPERTIES -----

    /**
     * The name of the server configuration for this request.
     */
    @Setter(AccessLevel.NONE)
    private String config;

    /**
     * The data source of the required resource (i.e a file path or a table
     * name).
     */
    private String dataSource;

    /**
     * The index of a fragment in a file
     */
    private int fragmentIndex;

    /**
     * The byte serialization of a data fragment.
     */
    private byte[] fragmentMetadata = null;


    /**
     * The filter string, <tt>null</tt> if #hasFilter is <tt>false</tt>.
     */
    private String filterString;

    /**
     * The ClassName for the java class that was defined as Metadata or null
     * if no metadata was defined.
     */
    private Object metadata;

    /**
     * The current output format, either {@link OutputFormat#TEXT},
     * {@link OutputFormat#Binary} or {@link OutputFormat#GPDBWritable}.
     */
    private OutputFormat outputFormat;

    /**
     * The server port providing the service.
     */
    private int port;

    /**
     * The server name providing the service.
     */
    private String host;

    /**
     * The Kerberos token information.
     */
    private String token;

    /**
     * Statistics parameter. Returns the max number of fragments to return for
     * ANALYZE sampling. The value is set in Greenplum side using the GUC
     * pxf_stats_max_fragments.
     */
    @Setter(AccessLevel.NONE)
    private int statsMaxFragments = 0;

    /**
     * Statistics parameter. Returns a number between 0.0001 and 1.0,
     * representing the sampling ratio on each fragment for ANALYZE sampling.
     * The value is set in Greenplum side based on ANALYZE computations and the
     * number of sampled fragments.
     */
    @Setter(AccessLevel.NONE)
    private float statsSampleRatio = 0;

    /**
     * Number of attributes projected in query.
     * <p>
     * Example:
     * SELECT col1, col2, col3... : number of attributes projected - 3
     * SELECT col1, col2, col3... WHERE col4=a : number of attributes projected - 4
     * SELECT *... : number of attributes projected - 0
     */
    private int numAttrsProjected;

    /**
     * The scheme defined at the profile level
     */
    private String profileScheme;

    /**
     * The protocol defined at the foreign data wrapper (FDW) level
     */
    private String protocol;

    /**
     * The format defined at the FDW foreign table level
     */
    private String format;

    /**
     * Encapsulates CSV parsing information
     */
    private GreenplumCSV greenplumCSV = new GreenplumCSV();

    /**
     * The name of the recordkey column. It can appear in any location in the
     * columns list. By specifying the recordkey column, the user declares that
     * he is interested to receive for every record retrieved also the the
     * recordkey in the database. The recordkey is present in HBase table (it is
     * called rowkey), and in sequence files. When the HDFS storage element
     * queried will not have a recordkey and the user will still specify it in
     * the "create external table" statement, then the values for this field
     * will be null. This field will always be the first field in the tuple
     * returned.
     */
    private ColumnDescriptor recordkeyColumn;

    /**
     * The contents of pxf_remote_service_login set in Greenplum. Should the
     * user set it to an empty string this function will return null.
     */
    private String remoteLogin;

    /**
     * The contents of pxf_remote_service_secret set in Greenplum. Should the
     * user set it to an empty string this function will return null.
     */
    private String remoteSecret;

    /**
     * The current segment ID in Greenplum.
     */
    private int segmentId;

    /**
     * The transaction ID for the current Greenplum query.
     */
    private String transactionId;

    /**
     * The name of the server to access. The name will be used to build
     * a path for the config files (i.e. $PXF_CONF/servers/$serverName/*.xml)
     */
    @Setter(AccessLevel.NONE)
    private String serverName = "default";

    /**
     * The number of segments in Greenplum.
     */
    private int totalSegments;

    /**
     * Whether this request is thread safe. If it is not, request will be
     * handled sequentially and not in parallel.
     */
    private boolean threadSafe = true;

    /**
     * The list of column descriptors
     */
    private List<ColumnDescriptor> tupleDescription = new ArrayList<>();

    /**
     * The identity of the end-user making the request.
     */
    private String user;

    /**
     * Any custom user data that may have been passed from the fragmenter.
     * Will mostly be used by the accessor or resolver.
     */
    private byte[] userData;

    /**
     * Additional Configuration Properties to be added to configuration for
     * the request
     */
    private Map<String, String> additionalConfigProps;

    /**
     * Protocol-specific settings
     */
    private ProtocolSettings protocolSettings;

    // ----- USER-DEFINED OPTIONS other than NAMED PROPERTIES -----
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * Returns a String value of the given option or a default value if the option was not provided
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @return string value of the option or default value if the option was not provided
     */
    public String getOption(String option, String defaultValue) {
        return options.getOrDefault(option, defaultValue);
    }

    /**
     * Returns an integer value of the given option or a default value if the option was not provided.
     * Will throw an IllegalArgumentException if the option value can not be represented as an integer
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @return integer value of the option or default value if the option was not provided
     */
    public int getOption(String option, int defaultValue) {
        return getOption(option, defaultValue, false);
    }

    /**
     * Returns an integer value of the given option or a default value if the option was not provided.
     * Will throw an IllegalArgumentException if the option value can not be represented as an integer or
     * if the integer is negative but only natural integer was expected.
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @param naturalOnly  true if the integer is expected to be non-negative (natural), false otherwise
     * @return integer value of the option or default value if the option was not provided
     */
    public int getOption(String option, int defaultValue, boolean naturalOnly) {
        int result = defaultValue;
        String value = options.get(option);
        if (value != null) {
            try {
                result = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                    "Property %s has incorrect value %s : must be a%s integer", option, value, naturalOnly ? " non-negative" : "n"), e);
            }
            if (naturalOnly && result < 0) {
                throw new IllegalArgumentException(String.format(
                    "Property %s has incorrect value %s : must be a non-negative integer", option, value));
            }
        }
        return result;
    }

    /**
     * Returns a string value of the given option or null if the option was not provided.
     *
     * @param option name of the option
     * @return string value of the given option or null if the option was not provided.
     */
    public String getOption(String option) {
        return options.get(option);
    }

    /**
     * Adds an option with the given name and value to the set of options.
     *
     * @param name  name of the option
     * @param value value of the option
     */
    public void addOption(String name, String value) {
        options.put(name, value);
    }

    /**
     * Returns unmodifiable map of options.
     *
     * @return map of options, with keys as option names and values as option values
     */
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    /**
     * Returns true if there is a filter string to parse.
     *
     * @return whether there is a filter string
     */
    public boolean hasFilter() {
        return filterString != null;
    }

    /**
     * Returns true if there is column projection.
     *
     * @return true if there is column projection, false otherwise
     */
    public boolean hasColumnProjection() {
        return numAttrsProjected > 0 && numAttrsProjected < tupleDescription.size();
    }

    /**
     * Returns the number of columns in tuple description.
     *
     * @return number of columns
     */
    public int getColumns() {
        return tupleDescription.size();
    }

    /**
     * Returns column index from tuple description.
     *
     * @param index index of column
     * @return column by index
     */
    public ColumnDescriptor getColumn(int index) {
        return tupleDescription.get(index);
    }

    /**
     * Sets the name of the server configuration for this request.
     *
     * @param config the directory name for the configuration
     */
    public void setConfig(String config) {
        if (StringUtils.isNotBlank(config) && !Utilities.isValidDirectoryName(config)) {
            fail("invalid CONFIG directory name '%s'", config);
        }
        this.config = config;
    }

    /**
     * Sets the name of the server in a multi-server setup.
     * If the name is blank, it is defaulted to "default"
     *
     * @param serverName the name of the server
     */
    public void setServerName(String serverName) {
        if (StringUtils.isNotBlank(serverName)) {

            if (!Utilities.isValidRestrictedDirectoryName(serverName)) {
                throw new IllegalArgumentException(String.format("Invalid server name '%s'", serverName));
            }

            this.serverName = serverName.toLowerCase();
        }
    }

    public void setStatsMaxFragments(int statsMaxFragments) {
        this.statsMaxFragments = statsMaxFragments;
        if (statsMaxFragments <= 0) {
            throw new IllegalArgumentException(String
                .format("Wrong value '%d'. STATS-MAX-FRAGMENTS must be a positive integer",
                    statsMaxFragments));
        }
    }

    public void setStatsSampleRatio(float statsSampleRatio) {
        this.statsSampleRatio = statsSampleRatio;
        if (statsSampleRatio < 0.0001 || statsSampleRatio > 1.0) {
            throw new IllegalArgumentException(
                "Wrong value '"
                    + statsSampleRatio
                    + "'. "
                    + "STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
        }
    }

    public void validate() {
        if ((statsSampleRatio > 0) != (statsMaxFragments > 0)) {
            fail("Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together");
        }

        // accessor and resolver are user properties, might be missing if profile is not set
        ensureNotNull("PROTOCOL", protocol);
    }

    private void ensureNotNull(String property, Object value) {
        if (value == null) {
            fail("Property %s has no value in the current request", property);
        }
    }

    private void fail(String message, Object... args) {
        String errorMessage = String.format(message, args);
        throw new IllegalArgumentException(errorMessage);
    }
}
