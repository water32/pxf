package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides backwards compatibility for Hive 1.x.x servers.
 * In Hive 1.x.x, an API call get_table_req is made, however this API
 * call was introduced in Hive version 2. This class provides a fallback
 * for older Hive servers to still be able query metadata.
 * <p>
 * The motivation for this approach is taken from here:
 * https://github.com/HotelsDotCom/waggle-dance/pull/133/files
 */
@SuppressWarnings("deprecation")
public class HiveMetaStoreClientCompatibility1xx extends HiveMetaStoreClient implements IMetaStoreClient {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public HiveMetaStoreClientCompatibility1xx(HiveConf conf) throws MetaException {
        super(conf);
    }

    public HiveMetaStoreClientCompatibility1xx(HiveConf conf, HiveMetaHookLoader hookLoader) throws MetaException {
        super(conf, hookLoader);
    }

    public HiveMetaStoreClientCompatibility1xx(HiveConf conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    /**
     * Returns the table given a dbname and table name. This will fallback
     * in Hive 1.x.x servers when a get_table_req API call is made.
     *
     * @param dbname The database the table is located in.
     * @param name   Name of the table to fetch.
     * @return An object representing the table.
     * @throws TException A thrift communication error occurred
     */
    @Override
    public Table getTable(String dbname, String name) throws TException {
        try {
            return super.getTable(dbname, name);
        } catch (TException e) {
            try {
                LOG.debug("Couldn't invoke method getTable");
                if (e.getClass().isAssignableFrom(TApplicationException.class)) {
                    LOG.debug("Attempting to fallback");
                    Table table = client.get_table(dbname, name);
                    return new GetTableResult(table).getTable();
                }
            } catch (MetaException | NoSuchObjectException ex) {
                LOG.debug("Original exception not re-thrown", e);
                throw ex;
            } catch (TTransportException transportException) {
                /*
                Propagate a TTransportException to allow RetryingMetaStoreClient (which proxies this class) to retry connecting to the metastore.
                The number of retries can be set in the hive-site.xml using hive.metastore.failure.retries.
                 */
                throw transportException;
            } catch (Throwable t) {
                LOG.warn("Unable to run compatibility for metastore client method get_table_req. Will rethrow original exception: ", t);
            }
            throw e;
        }
    }

    /**
     * With the upgrade to Hive Client 3.1.2, there is incompatibility with older Hive servers.
     * Specifically, a call using the newer hive client has a change in the HiveMetaStoreClient.java.
     * Hive Client 3.1.2 now prepends the catalog name in front of the database name and causes errors
     * with order hive clients. Ex: NoSuchObjectException(message:@hive#default.test_hive table not found)
     *
     * @param db_name     The database the table is located in.
     * @param tbl_name    Name of the table to fetch.
     * @param max_parts   Maximum number of partitions to return.
     * @return            A list of partitions.
     * @throws TException A thrift communication error occurred
     */
    @Override
    public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts) throws TException {
        try {
            return super.listPartitions(db_name, tbl_name, max_parts);
        } catch (TException e) {
            try {
                LOG.debug("Couldn't invoke method listPartitions");
                if (e.getClass().isAssignableFrom(NoSuchObjectException.class)) {
                    LOG.debug("Attempting to fallback");
                    List<Partition> parts = this.client.get_partitions(db_name, tbl_name, max_parts);
                    return deepCopyPartitions(parts);
                }
            } catch (MetaException | NoSuchObjectException ex) {
                LOG.debug("Original exception not re-thrown", e);
                throw ex;
            } catch (Throwable t) {
                LOG.warn("Unable to run compatibility for metastore client method listPartitions. Will rethrow original exception: ", t);
            }
        throw e;

        }
    }

    /**
     * With the upgrade to Hive Client 3.1.2, there is incompatibility with older Hive servers.
     * Specifically, a call using the newer hive client has a change in the HiveMetaStoreClient.java.
     * Hive Client 3.1.2 now prepends the catalog name in front of the database name and causes errors
     * with order hive clients. Ex: NoSuchObjectException(message:@hive#default.test_hive table not found)
     *
     * @param db_name     The database the table is located in.
     * @param tbl_name    Name of the table to fetch.
     * @param filter      The filter string
     * @param max_parts   Maximum number of partitions to return.
     * @return            A list of partitions.
     * @throws TException A thrift communication error occurred
     */
    @Override
    public List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter, short max_parts) throws TException {
        try {
            return super.listPartitionsByFilter(db_name, tbl_name, filter, max_parts);
        } catch (TException e) {
            try {
                LOG.debug("Couldn't invoke method listPartitionsByFilter");
                if (e.getClass().isAssignableFrom(NoSuchObjectException.class)) {
                    LOG.debug("Attempting to fallback");
                    List<Partition> parts = this.client.get_partitions_by_filter(db_name, tbl_name, filter, max_parts);
                    return deepCopyPartitions(parts);
                }
            } catch (MetaException | NoSuchObjectException ex) {
                LOG.debug("Original exception not re-thrown", e);
                throw ex;
            } catch (Throwable t) {
                LOG.warn("Unable to run compatibility for metastore client method listPartitionsByFilter. Will rethrow original exception: ", t);
            }
            throw e;

        }
    }

    /**
     * Keeping with the existing functionality in HiveMetaStoreClient.java which deep copies
     * the list of partitions retrieved from the metastore. This function re-implements the
     * function of the same name in HiveMetaStoreClient.java
     *
     * @param partitions    the list of partitions to copy
     * @return              A copy of the given list of partitions
     */
    private List<Partition> deepCopyPartitions(List<Partition> partitions) {
        if (partitions == null) {
            return null;
        }
        List<Partition> copy = new ArrayList<>(partitions.size());

        for (Partition part : partitions) {
            copy.add(deepCopy(part));
        }
        return copy;
    }
}
