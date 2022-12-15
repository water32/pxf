# Known Issues

## 20221212

### Component

PXF service

### Description

When querying a PXF external table that is configured fro read from cloud
storage (e.g., AWS S3, GCS, etc) after querying a different PXF external table
that is configured to read from a Hadoop (HDFS and/or Hive) cluster with
Kerberos authentication, the query fails with the following error

```console
ERROR:  PXF server error : Can't get Master Kerberos principal for use as renewer
```

See #909 for more details.

### Details

When PXF's `HdfsDataFragmenter` is determining the set of fragments that it
needs to read for a given query, it ends up calling the `listStatus` method of
`FileInputFormat`. One of the first things this method does is call the
[`obtainTokensForNamenodes`][0] method of `TokenCache`. This method first
checks if security is enabled by calling the static method `isSecurityEnabled`
of `UserGroupInformation` before doing the actual work of obtaining delegation
tokens from the namenodes. Normally when accessing Google Cloud Storage via its
Hadoop connector, `isSecurityEnabled` should return false; however, PXF may be
serving multiple concurrent queries using different servers, some of which are
using Kerberos authentication. This can cause `isSecurityEnabled` to
incorrectly report true.

It looks like this was encountered with PXF before and addressed in commit
b4074356. The relevant bit from that change was setting
`mapreduce.job.hdfs-servers.token-renewal.exclude` to the bucket name. The
effect of doing this, is that `TokenCache` will [skip renewing tokens][1].
However, in the case of a bucket name with an underscore in the name,
`isTokenRenewalExcluded` will always return false. The reason for that is
that underscore is a not valid character for hostnames. When
`isTokenRenewalExcluded` gets the host from the [`FileSystem`][2], it will
always be `null`. For the case of a bucket name that only contains valid
characters (letters from a to z, the digits from 0 to 9, and the hyphen), the
bucket name will be returned as the host and this will equal the value that PXF
sets `mapreduce.job.hdfs-servers.token-renewal.exclude` to.

[0]: https://github.com/apache/hadoop/blob/rel/release-2.10.2/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/FileInputFormat.java#L213
[1]: https://github.com/apache/hadoop/blob/rel/release-2.10.2/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/security/TokenCache.java#L130-L132
[2]: https://github.com/apache/hadoop/blob/rel/release-2.10.2/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/security/TokenCache.java#L110

### Workaround

If you plan to use PXF to read files from both cloud storage (e.g., AWS S3,
GCS, etc) and Hadoop (HDFS and/or Hive) clusters configured with Kerberos
authentication, ensure that your cloud storage buckets only contain the ASCII
letters 'a' through 'z', the digits '0' through '9', and the hyphen '-'.
