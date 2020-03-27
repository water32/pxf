package org.greenplum.pxf.plugins.jdbc;

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

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcDataSplitterTest {

    private RequestContext context;
    private JdbcDataSplitter splitter;

    @BeforeEach
    public void setUp() {
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("table");
        context.setUser("test-user");
        context.setTotalSegments(1);
        splitter = new JdbcDataSplitter(context, new Configuration());
    }

    @Test
    public void testNoPartition() {

        splitter.initialize(context, new Configuration());

        assertTrue(splitter.hasNext());
        assertNotNull(splitter.next());
        assertFalse(splitter.hasNext());
        assertThrows(NoSuchElementException.class,
            () -> splitter.next());
    }

    @Test
    public void testNoPartitionUsingNextOnly() {

        splitter.initialize(context, new Configuration());

        assertNotNull(splitter.next());
        assertThrows(NoSuchElementException.class,
            () -> splitter.next());
    }

    @Test
    public void testPartitionByTypeInvalid() {
        context.addOption("PARTITION_BY", "level:float");
        assertThrows(IllegalArgumentException.class,
            () -> splitter.initialize(context, new Configuration()));
    }

    @Test
    public void testPartitionByFormatInvalid() {
        context.addOption("PARTITION_BY", "level-enum");
        assertThrows(IllegalArgumentException.class,
            () -> splitter.initialize(context, new Configuration()));
    }
}
