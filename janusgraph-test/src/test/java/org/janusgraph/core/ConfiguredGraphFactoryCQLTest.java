// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core;

import com.datastax.driver.core.Session;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ConfiguredGraphFactoryCQLTest {

    @Container
    public static final JanusGraphCassandraContainer cqlContainer;
    private static final JanusGraphManager gm;

    static {
        cqlContainer = new JanusGraphCassandraContainer();
        gm = new JanusGraphManager(new Settings());
    }

    @BeforeEach
    public void setup() {
        // Create ConfigurationManagementGraph instance after JanusGraphCassandraContainer
        // is ready.
        try {
            ConfigurationManagementGraph.getInstance();
        } catch (ConfigurationManagementGraphNotEnabledException e) {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "cql");
            map.put(STORAGE_HOSTS.toStringWithoutRoot(), cqlContainer.getContainerIpAddress());
            map.put(STORAGE_PORT.toStringWithoutRoot(), cqlContainer.getFirstMappedPort());
            final MapConfiguration config = new MapConfiguration(map);
            final StandardJanusGraph mgmtGraph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
            // Instantiate the ConfigurationManagementGraph Singleton
            new ConfigurationManagementGraph(mgmtGraph);
        }
    }

    @Test
    public void dropGraphShouldRemoveGraphFromCache() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "cql");
            map.put(STORAGE_HOSTS.toStringWithoutRoot(), cqlContainer.getContainerIpAddress());
            map.put(STORAGE_PORT.toStringWithoutRoot(), cqlContainer.getFirstMappedPort());
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));

            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);

            ConfiguredGraphFactory.drop("graph1");
            assertNull(gm.getGraph("graph1"));
            assertTrue(graph.isClosed());

            Session cql = cqlContainer.getCluster().connect();
            Object graph_keyspace = cql.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?", "graph1").one();
            cql.close();

            assertNull(graph_keyspace);
        } catch (Exception e) {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }
}

