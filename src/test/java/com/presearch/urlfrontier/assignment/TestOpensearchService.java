/**
 * SPDX-FileCopyrightText: 2022 Presearch SPDX-License-Identifier: Apache-2.0 Licensed to Presearch
 * under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. DigitalPebble licenses this file to You
 * under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.presearch.urlfrontier.assignment;

import com.presearch.urlfrontier.Constants;
import com.presearch.urlfrontier.OpensearchService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class TestOpensearchService {

    private static String OPENSEARCH_VERSION = "latest";

    static {
        String version = System.getProperty("opensearch-version");
        if (version != null) OPENSEARCH_VERSION = version;
    }

    @Rule
    public GenericContainer opensearchContainer =
            new GenericContainer(
                            DockerImageName.parse(
                                    "opensearchproject/opensearch:" + OPENSEARCH_VERSION))
                    .withExposedPorts(9200)
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("discovery.type", "single-node")
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m");

    @Test
    public void checkIndexCreation() throws Exception {
        String host = opensearchContainer.getHost();
        Integer port = opensearchContainer.getFirstMappedPort();

        Map<String, String> configuration = new HashMap<>();
        configuration.put(Constants.OSUserParamName, "admin");
        configuration.put(Constants.OSPasswordParamName, "admin");
        configuration.put(Constants.OSHostParamName, host);
        configuration.put(Constants.OSPortParamName, port.toString());

        configuration.put(
                Constants.AssignmentClassParamName,
                "com.presearch.urlfrontier.assignment.DummyAssigner");

        OpensearchService service = new OpensearchService(configuration);

        // start it as a grpc service
        int grpcPort = ThreadLocalRandom.current().nextInt(7000, 8000);
        Server server = ServerBuilder.forPort(grpcPort).addService(service).build();
        server.start();

        // at this stage it has been given all the assignments
        // we need to push some URLs to it

        // and wait a bit to give it a chance to map from the assignments to the queues

        server.shutdownNow();
    }
}
