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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Unit tests to check that the hash assignments is working fine */
public class TestOpensearchAssigner {

    static class AssignmentHolder implements AssignmentsListener {

        boolean changed = false;

        @Override
        public void setAssignmentsChanged() {
            changed = true;
        }

        public boolean hasChanged() {
            return changed;
        }

        public void setChanged(boolean change) {
            changed = change;
        }

        @Override
        public String getHostAndPort() {
            return null;
        }

        @Override
        public void setNodes(Set<String> n) {}
    }

    @Rule
    public GenericContainer opensearchContainer =
            new GenericContainer(DockerImageName.parse("opensearchproject/opensearch:1.3.1"))
                    .withExposedPorts(9200)
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("discovery.type", "single-node")
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m");

    @Test
    public void simpleTestFrontier() throws InterruptedException, IOException {
        String host = opensearchContainer.getHost();
        Integer port = opensearchContainer.getFirstMappedPort();

        Map<String, String> configuration = new HashMap<>();
        configuration.put(Constants.OSUserParamName, "admin");
        configuration.put(Constants.OSPasswordParamName, "admin");
        configuration.put(Constants.OSHostParamName, host);
        configuration.put(Constants.OSPortParamName, port.toString());

        int heartbeatSec = 10;
        configuration.put(IAssigner.HEARBEAT_CONFIG_NAME, Integer.toString(heartbeatSec));

        IAssigner assigner = createAssigner(configuration, "FIRST");

        // sleep for several heartbeats
        Thread.sleep(2 * heartbeatSec * 1000);

        // on his own - should get the whole lot
        Assert.assertEquals(
                IAssigner.DEFAULT_TOTAL_NUMBER_ASSIGNMENTS,
                assigner.getPartitionsAssigned().size());

        assigner.close();
    }

    @Test
    public void twoFrontiers() throws InterruptedException, IOException {
        String host = opensearchContainer.getHost();
        Integer port = opensearchContainer.getFirstMappedPort();

        int heartbeatSec = 10;

        Map<String, String> configuration = new HashMap<>();
        configuration.put(Constants.OSHostParamName, host);
        configuration.put(Constants.OSPortParamName, port.toString());
        configuration.put(Constants.OSUserParamName, "admin");
        configuration.put(Constants.OSPasswordParamName, "admin");
        configuration.put(IAssigner.HEARBEAT_CONFIG_NAME, Integer.toString(heartbeatSec));

        IAssigner first = createAssigner(configuration, "FIRST");
        IAssigner second = createAssigner(configuration, "SECOND");

        // sleep for several heartbeats so that the assigners can converge
        Thread.sleep(5 * heartbeatSec * 1000);

        // check that they both have half the assignments
        Assert.assertEquals(
                IAssigner.DEFAULT_TOTAL_NUMBER_ASSIGNMENTS / 2,
                first.getPartitionsAssigned().size());
        Assert.assertEquals(
                IAssigner.DEFAULT_TOTAL_NUMBER_ASSIGNMENTS / 2,
                second.getPartitionsAssigned().size());

        // kill the first one
        first.close();

        // sleep for a few heartbeats so that the second assigner can detect that the first has
        // left
        Thread.sleep(3 * heartbeatSec * 1000);

        // the second assigner should have all the partitions
        Assert.assertEquals(
                IAssigner.DEFAULT_TOTAL_NUMBER_ASSIGNMENTS, second.getPartitionsAssigned().size());

        // kill the second one
        second.close();
    }

    private static IAssigner createAssigner(Map<String, String> configuration, String name) {
        AssignmentHolder holder1 = new AssignmentHolder();
        configuration.put(IAssigner.UUID_CONFIG_NAME, name);
        IAssigner assign = new OpensearchAssigner();
        assign.setListener(holder1);
        assign.init(configuration);
        return assign;
    }

    @Test
    public void threeFrontiers() throws InterruptedException, IOException {
        String host = opensearchContainer.getHost();
        Integer port = opensearchContainer.getFirstMappedPort();

        int heartbeatSec = 10;

        Map<String, String> configuration = new HashMap<>();
        configuration.put(Constants.OSUserParamName, "admin");
        configuration.put(Constants.OSPasswordParamName, "admin");
        configuration.put(Constants.OSHostParamName, host);
        configuration.put(Constants.OSPortParamName, port.toString());
        configuration.put(IAssigner.HEARBEAT_CONFIG_NAME, Integer.toString(heartbeatSec));

        IAssigner first = createAssigner(configuration, "FIRST");
        IAssigner second = createAssigner(configuration, "SECOND");
        IAssigner third = createAssigner(configuration, "THIRD");

        // sleep for several heartbeats to give the assigners time to converge
        Thread.sleep(5 * heartbeatSec * 1000);

        // check that no partition has been left behind
        // and that we were able to deal with remainders

        Assert.assertEquals(334, first.getPartitionsAssigned().size());
        Assert.assertEquals(333, second.getPartitionsAssigned().size());
        Assert.assertEquals(333, third.getPartitionsAssigned().size());

        first.close();
        second.close();
        third.close();
    }
}
