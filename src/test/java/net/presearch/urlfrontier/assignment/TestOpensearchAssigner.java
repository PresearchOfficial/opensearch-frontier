package net.presearch.urlfrontier.assignment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import net.presearch.urlfrontier.Constants;
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
    }

    @Rule
    public GenericContainer opensearchContainer =
            new GenericContainer(DockerImageName.parse("opensearchproject/opensearch:latest"))
                    .withExposedPorts(9200)
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("discovery.type", "single-node")
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m");

    @Test
    public void simpleTestFrontier() throws InterruptedException, IOException {
        String host = opensearchContainer.getHost();
        Integer port = opensearchContainer.getFirstMappedPort();

        Map<String, String> configuration = new HashMap<>();
        configuration.put(Constants.OSHostParamName, host);
        configuration.put(Constants.OSPortParamName, port.toString());

        IAssigner assigner = createAssigner(configuration, "FIRST");

        // sleep for a few seconds
        Thread.sleep(2 * 1000);

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

        int heartbeatSec = 2;

        Map<String, String> configuration = new HashMap<>();
        configuration.put(Constants.OSHostParamName, host);
        configuration.put(Constants.OSPortParamName, port.toString());
        configuration.put(IAssigner.HEARBEAT_CONFIG_NAME, Integer.toString(heartbeatSec));

        IAssigner first = createAssigner(configuration, "FIRST");
        IAssigner second = createAssigner(configuration, "SECOND");

        // sleep for several heartbeats so that the assigners can converge
        Thread.sleep(10 * heartbeatSec * 1000);

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

        int heartbeatSec = 2;

        Map<String, String> configuration = new HashMap<>();
        configuration.put(Constants.OSHostParamName, host);
        configuration.put(Constants.OSPortParamName, port.toString());
        configuration.put(IAssigner.HEARBEAT_CONFIG_NAME, Integer.toString(heartbeatSec));

        IAssigner first = createAssigner(configuration, "FIRST");
        IAssigner second = createAssigner(configuration, "SECOND");
        IAssigner third = createAssigner(configuration, "THIRD");

        // sleep for several heartbeats to give the assigners time to converge
        Thread.sleep(20 * heartbeatSec * 1000);

        // check that no partition has been left behind
        // and that we were able to deal with remainders

        Assert.assertEquals(334, first.getPartitionsAssigned().size());
        Assert.assertEquals(333, second.getPartitionsAssigned().size());
        Assert.assertEquals(333, third.getPartitionsAssigned().size());
    }
}
