package net.presearch.urlfrontier.assignment;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import net.presearch.urlfrontier.Constants;
import net.presearch.urlfrontier.OpensearchService;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class TestOpensearchService {

    @Rule
    public GenericContainer opensearchContainer =
            new GenericContainer(DockerImageName.parse("opensearchproject/opensearch:latest"))
                    .withExposedPorts(9200)
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("discovery.type", "single-node")
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m");

    @Test
    public void checkIndexCreation() throws Exception {
        String host = opensearchContainer.getHost();
        Integer port = opensearchContainer.getFirstMappedPort();

        Map<String, String> configuration = new HashMap<>();
        configuration.put(Constants.OSHostParamName, host);
        configuration.put(Constants.OSPortParamName, port.toString());

        configuration.put(
                Constants.AssignmentClassParamName,
                "net.presearch.urlfrontier.assignment.DummyAssigner");

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
