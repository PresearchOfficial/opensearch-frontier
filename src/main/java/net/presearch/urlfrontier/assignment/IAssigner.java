package net.presearch.urlfrontier.assignment;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

/**
 * Assigners provide a mechanism for Frontiers to be assigned a set of hash partitions. These hashes
 * divide the crawl space and are associated with a set of queues (grouped by domains or hostnames).
 * The implementations of the assignments can be based on a strongly consistent or eventually
 * consistent mechanism. Each Frontier instance gets its own assignment instance.
 */
public interface IAssigner extends Closeable {

    public static final int DEFAULT_TOTAL_NUMBER_ASSIGNMENTS = 1000;

    public static final int DEFAULT_HEARTBEAT_SEC = 60;

    public static final int DEFAULT_TTL_SEC = DEFAULT_HEARTBEAT_SEC * 2;

    public static final String UUID_CONFIG_NAME = "uuid";

    public static final String HEARBEAT_CONFIG_NAME = "heartbeat";

    public static final String TTL_CONFIG_NAME = "assignments.ttl";

    public static final String TOTAL_ASSIGNMENT_COUNT_CONFIG_NAME = "assignments.total";

    /**
     * Returns the set of partitions currently assigned to a given frontier
     *
     * @return set of hashes - can be empty if no assignments are available.
     */
    Set<String> getPartitionsAssigned();

    void init(Map<String, String> userConfig);

    void setListener(AssignmentsListener listener);
}
