package net.presearch.urlfrontier.assignment;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Gives all the partitions to a single Frontier - useful for testing */
public class DummyAssigner implements Runnable, IAssigner {

    private final Set<String> partitionsAssigned = new HashSet<>();

    // get a value by default but could be overridden in the init method
    private int totalNumberOfAssignments = DEFAULT_TOTAL_NUMBER_ASSIGNMENTS;

    // frontier instance using the assigner
    // the interface allows to notify it that its assignments have changed
    private AssignmentsListener listener;

    public DummyAssigner() {}

    @Override
    public void init(Map<String, String> userConfig) {
        for (int i = 0; i < totalNumberOfAssignments; i++) {
            partitionsAssigned.add(Integer.toString(i));
        }
    }

    @Override
    public Set<String> getPartitionsAssigned() {
        return partitionsAssigned;
    }

    @Override
    public void run() {}

    @Override
    public void setListener(AssignmentsListener listener) {
        this.listener = listener;
        this.listener.setAssignmentsChanged();
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
    }
}
