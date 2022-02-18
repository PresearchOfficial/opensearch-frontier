package net.presearch.urlfrontier.assignment;

/**
 * Implemented by Frontiers so that the Assigners can notify them that there has been a change in
 * the partitions assigned to them.
 */
public interface AssignmentsListener {
    /** Notifies that there has been a change in the assignments of partitions */
    void setAssignmentsChanged();
}
