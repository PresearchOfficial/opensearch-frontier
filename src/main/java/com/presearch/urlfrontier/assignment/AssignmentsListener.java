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

import java.util.List;

/**
 * Implemented by Frontiers so that the Assigners can notify them that there has been a change in
 * the partitions assigned to them.
 */
public interface AssignmentsListener {
    /** Notifies that there has been a change in the assignments of partitions */
    void setAssignmentsChanged();

    /** Used by heartbeat to identify a Frontier * */
    String getHostAndPort();

    /** Report back to the assigner with the list of nodes in the cluster * */
    public void setNodes(List<String> n);
}
