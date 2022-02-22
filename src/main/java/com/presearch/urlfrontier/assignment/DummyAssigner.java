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
