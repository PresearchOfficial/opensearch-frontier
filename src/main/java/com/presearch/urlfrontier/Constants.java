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
package com.presearch.urlfrontier;

public interface Constants {

    String OSHostParamName = "opensearch.hostname";
    String OSPortParamName = "opensearch.port";
    String AssignmentClassParamName = "assignment.class";
    String QueueIDFieldName = "queueID";
    String CrawlIDFieldName = "crawlID";
    String OSConcRequParamName = "opensearch.concurrent.requests";
    String OSBulkActionsParamName = "opensearch.bulk.actions";
    String OSFlushIntervalParamName = "opensearch.flush.interval";
    String minsBetweenAssignmentRefreshParamName = "opensearch.time.between.refresh";
    String OSUserParamName = "opensearch.user";
    String OSPasswordParamName = "opensearch.password";
}
