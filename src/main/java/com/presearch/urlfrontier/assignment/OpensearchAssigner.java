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
import com.presearch.urlfrontier.IndexCreation;
import com.presearch.urlfrontier.OpensearchService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.joda.time.Instant;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.slf4j.LoggerFactory;

/** Uses Opensearch indices to assign crawl partitions to Frontier instances * */
public class OpensearchAssigner implements IAssigner, Runnable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OpensearchService.class);
    private static final String assignmentsIndexName = "assignments";
    private static final String frontiersIndexName = "frontiers";

    private int totalNumberOfAssignments;

    private RestHighLevelClient client;

    // get a notification of a change of assignments
    private AssignmentsListener listener;

    // unique ID for this frontier
    private String uuid = UUID.randomUUID().toString();

    // TTL for assignments
    private int ttl = DEFAULT_TTL_SEC;

    private BulkProcessor bulkProcessor;

    private boolean closed;

    @Override
    public void init(Map<String, String> configuration) {
        String host = configuration.getOrDefault(Constants.OSHostParamName, "localhost");
        String port = configuration.getOrDefault(Constants.OSPortParamName, "9200");

        String user = configuration.get(Constants.OSUserParamName);
        String password = configuration.getOrDefault(Constants.OSPasswordParamName, "admin");

        String scheme = configuration.getOrDefault(Constants.OSSchemeParamName, "http");

        int hb =
                Integer.parseInt(
                        configuration.getOrDefault(
                                HEARBEAT_CONFIG_NAME, Integer.toString(DEFAULT_HEARTBEAT_SEC)));

        ttl =
                Integer.parseInt(
                        configuration.getOrDefault(TTL_CONFIG_NAME, Integer.toString(hb * 2)));

        totalNumberOfAssignments =
                Integer.parseInt(
                        configuration.getOrDefault(
                                TOTAL_ASSIGNMENT_COUNT_CONFIG_NAME,
                                Integer.toString(DEFAULT_TOTAL_NUMBER_ASSIGNMENTS)));

        // can set uuid via config
        uuid = configuration.getOrDefault(UUID_CONFIG_NAME, uuid);

        // Create a client.
        RestClientBuilder builder =
                RestClient.builder(new HttpHost(host, Integer.parseInt(port), scheme));

        if (user != null) {
            // Establish credentials to use basic authentication.
            // Only for demo purposes. Don't specify your credentials in code.
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(user, password));

            builder.setHttpClientConfigCallback(
                    new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(
                                HttpAsyncClientBuilder httpClientBuilder) {
                            return httpClientBuilder.setDefaultCredentialsProvider(
                                    credentialsProvider);
                        }
                    });
        }

        // set to true?
        builder.setCompressionEnabled(false);

        client = new RestHighLevelClient(builder);

        // check that indices exist and that the connection is fine
        try {
            IndexCreation.checkOrCreateIndex(client, assignmentsIndexName, LOG);
            IndexCreation.checkOrCreateIndex(client, frontiersIndexName, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // renew its leases - might contain the ones it previously had, with some
        // added or removed
        final BulkProcessor.Listener listener =
                new BulkProcessor.Listener() {
                    @Override
                    public void afterBulk(long arg0, BulkRequest request, BulkResponse response) {
                        LOG.debug(
                                "{} Ack docs indexed into Opensearch in {}",
                                uuid,
                                response.getTook());
                    }

                    @Override
                    public void afterBulk(long arg0, BulkRequest request, Throwable arg2) {
                        LOG.error("{} Exception obtained from Opensearch", uuid, arg2);
                    }

                    @Override
                    public void beforeBulk(long arg0, BulkRequest arg1) {}
                };

        int concurrentRequests =
                Integer.parseInt(configuration.getOrDefault(Constants.OSConcRequParamName, "2"));

        int bulkActions =
                Integer.parseInt(
                        configuration.getOrDefault(Constants.OSBulkActionsParamName, "500"));

        String flushIntervalString =
                configuration.getOrDefault(Constants.OSFlushIntervalParamName, "1s");

        TimeValue flushInterval =
                TimeValue.parseTimeValue(
                        flushIntervalString, TimeValue.timeValueSeconds(5), "flushInterval");

        bulkProcessor =
                BulkProcessor.builder(
                                (request, bulkListener) ->
                                        client.bulkAsync(
                                                request, RequestOptions.DEFAULT, bulkListener),
                                listener)
                        .setFlushInterval(flushInterval)
                        .setBulkActions(bulkActions)
                        .setConcurrentRequests(concurrentRequests)
                        .build();

        // create a timer so that the heartbeat and other tasks are done as
        // expected
        // value could be overridden in the config
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this, 0, hb, TimeUnit.SECONDS);

        LOG.info("Started frontier {} with heartbeat every {} sec and TTL {} sec", uuid, hb, ttl);
    }

    @Override
    public void setListener(AssignmentsListener listener) {
        this.listener = listener;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        client.close();
    }

    @Override
    public Set<String> getPartitionsAssigned() {
        SearchRequest searchRequest = new SearchRequest(assignmentsIndexName);

        Instant start = Instant.now();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        org.opensearch.index.query.BoolQueryBuilder queryBuilder =
                org.opensearch.index.query.QueryBuilders.boolQuery();
        queryBuilder.filter(org.opensearch.index.query.QueryBuilders.termQuery("frontierID", uuid));

        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(totalNumberOfAssignments);
        searchSourceBuilder.explain(false);
        searchSourceBuilder.trackTotalHits(false);
        searchSourceBuilder.trackScores(false);

        searchRequest.source(searchSourceBuilder);

        HashSet<String> partitions = new HashSet<>();

        try {
            // get the results
            SearchResponse results = client.search(searchRequest, RequestOptions.DEFAULT);
            for (SearchHit hit : results.getHits()) {
                partitions.add(hit.getSourceAsMap().get("assignmentHash").toString());
            }
        } catch (Exception e) {
            LOG.error("Exception caught when retrieving partitions for {}", uuid, e);
        }

        Instant end = Instant.now();

        LOG.info(
                "Frontier {} got the list of its {} partitions in {} msec",
                uuid,
                partitions.size(),
                end.getMillis() - start.getMillis());

        return partitions;
    }

    /**
     * Check every single set of assignments and if anything is missing or has timeouts, add them to
     * the set of available assignments.
     */
    @Override
    // called for every heartbeat
    public void run() {

        LOG.debug("heartbeat - frontier {}", uuid);

        if (closed) return;

        long timestamp = Instant.now().getMillis();

        final Map<String, Object> mapFields1 = new HashMap<>();
        mapFields1.put("frontierID", uuid);
        mapFields1.put("lastSeen", timestamp);
        IndexRequest qrequest1 =
                new IndexRequest(OpensearchAssigner.frontiersIndexName).source(mapFields1).id(uuid);
        try {
            client.index(qrequest1, RequestOptions.DEFAULT);
        } catch (IOException e1) {
            LOG.error("Exception caught when registering frontier", e1);
        }

        LOG.debug("Cleanup - frontier {} ready to cleanup assignments", uuid);

        final boolean[] found_array = new boolean[totalNumberOfAssignments];
        int active = 0;

        SearchRequest searchRequest = new SearchRequest(assignmentsIndexName);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(org.opensearch.index.query.QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(totalNumberOfAssignments);
        searchSourceBuilder.explain(false);
        searchSourceBuilder.trackTotalHits(false);
        searchSourceBuilder.trackScores(false);

        searchRequest.source(searchSourceBuilder);

        long timeToDie = timestamp - (ttl * 1000);

        List<Integer> partitionsAssigned = new ArrayList<>();

        final Set<String> frontierIDs = new HashSet<>();

        // the current assigner might not have had the time to check in
        if (!frontierIDs.contains(uuid)) {
            frontierIDs.add(uuid);
        }

        try {
            SearchResponse results = client.search(searchRequest, RequestOptions.DEFAULT);
            for (SearchHit hit : results.getHits()) {
                Map<String, Object> fields = hit.getSourceAsMap();
                long lastSeen = (long) fields.get("lastSeen");
                // too old ?
                if (lastSeen < timeToDie) {
                    continue;
                }
                String frontierID = fields.get("frontierID").toString();
                int assignmentHash = (int) fields.get("assignmentHash");
                if (frontierID.equals(uuid)) {
                    partitionsAssigned.add(assignmentHash);
                }
                found_array[assignmentHash] = true;
                active++;
            }

            // now search on the frontiers
            searchRequest.indices(OpensearchAssigner.frontiersIndexName);
            searchRequest.source().seqNoAndPrimaryTerm(Boolean.TRUE);
            results = client.search(searchRequest, RequestOptions.DEFAULT);

            for (SearchHit hit : results.getHits()) {
                Map<String, Object> fields = hit.getSourceAsMap();
                long lastSeen = (long) fields.get("lastSeen");
                // too old ?
                if (lastSeen < timeToDie) {
                    DeleteRequest drequest =
                            new DeleteRequest(OpensearchAssigner.frontiersIndexName, hit.getId());
                    drequest.setIfPrimaryTerm(hit.getPrimaryTerm());
                    drequest.setIfSeqNo(hit.getSeqNo());
                    bulkProcessor.add(drequest);
                    continue;
                }
                String frontierID = fields.get("frontierID").toString();
                if (!frontierIDs.contains(frontierID)) {
                    frontierIDs.add(frontierID);
                }
                continue;
            }
        } catch (Exception e) {
            LOG.error("Exception caught when scanning partitions", e);
        }

        LOG.info(
                "Cleanup - frontier {} found {} active assignment(s) and {} active frontier(s)",
                uuid,
                active,
                frontierIDs.size());

        final int inactive = totalNumberOfAssignments - active;

        // work out the number of assignments needed
        int assignmentsNeeded = totalNumberOfAssignments / frontierIDs.size();
        int remainder = totalNumberOfAssignments % frontierIDs.size();

        // work out if the current frontier needs to have an extra slot
        // sort by alphabetical order
        if (remainder != 0) {
            List<String> lfrontierIDs = new ArrayList<>(frontierIDs);
            Collections.sort(lfrontierIDs);
            // if its rank is below the remainder then add one
            if (lfrontierIDs.indexOf(uuid) < remainder) {
                assignmentsNeeded++;
            }
        }

        int difference = partitionsAssigned.size() - assignmentsNeeded;
        if (difference == 0) {
            LOG.info("Frontier {} has the {} assignments it needs", uuid, assignmentsNeeded);
        } else if (difference < 0) {
            LOG.info(
                    "Frontier {} needs {} additional assignments to have the {} it needs",
                    uuid,
                    Math.abs(difference),
                    assignmentsNeeded);
            // try to claim additional assignments
            if (inactive > 0) {
                int added = 0;
                for (int i = 0; i < found_array.length && added < Math.abs(difference); i++) {
                    if (!found_array[i]) {
                        partitionsAssigned.add(i);
                        added++;
                    }
                }
            }
        } else {
            // too many, need to offload a few
            // remove a random selection from its partitions
            Collections.shuffle(partitionsAssigned);
            partitionsAssigned = partitionsAssigned.subList(difference, partitionsAssigned.size());
            // help a bit by explicitly deleting the assignments
            LOG.info("Frontier {} about to delete {} assignments", uuid, difference);
            for (Integer todelete : partitionsAssigned.subList(0, difference)) {
                DeleteRequest drequest =
                        new DeleteRequest(
                                OpensearchAssigner.assignmentsIndexName, todelete.toString());
                bulkProcessor.add(drequest);
            }
        }

        LOG.info("Frontier {} about to write {} assignments", uuid, partitionsAssigned.size());

        final Map<String, Object> mapFields = new HashMap<>();
        mapFields.put("frontierID", uuid);
        mapFields.put("lastSeen", timestamp);

        for (Integer partition : partitionsAssigned) {
            mapFields.put("assignmentHash", partition);
            IndexRequest qrequest =
                    new IndexRequest(OpensearchAssigner.assignmentsIndexName)
                            .source(mapFields)
                            .id(partition.toString());
            bulkProcessor.add(qrequest);
        }

        // tell the frontier that its assignments might have changed
        if (difference != 0) {
            // sleep a bit first to give Opensearch time to write the changes
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // set the flag back to <code>true
            }
            listener.setAssignmentsChanged();
        }
    }
}
