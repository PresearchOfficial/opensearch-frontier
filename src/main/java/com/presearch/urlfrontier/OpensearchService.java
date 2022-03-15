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

import com.presearch.urlfrontier.assignment.AssignmentsListener;
import com.presearch.urlfrontier.assignment.IAssigner;
import com.presearch.urlfrontier.assignment.OpensearchAssigner;
import crawlercommons.urlfrontier.CrawlID;
import crawlercommons.urlfrontier.Urlfrontier.KnownURLItem;
import crawlercommons.urlfrontier.Urlfrontier.Stats;
import crawlercommons.urlfrontier.Urlfrontier.StringList;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo.Builder;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import crawlercommons.urlfrontier.service.AbstractFrontierService;
import crawlercommons.urlfrontier.service.QueueInterface;
import crawlercommons.urlfrontier.service.QueueWithinCrawl;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.MultiSearchResponse.Item;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.LoggerFactory;

/** Frontier implementation using Opensearch as a backend * */
public class OpensearchService extends AbstractFrontierService
        implements Closeable, AssignmentsListener, Runnable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OpensearchService.class);

    private static final String statusIndexName = "status";
    private static final String queuesIndexName = "queues";

    private final ConcurrentHashMap<QueueWithinCrawl, QueueWithinCrawl> queuesBeingDeleted =
            new ConcurrentHashMap<>();

    private final RestHighLevelClient client;

    private final boolean doRouting;

    private boolean isClosed;

    private final int totalNumberAssignments;

    private final int minsBetweenAssignmentRefresh;

    private final AtomicBoolean refreshingMappings = new AtomicBoolean(false);
    private final AtomicBoolean assignmentsChanged = new AtomicBoolean(false);

    private Instant timeLastRefresh;

    private final BulkProcessor.Listener listener =
            new BulkProcessor.Listener() {
                @Override
                public void afterBulk(long arg0, BulkRequest request, BulkResponse response) {}

                @Override
                public void afterBulk(long arg0, BulkRequest request, Throwable arg2) {
                    LOG.error("Exception obtained from Opensearch", arg2);
                }

                @Override
                public void beforeBulk(long arg0, BulkRequest arg1) {}
            };

    private BulkProcessor bulkProcessor;

    private IAssigner assigner;

    // current partitions
    // this in an intermediate level before the queues in AbstractFrontierService
    // they need to be kept in sync and the mapping must be revisited
    protected final Map<String, Set<QueueWithinCrawl>> partitions =
            Collections.synchronizedMap(new LinkedHashMap<>());

    // no explicit config
    public OpensearchService() throws Exception {
        this(new HashMap<String, String>());
    }

    public OpensearchService(final Map<String, String> configuration) throws Exception {

        String host = configuration.getOrDefault(Constants.OSHostParamName, "localhost");
        String port = configuration.getOrDefault(Constants.OSPortParamName, "9200");

        String user = configuration.get(Constants.OSUserParamName);
        String password = configuration.getOrDefault(Constants.OSPasswordParamName, "admin");

        String scheme = configuration.getOrDefault(Constants.OSSchemeParamName, "http");

        totalNumberAssignments =
                Integer.parseInt(
                        configuration.getOrDefault(
                                IAssigner.TOTAL_ASSIGNMENT_COUNT_CONFIG_NAME,
                                Integer.toString(IAssigner.DEFAULT_TOTAL_NUMBER_ASSIGNMENTS)));

        minsBetweenAssignmentRefresh =
                Integer.parseInt(
                        configuration.getOrDefault(
                                Constants.minsBetweenAssignmentRefreshParamName, "10"));

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
        // check that indices exist and that the connection is fine
        try {
            IndexCreation.checkOrCreateIndex(client, statusIndexName, LOG);
            IndexCreation.checkOrCreateIndex(client, queuesIndexName, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        doRouting = false;

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

        // create an assigner so that this Frontier can get whole or parts of the whole
        // crawl set
        // and get notifications of when the assignments have changed
        String assignmentClass =
                configuration.getOrDefault(
                        Constants.AssignmentClassParamName, OpensearchAssigner.class.getName());
        try {
            Class<?> assigclass = Class.forName(assignmentClass);
            boolean interfaceOK = IAssigner.class.isAssignableFrom(assigclass);
            if (!interfaceOK) {
                throw new RuntimeException(
                        "Class " + assignmentClass + " must implement IAssigner");
            }
            assigner = (IAssigner) assigclass.newInstance();
            assigner.setListener(this);
            assigner.init(configuration);
        } catch (Exception e) {
            throw new RuntimeException("Can't instanciate " + assignmentClass);
        }

        // create a timer so that the we periodically
        // check whether the the mappings need refreshing
        // either because the assignments have changed
        // or because it has been a while
        // this allows to add or remove queues for the mappings we already had
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);

        int URLsPerQueueToCache = 10;
        int numRequestsPerBatch = 50;
        int msecBetweenReadRequests = 100;

        new QueuesPopulator(numRequestsPerBatch, URLsPerQueueToCache, msecBetweenReadRequests)
                .start();
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        assigner.close();
        client.close();
    }

    @Override
    protected int sendURLsForQueue(
            QueueInterface queue,
            QueueWithinCrawl key,
            int maxURLsPerQueue,
            int secsUntilRequestable,
            long now,
            StreamObserver<URLInfo> responseObserver) {

        Queue q = (Queue) queue;

        int countSent = 0;

        // only use URLs from the cache
        while (q.bufferSize() > 0 && countSent < maxURLsPerQueue) {
            URLInfo next = q.getBuffer().remove(0);
            q.holdUntil(next.getUrl(), now + secsUntilRequestable);
            responseObserver.onNext(next);
            countSent++;
        }

        return countSent;
    }

    @Override
    public StreamObserver<URLItem> putURLs(
            StreamObserver<crawlercommons.urlfrontier.Urlfrontier.String> responseObserver) {

        AtomicBoolean completed = new AtomicBoolean(false);

        return new StreamObserver<URLItem>() {

            @Override
            public void onNext(URLItem value) {

                Instant nextFetchDate = null;
                boolean discovered = true;
                URLInfo info;

                if (value.hasDiscovered()) {
                    info = value.getDiscovered().getInfo();
                    nextFetchDate = Instant.now();
                } else {
                    KnownURLItem known = value.getKnown();
                    info = known.getInfo();
                    if (known.getRefetchableFromDate() != 0)
                        nextFetchDate = Instant.ofEpochSecond(known.getRefetchableFromDate());
                    discovered = Boolean.FALSE;
                }

                String Qkey = info.getKey();
                String url = info.getUrl();
                String crawlID = CrawlID.normaliseCrawlID(info.getCrawlID());

                // has a queue key been defined? if not use the hostname
                if (Qkey.equals("")) {
                    LOG.debug("key missing for {}", url);
                    Qkey = provideMissingKey(url);
                    if (Qkey == null) {
                        LOG.error("Malformed URL {}", url);
                        responseObserver.onNext(
                                crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                        .setValue(url)
                                        .build());
                        return;
                    }
                }

                // check that the key is not too long
                if (Qkey.length() > 255) {
                    LOG.error("Key too long: {}", Qkey);
                    responseObserver.onNext(
                            crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                    .setValue(url)
                                    .build());
                    return;
                }

                QueueWithinCrawl qk = QueueWithinCrawl.get(Qkey, crawlID);

                // ignore this URL if the queue is being deleted
                if (queuesBeingDeleted.containsKey(qk)) {
                    LOG.info("Not adding {} as its queue {} is being deleted", url, qk);
                    responseObserver.onNext(
                            crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                    .setValue(url)
                                    .build());
                    return;
                }

                // create an entry in the queues index
                // String queueName and crawlID
                // is this a queue we already handle?
                // most likely scenario
                if (!queues.containsKey(qk)) {
                    Map<String, String> mapFields = new HashMap<>();
                    mapFields.put(Constants.QueueIDFieldName, qk.getQueue());
                    mapFields.put(Constants.CrawlIDFieldName, qk.getCrawlid());

                    // compute an assignment hash for the key
                    int assignmentHash = Math.abs(qk.hashCode() % totalNumberAssignments);
                    mapFields.put("assignmentHash", Integer.toString(assignmentHash));

                    // small optimisation - we know it is a host we have an assignment for
                    // no need to wait for the next sync
                    if (partitions.containsKey(Integer.toString(assignmentHash))) {
                        partitions.putIfAbsent(
                                Integer.toString(assignmentHash), new HashSet<QueueWithinCrawl>());
                        Set<QueueWithinCrawl> localqueues =
                                partitions.get(Integer.toString(assignmentHash));
                        if (!localqueues.contains(qk)) {
                            localqueues.add(qk);
                        }
                        // add it to the list of queues
                        queues.putIfAbsent(qk, new Queue());
                    }

                    String sha256hex =
                            org.apache.commons.codec.digest.DigestUtils.sha256Hex(qk.toString());

                    IndexRequest qrequest =
                            new IndexRequest(queuesIndexName)
                                    .source(mapFields)
                                    .create(true)
                                    .id(sha256hex);
                    bulkProcessor.add(qrequest);
                }

                String sha256hex =
                        org.apache.commons.codec.digest.DigestUtils.sha256Hex(
                                qk.toString() + "_" + url);

                // have a cache mechanism to avoid sending duplicate content?

                // send to Opensearch as a bulk
                try {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    builder.field("url", url);
                    builder.field(Constants.QueueIDFieldName, qk.getQueue());
                    builder.field(Constants.CrawlIDFieldName, qk.getCrawlid());

                    builder.startObject("metadata");

                    Iterator<Entry<String, StringList>> entries =
                            info.getMetadataMap().entrySet().iterator();

                    while (entries.hasNext()) {
                        Entry<String, StringList> entry = entries.next();
                        // String mdkey = entry.getKey().replaceAll("\\.", "%2E");
                        String mdkey = entry.getKey();
                        builder.array(mdkey, entry.getValue().getValuesList().toArray());
                    }

                    builder.endObject();

                    if (nextFetchDate != null) {
                        builder.field("nextFetchDate", nextFetchDate);
                    }

                    builder.endObject();

                    // check that we don't overwrite an existing entry
                    // When create is used, the index operation will fail if a document
                    // by that id already exists in the index.

                    IndexRequest request = new IndexRequest(statusIndexName);
                    request.source(builder).id(sha256hex).create(discovered);

                    if (doRouting) {
                        request.routing(Qkey);
                    }

                    LOG.debug("Sending to ES buffer {} with ID {}", url, sha256hex);

                    bulkProcessor.add(request);

                    // ack everything for now - fire and forget
                    responseObserver.onNext(
                            crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                    .setValue(url)
                                    .build());
                } catch (Exception e) {
                    LOG.error("Exception while sending {}", url, e);
                    responseObserver.onNext(
                            crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                    .setValue(url)
                                    .build());
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.error("Throwable caught", t);
            }

            @Override
            public void onCompleted() {
                completed.set(true);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    /**
     * Notification by the Assigner that the assignments have changed; this should return promptly
     * so as to not block the work of the assigner.
     */
    public void setAssignmentsChanged() {

        LOG.debug("Assigmnents changed");

        Set<String> newPartitions = assigner.getPartitionsAssigned();

        // work out what has been lost
        for (String oldPart : partitions.keySet()) {
            if (!newPartitions.contains(oldPart)) {
                // remove it and the corresponding queues
                Set<QueueWithinCrawl> queues4partitions = partitions.remove(oldPart);
                // can be null if we haven't mapped the queues to it yet
                if (queues4partitions != null) {
                    for (QueueWithinCrawl mappedQueues : queues4partitions) {
                        queues.remove(mappedQueues);
                    }
                }
            }
        }
        // add what has been added
        for (String part : newPartitions) {
            // store an empty list for now, it will be populated when the mappings
            // are refreshed
            partitions.putIfAbsent(part, new HashSet<QueueWithinCrawl>());
        }

        assignmentsChanged.set(true);
    }

    @Override
    public void run() {
        // don't run if this instance is closing or closed
        if (isClosed) return;

        // has enough time elapsed since the previous refresh or
        // have we had a recent change of assignments?
        if (assignmentsChanged.get()) {
            assignmentsChanged.set(false);
            refreshMappings("assignments changed");
        } else if (timeLastRefresh != null
                && Instant.now()
                        .isAfter(timeLastRefresh.plusSeconds(minsBetweenAssignmentRefresh * 60))) {
            refreshMappings("refresh is overdue");
        }
    }

    /**
     * Refresh the mappings from partitions to queues. Might take some time compared to scanning the
     * whole set of queues but means there is less latency. As the number of frontiers instances
     * goes up, this will take less and less time. The mappings are refreshed shortly after an
     * assignment change or periodically every N minutes.
     */
    public void refreshMappings(String reason) {
        // do not refresh the mappings
        // if it is already being done by a different thread
        if (refreshingMappings.get()) {
            return;
        }

        if (partitions.isEmpty()) {
            return;
        }

        LOG.info("Refreshing mappings because {}...", reason);

        refreshingMappings.set(true);

        long start = System.currentTimeMillis();

        // iterate on the partitions keys
        // copy the values so that we can check that they haven't been deleted in the
        // meantime
        ArrayList<String> partitionsID = new ArrayList(partitions.keySet());

        int totalQueues = 0;
        int partitionsCount = 0;

        final Set<QueueWithinCrawl> found = new HashSet<>();

        try {

            for (String partitionID : partitionsID) {
                // gone?
                if (!partitions.containsKey(partitionID)) continue;

                LOG.debug("Getting queues for partition {}", partitionID);

                partitionsCount++;

                // existing list
                Set<QueueWithinCrawl> existingList = partitions.get(partitionID);

                // query the queues index to get all the ones having this partition id
                SearchRequest searchRequest = new SearchRequest(this.queuesIndexName);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                // assume there won't be more than 10K
                searchSourceBuilder.size(10000);
                searchSourceBuilder.query(
                        QueryBuilders.boolQuery()
                                .filter(QueryBuilders.termQuery("assignmentHash", partitionID)));
                searchRequest.source(searchSourceBuilder);

                SearchResponse searchResponse =
                        client.search(searchRequest, RequestOptions.DEFAULT);
                SearchHits hits = searchResponse.getHits();
                for (SearchHit h : hits.getHits()) {
                    String queueID = h.getSourceAsMap().get(Constants.QueueIDFieldName).toString();
                    String crawlID = h.getSourceAsMap().get(Constants.CrawlIDFieldName).toString();
                    QueueWithinCrawl qwc = QueueWithinCrawl.get(queueID, crawlID);
                    queues.putIfAbsent(qwc, new Queue());
                    found.add(qwc);
                    totalQueues++;
                }

                // delete the ones that have disappeared since
                for (QueueWithinCrawl existing : existingList) {
                    if (!found.contains(existing)) {
                        queues.remove(existing);
                    }
                }

                // replace values
                partitions.put(partitionID, found);

                LOG.debug("Found {} queues for partition {}", found.size(), partitionID);

                found.clear();
            }

            long timeSpent = System.currentTimeMillis() - start;

            // finished the lot
            LOG.info(
                    "Queues returned {} [now {}] for {} partitions in {} msec",
                    totalQueues,
                    queues.size(),
                    partitionsCount,
                    timeSpent);

        } catch (IOException e) {
            LOG.error("Exception caught when reading mapping from Opensearch", e);
        } finally {
            refreshingMappings.set(false);
            timeLastRefresh = Instant.now();
        }
    }

    @Override
    public void deleteCrawl(
            crawlercommons.urlfrontier.Urlfrontier.String crawlID,
            io.grpc.stub.StreamObserver<crawlercommons.urlfrontier.Urlfrontier.Integer>
                    responseObserver) {

        final String normalisedCrawlID = CrawlID.normaliseCrawlID(crawlID.getValue());

        int countDeleted = 0;

        // delete the URLs in Opensearch then the queues
        DeleteByQueryRequest dbqrequest = new DeleteByQueryRequest(this.statusIndexName);
        dbqrequest.setQuery(QueryBuilders.termQuery(Constants.CrawlIDFieldName, normalisedCrawlID));

        try {
            BulkByScrollResponse deletion =
                    client.deleteByQuery(dbqrequest, RequestOptions.DEFAULT);
            countDeleted = (int) deletion.getDeleted();

            dbqrequest.indices(this.queuesIndexName);
            deletion = client.deleteByQuery(dbqrequest, RequestOptions.DEFAULT);

            // remove them from the partitions
            partitions.forEach(
                    (k, v) -> {
                        // remove all the values with the crawlID
                        v.removeIf(q -> q.getCrawlid().equals(normalisedCrawlID));
                    });

            // then from the queues
            synchronized (queues) {
                queues.entrySet().removeIf(q -> q.getKey().getCrawlid().equals(normalisedCrawlID));
            }

        } catch (IOException e) {
            LOG.error("Exception caught when deleting crawl {}", normalisedCrawlID, e);
        }

        responseObserver.onNext(
                crawlercommons.urlfrontier.Urlfrontier.Integer.newBuilder()
                        .setValue(countDeleted)
                        .build());
        responseObserver.onCompleted();
    }

    /**
     *
     *
     * <pre>
     * * Delete a queue based on the key in parameter *
     * </pre>
     */
    @Override
    public void deleteQueue(
            crawlercommons.urlfrontier.Urlfrontier.QueueWithinCrawlParams request,
            StreamObserver<crawlercommons.urlfrontier.Urlfrontier.Integer> responseObserver) {
        QueueWithinCrawl qwc = QueueWithinCrawl.get(request.getKey(), request.getCrawlID());
        int countDeleted = 0;

        queuesBeingDeleted.put(qwc, qwc);

        // delete the queue in the Opensearch indices
        DeleteByQueryRequest dbqrequest =
                new DeleteByQueryRequest(this.statusIndexName, this.queuesIndexName);
        BoolQueryBuilder bq = QueryBuilders.boolQuery();
        bq.must(QueryBuilders.termQuery(Constants.QueueIDFieldName, qwc.getQueue()));
        bq.must(QueryBuilders.termQuery(Constants.CrawlIDFieldName, qwc.getCrawlid()));
        dbqrequest.setQuery(bq);

        try {
            BulkByScrollResponse deletion =
                    client.deleteByQuery(dbqrequest, RequestOptions.DEFAULT);
            // remove it from the partitions
            partitions.forEach(
                    (k, v) -> {
                        v.remove(qwc);
                    });
            queues.remove(qwc);
            countDeleted = (int) deletion.getDeleted() - 1;
        } catch (IOException e) {
            LOG.error(
                    "Exception caught when deleting queue {} in crawl {}",
                    qwc.getQueue(),
                    qwc.getCrawlid(),
                    e);
        }

        responseObserver.onNext(
                crawlercommons.urlfrontier.Urlfrontier.Integer.newBuilder()
                        .setValue(countDeleted)
                        .build());
        responseObserver.onCompleted();

        queuesBeingDeleted.remove(qwc);
    }

    @Override
    /**
     * The queues objects do not have all the information, we need to get it from the Opensearch
     * indices. Returns the global stats apart from inprocess which is for the queues managed by
     * this instance.
     */
    public void getStats(
            crawlercommons.urlfrontier.Urlfrontier.QueueWithinCrawlParams request,
            StreamObserver<Stats> responseObserver) {
        LOG.info("Received stats request");

        final Map<String, Long> s = new HashMap<>();

        int inProc = 0;
        long numQueues = 0;
        long size = 0;
        long completed = 0;
        long numLocalQueues = 0;

        Collection<QueueInterface> _queues = queues.values();

        String filteredQueue = null;

        // specific queue?
        if (!request.getKey().isEmpty()) {
            _queues = new LinkedList<>();
            filteredQueue = request.getKey();

            QueueInterface q = queues.get(filteredQueue);
            if (q != null) {
                _queues.add(q);
            } else {
                // TODO notify an error to the client ?
                LOG.info("Can't get stats for queue {}", filteredQueue);
            }
        }

        long now = Instant.now().getEpochSecond();

        // backed by the queues so can result in a
        // ConcurrentModificationException
        synchronized (queues) {
            for (QueueInterface q : _queues) {
                inProc += q.getInProcess(now);
                numLocalQueues++;
            }
        }

        // size is the total number of URLs in the index
        CountRequest countRequest = new CountRequest(this.statusIndexName);
        try {
            // was this for a specific queue?
            if (filteredQueue != null) {
                countRequest.query(
                        QueryBuilders.boolQuery()
                                .filter(
                                        QueryBuilders.termQuery(
                                                Constants.QueueIDFieldName, filteredQueue)));
            }
            CountResponse countResult = client.count(countRequest, RequestOptions.DEFAULT);
            size = countResult.getCount();

            // now get the ones that are completed i.e. they are not scheduled
            countRequest.query(
                    QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("nextFetchDate")));
            countResult = client.count(countRequest, RequestOptions.DEFAULT);
            completed = countResult.getCount();

            // finally the total number of queues
            countRequest = new CountRequest(this.queuesIndexName);
            countResult = client.count(countRequest, RequestOptions.DEFAULT);
            numQueues = countResult.getCount();

        } catch (IOException e) {
            LOG.info("Error when getting counts for {}", statusIndexName, e);
        }

        // put count completed as custom stats for now
        // add it as a proper field later?
        s.put("completed", completed);

        s.put("numLocalQueues", numLocalQueues);

        s.put("numLocalAssignments", new Long(partitions.size()));

        Stats stats =
                Stats.newBuilder()
                        .setNumberOfQueues(numQueues)
                        .setSize(size)
                        .setInProcess(inProc)
                        .putAllCounts(s)
                        .build();
        responseObserver.onNext(stats);
        responseObserver.onCompleted();
    }

    /**
     * Iterates on the queues and populates them with data from Opensearch, independently of
     * requests coming in.
     */
    class QueuesPopulator extends Thread implements ActionListener<MultiSearchResponse> {

        /** TODO Replace with a cache with TTL in case errors prevent things from being removed * */
        private final Set<String> queuesBeingPopulated = new HashSet<>();

        private Instant lastQuery = Instant.now();

        private final int minDelayBetweenLoads;
        private final int urlsToPutInCache;
        private final int numrequests;

        QueuesPopulator(int numrequests, int urlsToPutInCache, int minDelayBetweenLoads) {
            this.numrequests = numrequests;
            this.urlsToPutInCache = urlsToPutInCache;
            this.minDelayBetweenLoads = minDelayBetweenLoads;
        }

        @Override
        public void run() {
            while (!isClosed) {
                // implement delay between requests
                long msecTowait =
                        minDelayBetweenLoads
                                - (Instant.now().toEpochMilli() - lastQuery.toEpochMilli());
                if (msecTowait > 0) {
                    try {
                        Thread.sleep(msecTowait);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                synchronized (queues) {

                    // do as async multisearch
                    MultiSearchRequest msr = new MultiSearchRequest();

                    Iterator<Entry<QueueWithinCrawl, QueueInterface>> iterator =
                            queues.entrySet().iterator();

                    lastQuery = Instant.now();

                    while (iterator.hasNext() && msr.requests().size() < numrequests) {
                        Entry<QueueWithinCrawl, QueueInterface> e = iterator.next();

                        // check that it isn't blocked
                        if (e.getValue().getBlockedUntil() >= lastQuery.getEpochSecond()) {
                            continue;
                        }

                        if (queuesBeingPopulated.contains(e.getKey().toString())) {
                            continue;
                        }

                        SearchRequest searchRequest = new SearchRequest(statusIndexName);
                        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                        org.opensearch.index.query.BoolQueryBuilder queryBuilder =
                                org.opensearch.index.query.QueryBuilders.boolQuery();
                        // query on nextFetchDate and also on the queue key and crawlid
                        queryBuilder.filter(
                                org.opensearch.index.query.QueryBuilders.termQuery(
                                        Constants.QueueIDFieldName, e.getKey().getQueue()));
                        queryBuilder.filter(
                                org.opensearch.index.query.QueryBuilders.termQuery(
                                        Constants.CrawlIDFieldName, e.getKey().getCrawlid()));
                        queryBuilder.filter(
                                org.opensearch.index.query.QueryBuilders.rangeQuery("nextFetchDate")
                                        .lte(lastQuery));

                        searchSourceBuilder.query(queryBuilder);
                        // ask for twice as many so that we can cache the rest
                        searchSourceBuilder.size(urlsToPutInCache);
                        searchSourceBuilder.explain(false);
                        searchSourceBuilder.trackTotalHits(false);

                        // sort by ascending nextFetchDate i.e. older documents first
                        searchSourceBuilder.sort("nextFetchDate", SortOrder.ASC);

                        searchRequest.source(searchSourceBuilder);
                        msr.add(searchRequest);

                        queuesBeingPopulated.add(e.getKey().toString());
                    }

                    if (!msr.requests().isEmpty()) {
                        client.msearchAsync(msr, RequestOptions.DEFAULT, this);
                    }
                }
            }
        }

        @Override
        public void onResponse(MultiSearchResponse response) {
            LOG.debug("Multisearch took {}", response.getTook());

            for (Item item : response) {
                if (item.isFailure()) {
                    // TODO deal with the failure
                    continue;
                }
                for (SearchHit hits : item.getResponse().getHits()) {
                    Map<String, Object> fields = hits.getSourceAsMap();
                    // we need to convert from the fields to a URLInfo object
                    Builder urlInfoBuilder = URLInfo.newBuilder();
                    urlInfoBuilder.setUrl(fields.get("url").toString());
                    urlInfoBuilder.setCrawlID(fields.get(Constants.CrawlIDFieldName).toString());
                    urlInfoBuilder.setKey(fields.get(Constants.QueueIDFieldName).toString());

                    // add the metadata if they exist
                    Map<String, List<String>> mdAsMap =
                            (Map<String, List<String>>) fields.get("metadata");
                    if (mdAsMap != null) {
                        Iterator<Entry<String, List<String>>> mdIter =
                                mdAsMap.entrySet().iterator();
                        while (mdIter.hasNext()) {
                            Entry<String, List<String>> mdEntry = mdIter.next();
                            String mdkey = mdEntry.getKey();
                            Object mdValObj = mdEntry.getValue();

                            crawlercommons.urlfrontier.Urlfrontier.StringList.Builder slbuilder =
                                    StringList.newBuilder();

                            // single value
                            if (mdValObj instanceof String) {
                                slbuilder.addValues((String) mdValObj);
                            }
                            // multi valued
                            else {
                                slbuilder.addAllValues((List<String>) mdValObj);
                            }
                            urlInfoBuilder.putMetadata(mdkey, slbuilder.build());
                        }
                    }

                    QueueWithinCrawl qwc =
                            QueueWithinCrawl.get(
                                    urlInfoBuilder.getKey(), urlInfoBuilder.getCrawlID());

                    queuesBeingPopulated.remove(qwc.toString());

                    Queue queue = (Queue) queues.get(qwc);
                    queue.addToBuffer(urlInfoBuilder.build());
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            LOG.error("Exception received when querying Opensearch", e);
        }
    }
}
