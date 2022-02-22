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

import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import crawlercommons.urlfrontier.service.QueueInterface;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Queue implementation backed by Opensearch. Buffers results from Opensearch and keeps track of the
 * URLs in flight
 */
public class Queue implements QueueInterface {

    private long blockedUntil = -1;

    private int delay = -1;

    private long lastProduced = 0;

    private Map<String, Long> beingProcessed = null;

    private List<URLInfo> buffer;

    public Queue() {}

    public void addToBuffer(URLInfo info) {
        if (buffer == null) buffer = new ArrayList<>();
        buffer.add(info);
    }

    public List<URLInfo> getBuffer() {
        return buffer;
    }

    @Override
    public int getInProcess(long now) {
        synchronized (this) {
            if (beingProcessed == null) return 0;
            // check that the content of beingProcessed is still valid
            beingProcessed
                    .entrySet()
                    .removeIf(
                            e -> {
                                return e.getValue().longValue() <= now;
                            });
            return beingProcessed.size();
        }
    }

    public void holdUntil(String url, long timeinSec) {
        synchronized (this) {
            if (beingProcessed == null) beingProcessed = new LinkedHashMap<>();
            beingProcessed.put(url, timeinSec);
        }
    }

    public boolean isHeld(String url, long now) {
        synchronized (this) {
            if (beingProcessed == null) return false;
            Long timeout = beingProcessed.get(url);
            if (timeout != null) {
                if (timeout.longValue() < now) {
                    // release!
                    beingProcessed.remove(url);
                    return false;
                } else return true;
            }
            return false;
        }
    }

    public void removeFromProcessed(String url) {
        synchronized (this) {
            // should not happen
            if (beingProcessed == null) return;

            // remove from ephemeral cache of URLs in process
            beingProcessed.remove(url);
        }
    }

    @Override
    public int getCountCompleted() {
        // TODO get this value by querying Opensearch
        // for entries without a nextFetchDate
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBlockedUntil(long until) {
        blockedUntil = until;
    }

    @Override
    public long getBlockedUntil() {
        return blockedUntil;
    }

    @Override
    public int getDelay() {
        return delay;
    }

    @Override
    public void setDelay(int delayRequestable) {
        this.delay = delayRequestable;
    }

    @Override
    public long getLastProduced() {
        return lastProduced;
    }

    @Override
    public void setLastProduced(long lastProduced) {
        this.lastProduced = lastProduced;
    }

    @Override
    public int countActive() {
        // buffer size + being processed
        int count = 0;
        if (beingProcessed != null) count += beingProcessed.size();
        if (buffer != null) count += buffer.size();
        return count;
    }
}
