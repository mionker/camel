/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.sjms.consumer;

import java.util.ArrayList;
import java.util.List;

import jakarta.jms.Message;
import jakarta.jms.Session;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.sjms.SjmsConstants;
import org.apache.camel.component.sjms.SjmsConsumer;
import org.apache.camel.component.sjms.SjmsEndpoint;
import org.apache.camel.component.sjms.SjmsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes a completed batch of JMS messages as a single InOnly Exchange whose body is a List<Exchange>, one per message,
 * then commits/acknowledges (or rolls back/recovers) the shared session once for the whole batch. A single shared
 * instance across all of a container's BatchConsumerWorkers, same as EndpointMessageListener is shared across all
 * SimpleMessageListeners.
 * <p/>
 * Deliberately InOnly-only: no reply-to/InOut support (see design discussion) — there is no well-defined single "reply"
 * to a batch of N unrelated messages.
 */
public class BatchEndpointMessageListener {

    public static final String BATCH_SIZE_HEADER = "CamelSjmsBatchSize";

    private static final Logger LOG = LoggerFactory.getLogger(BatchEndpointMessageListener.class);

    private final SjmsConsumer consumer;
    private final SjmsEndpoint endpoint;
    private final Processor processor;

    public BatchEndpointMessageListener(SjmsConsumer consumer, SjmsEndpoint endpoint, Processor processor) {
        this.consumer = consumer;
        this.endpoint = endpoint;
        this.processor = processor;
    }

    void onBatch(List<Message> rawMessages, Session session) throws Exception {
        List<Exchange> exchanges = new ArrayList<>(rawMessages.size());
        for (Message m : rawMessages) {
            Exchange exchange = endpoint.createExchange(m, null);
            // Populate the headers and body of the Exchange in message
            exchange.getIn().getHeaders();
            exchange.getIn().getBody();
            exchanges.add(exchange);
        }

        Exchange batchExchange = consumer.createExchange(false);
        batchExchange.getIn().setBody(exchanges);
        // Like the standard consumer, store session on exchange as we may need it for transactions support
        batchExchange.setProperty(SjmsConstants.JMS_SESSION, session);
        batchExchange.getIn().setHeader(BATCH_SIZE_HEADER, exchanges.size());

        try {
            processor.process(batchExchange);
        } catch (Exception e) {
            batchExchange.setException(e);
        }

        Message lastMessage = rawMessages.get(rawMessages.size() - 1);
        try {
            if (!batchExchange.isFailed() && !batchExchange.isRollbackOnly()) {
                SjmsHelper.commitIfNeeded(session, lastMessage);
            } else {
                if (batchExchange.getException() != null) {
                    LOG.warn("Batch of {} message(s) failed processing on {}: {}",
                            rawMessages.size(), endpoint.getEndpointUri(),
                            batchExchange.getException().getMessage(), batchExchange.getException());
                }
                SjmsHelper.rollbackIfNeeded(session);
            }
        } finally {
            consumer.releaseExchange(batchExchange, false);
        }
    }
}
