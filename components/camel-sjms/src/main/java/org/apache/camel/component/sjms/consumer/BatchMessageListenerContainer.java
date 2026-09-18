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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.apache.camel.component.sjms.SjmsEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SimpleMessageListenerContainer} variant that pulls messages via {@code MessageConsumer.receive(timeout)} on
 * a dedicated worker thread per session, accumulating them into batches (by size and/or timeout), instead of
 * registering a push {@code jakarta.jms.MessageListener}. This gives uniform, receive()-time acknowledgement semantics
 * for every message in a batch under AUTO_ACKNOWLEDGE, matching SjmsPollingConsumer, rather than the inconsistent
 * per-message-callback-time semantics a push listener would give.
 * <p/>
 * Connection creation, exception handling, reconnection and concurrentConsumers fan-out are inherited unchanged from
 * {@link SimpleMessageListenerContainer}; only how each session's messages are pulled and dispatched differs.
 */
public class BatchMessageListenerContainer extends SimpleMessageListenerContainer {

    private static final Logger LOG = LoggerFactory.getLogger(BatchMessageListenerContainer.class);

    private final SjmsEndpoint endpoint;
    private BatchEndpointMessageListener batchListener;
    private ExecutorService workerExecutor;
    private final List<BatchConsumerWorker> workers = new CopyOnWriteArrayList<>();

    public BatchMessageListenerContainer(SjmsEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    public void setBatchListener(BatchEndpointMessageListener batchListener) {
        this.batchListener = batchListener;
    }

    @Override
    protected void doStart() throws Exception {
        workerExecutor = endpoint.getCamelContext().getExecutorServiceManager().newFixedThreadPool(
                this, "SjmsBatchConsumer[" + endpoint.getDestinationName() + "]",
                Math.max(1, this.getConcurrentConsumers()));

        // triggers connection + session/consumer creation, calling configureConsumer() below
        // for each session per concurrentConsumers, and re-invokes it again on reconnection
        super.doStart();
    }

    @Override
    protected void configureConsumer(MessageConsumer consumer, Session session) {
        BatchConsumerWorker worker = new BatchConsumerWorker(
                endpoint, batchListener, consumer, session);
        workers.add(worker);
        CompletableFuture.runAsync(worker, workerExecutor)
                .whenComplete((v, ex) -> onWorkerExit(worker, ex));
    }

    @Override
    protected void doStop() throws Exception {
        // signal graceful stop: each worker finishes its current receive(), drains and routes
        // any partial batch, then exits its loop
        for (BatchConsumerWorker worker : workers) {
            worker.shutdown();
        }
        if (workerExecutor != null) {
            workerExecutor.shutdown();
            if (!workerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Batch consumer workers for {} did not stop within 30s; forcing shutdown",
                        endpoint.getEndpointUri());
                workerExecutor.shutdownNow();
            }
        }
        workers.clear();

        // now safe to close consumers/sessions/connection
        super.doStop();
    }

    private void onWorkerExit(BatchConsumerWorker worker, Throwable ex) {
        if (ex == null || worker.isShutdownRequested()) {
            return;
        }
        stopConsumers();
        scheduleConnectionRecovery();
    }

    @Override
    public void onException(JMSException exception) {
        for (BatchConsumerWorker worker : workers) {
            worker.shutdown();
        }
        workers.clear();
        super.onException(exception);
    }
}
