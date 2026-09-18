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
package org.apache.camel.component.sjms.batch;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.sjms.support.JmsTestSupport;
import org.junit.jupiter.api.Test;

import static org.apache.camel.component.sjms.batch.BatchTestSupport.assertBatchSizesInOrder;

public class BatchConsumerQueueTest extends JmsTestSupport {

    private static final String SJMS_QUEUE_NAME
            = "sjms:queue:batch.consumer.queue.BatchConsumerQueueTest?batching=true&batchSize=5";
    private static final String MOCK_RESULT = "mock:result";

    @Test
    public void testOneCompleteBatch() throws Exception {
        MockEndpoint mock = getMockEndpoint(MOCK_RESULT);
        mock.expectedMessageCount(1);

        BatchTestSupport.sendMessages(template, SJMS_QUEUE_NAME, 5);

        mock.assertIsSatisfied();
        assertBatchSizesInOrder(mock, 5);
    }

    @Test
    public void testTwoCompleteBatches() throws Exception {
        MockEndpoint mock = getMockEndpoint(MOCK_RESULT);
        mock.expectedMessageCount(2);

        BatchTestSupport.sendMessages(template, SJMS_QUEUE_NAME, 10);

        mock.assertIsSatisfied();
        assertBatchSizesInOrder(mock, 5, 5);
    }

    @Test
    public void testOneIncompleteBatch() throws Exception {
        MockEndpoint mock = getMockEndpoint(MOCK_RESULT);
        mock.expectedMessageCount(1);

        BatchTestSupport.sendMessages(template, SJMS_QUEUE_NAME, 2);

        mock.assertIsSatisfied();
        assertBatchSizesInOrder(mock, 2);
    }

    @Test
    public void testOneCompleteBatchAndOneIncompleteBatch() throws Exception {
        MockEndpoint mock = getMockEndpoint(MOCK_RESULT);
        mock.expectedMessageCount(2);

        BatchTestSupport.sendMessages(template, SJMS_QUEUE_NAME, 7);

        mock.assertIsSatisfied();
        assertBatchSizesInOrder(mock, 5, 2);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {
                from(SJMS_QUEUE_NAME)
                        .to(MOCK_RESULT);
            }
        };
    }

}
