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

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.sjms.support.JmsTestSupport;
import org.junit.jupiter.api.Test;

public class BatchJmsSelectorTest extends JmsTestSupport {

    @Test
    public void testJmsSelector() throws Exception {
        MockEndpoint resultY = getMockEndpoint("mock:resultY");
        MockEndpoint resultX = getMockEndpoint("mock:resultX");

        resultY.expectedMessageCount(1); // one batch
        resultY.expectedMessagesMatches(exchange -> {
            List<Exchange> batch = exchange.getIn().getBody(List.class);
            return batch.size() == 2
                    && batch.stream().allMatch(e -> "y".equals(e.getIn().getHeader("cheese", String.class)));
        });

        resultX.expectedMessageCount(1);
        resultX.expectedMessagesMatches(exchange -> {
            List<Exchange> batch = exchange.getIn().getBody(List.class);
            return batch.size() == 3
                    && batch.stream().allMatch(e -> "x".equals(e.getIn().getHeader("cheese", String.class)));
        });

        template.sendBodyAndHeader("sjms:test.a.BatchJmsSelectorTest", "Hello there!", "cheese", "y");
        template.sendBodyAndHeader("sjms:test.a.BatchJmsSelectorTest", "Some x!", "cheese", "x");
        template.sendBodyAndHeader("sjms:test.a.BatchJmsSelectorTest", "Even more x!", "cheese", "x");
        template.sendBodyAndHeader("sjms:test.a.BatchJmsSelectorTest", "Goodbye!", "cheese", "y");
        template.sendBodyAndHeader("sjms:test.a.BatchJmsSelectorTest", "Another x!", "cheese", "x");

        resultY.assertIsSatisfied();
        resultX.assertIsSatisfied();
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {
                from("sjms:test.a.BatchJmsSelectorTest").to("log:test-before?showAll=true")
                        .to("sjms:test.b.BatchJmsSelectorTest");

                from("sjms:test.b.BatchJmsSelectorTest?batching=true&messageSelector=cheese='y'")
                        .to("log:test-after-y?showAll=true")
                        .to("mock:resultY");

                from("sjms:test.b.BatchJmsSelectorTest?batching=true&messageSelector=cheese='x'")
                        .to("log:test-after-x?showAll=true")
                        .to("mock:resultX");
            }
        };
    }
}
