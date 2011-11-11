/**
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
package net.lr.simplecluster.example;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.lr.simplecluster.FailoverHandler;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.util.ServiceHelper;

public class FailoverRoutePolicy implements FailoverHandler, RoutePolicy {

    String routeIds;
    Set<Consumer> consumers = new HashSet<Consumer>();

    public FailoverRoutePolicy() {
    }

    @Override
    public void start() {
        try {
            ServiceHelper.startServices(consumers);
            ServiceHelper.resumeServices(consumers);
        } catch (Exception e) {
        }
    }

    @Override
    public void stop() {
        try {
            ServiceHelper.suspendServices(consumers);
        } catch (Exception e) {
        }
    }

	@Override
	public void onInit(Route route) {
		consumers.add(route.getConsumer());
	}

	@Override
	public void onExchangeBegin(Route route, Exchange exchange) {
		// Do not interfere with exchanges
	}

	@Override
	public void onExchangeDone(Route route, Exchange exchange) {
		// Do not interfere with exchanges
	}

}
