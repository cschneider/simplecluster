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

import java.util.ArrayList;
import java.util.List;

import net.lr.simplecluster.FailoverHandler;

import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Route;
import org.apache.camel.SuspendableService;
import org.apache.camel.util.ServiceHelper;
import org.springframework.beans.factory.InitializingBean;

public class RouteFailoverHandler implements FailoverHandler {

    CamelContext context;
    String routeIds;


    public RouteFailoverHandler(CamelContext context) {
        this.context = context;
    }

    public void setRouteIds(String routeIds) {
        this.routeIds = routeIds;
    }

    public List<Consumer> getConsumers() {
        List<Consumer> endpoints = new ArrayList<Consumer>();
        String[] endpointIdAr = routeIds.split(",");
        for (String id : endpointIdAr) {
            Route route = context.getRoute(id);
            if (route == null) {
                throw new RuntimeException("Route with id " + id + " not found");
            }
            Consumer consumer = route.getConsumer();
            endpoints.add(consumer);
        }
        return endpoints;
    }

    @Override
    public void start() {
        List<Consumer> consumers = getConsumers();
        try {
            ServiceHelper.startServices(consumers);
            ServiceHelper.resumeServices(consumers);
        } catch (Exception e) {
        }
    }

    @Override
    public void stop() {
        List<Consumer> consumers = getConsumers();
        try {
            ServiceHelper.suspendServices(consumers);
        } catch (Exception e) {
        }
    }

}
