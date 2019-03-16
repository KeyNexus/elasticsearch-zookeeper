/*
 * Copyright 2011 Sonian Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sonian.elasticsearch.action.zookeeper;

import com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperDiscovery;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
//import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
//import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
//import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

//import static org.elasticsearch.common.collect.Lists.newArrayList;

/**
 */
/*
NodesRequest extends BaseNodesRequest<NodesRequest>,
                                           NodesResponse extends BaseNodesResponse,
                                           NodeRequest extends BaseNodeRequest,
                                           NodeResponse extends BaseNodeResponse>
 */
public class TransportNodesZooKeeperStatusAction extends
        TransportNodesAction<NodesZooKeeperStatusRequest,
                NodesZooKeeperStatusResponse,
                TransportNodesZooKeeperStatusAction.NodeZooKeeperStatusRequest,
                NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse
                > {
    private final ZooKeeperDiscovery zooKeeperDiscovery;

    private static final String ACTION_NAME = "/zookeeper/settings/get";

    private ClusterName clusterName;
    /*
    TransportNodesAction(Settings settings, String actionName, ThreadPool threadPool,
                                   ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                   Supplier<NodesRequest> request, Supplier<NodeRequest> nodeRequest,
                                   String nodeExecutor,
                                   Class<NodeResponse> nodeResponseClass) {
     */
    @Inject
    public TransportNodesZooKeeperStatusAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                               ClusterService clusterService, TransportService transportService,
                                               Discovery discovery, ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<NodesZooKeeperStatusRequest> request,
                                               Supplier<NodeZooKeeperStatusRequest> nodeRequest,
                                               String nodeExecutor,
                                               Class<NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse> nodeResponseClass) {
        super(settings, ACTION_NAME,  threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, request, nodeRequest, nodeExecutor, nodeResponseClass);
        if(discovery instanceof ZooKeeperDiscovery) {
            zooKeeperDiscovery = (ZooKeeperDiscovery) discovery;
        } else {
            zooKeeperDiscovery = null;
        }
        this.clusterName = clusterName;
    }

    /*@Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected NodesZooKeeperStatusRequest newRequest() {
        return new NodesZooKeeperStatusRequest();
    }*/

    /*
    @Override
    protected NodesZooKeeperStatusResponse newResponse(NodesZooKeeperStatusRequest nodesZooKeeperStatusRequest, AtomicReferenceArray responses) {

    }

    @Override
    protected NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse newResponse(NodesZooKeeperStatusRequest request, List<TransportNodesZooKeeperStatusAction.NodeZooKeeperStatusRequest> list, List<FailedNodeException> failures) {
        final List<NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse> nodeZooKeeperStatusResponses = new ArrayList();
        for (int i = 0; i < responses.size(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse) {
                nodeZooKeeperStatusResponses.add((NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse) resp);
            }
        }


        return new NodesZooKeeperStatusResponse(
                clusterName, nodeZooKeeperStatusResponses.toArray(
                new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse[nodeZooKeeperStatusResponses.size()]));
    }
    */

   /* @Override
    protected NodeZooKeeperStatusRequest newResponse(NodesZooKeeperStatusRequest request, List<NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse> responses, List<FailedNodeException> failures) {


        return null;
    }*/

    //@Override
   // protected NodeZooKeeperStatusRequest newNodeRequest() {
   //     return new NodeZooKeeperStatusRequest();
   // }


    @Override
    protected NodesZooKeeperStatusResponse newResponse(NodesZooKeeperStatusRequest request, List<NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse> nodeZooKeeperStatusResponses, List<FailedNodeException> failures) {
        final List<NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse> toReturn = new ArrayList();

        for (int i = 0; i < nodeZooKeeperStatusResponses.size(); i++) {
            Object resp = nodeZooKeeperStatusResponses.get(i);
            if (resp instanceof NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse) {
                toReturn.add((NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse) resp);
            }
        }


        return new NodesZooKeeperStatusResponse(clusterName, toReturn,failures );

       // return new NodesZooKeeperStatusResponse(
         //       clusterName, nodeZooKeeperStatusResponses.toArray(
           //     new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse[nodeZooKeeperStatusResponses.size()]));
    }

    @Override
    protected NodeZooKeeperStatusRequest newNodeRequest(String nodeId, NodesZooKeeperStatusRequest request) {
        return new NodeZooKeeperStatusRequest(request, nodeId);
    }

    /*
        @Override
        protected NodeZooKeeperStatusRequest newNodeRequest(String nodeId, NodesZooKeeperStatusRequest nodesZooKeeperStatusRequest) {
            return new NodeZooKeeperStatusRequest(nodesZooKeeperStatusRequest, nodeId);
        }
    */
    @Override
    protected NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse newNodeResponse() {
        return new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse();
    }



    //@Override
    protected NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse nodeOperation(NodeZooKeeperStatusRequest nodeZooKeeperStatusRequest) throws ElasticsearchException {
        if (zooKeeperDiscovery != null) {
            try {
                return new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse(
                        clusterService.state().nodes().getLocalNode(), true,
                        zooKeeperDiscovery.verifyConnection(nodeZooKeeperStatusRequest.timeout()));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        return new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse(
                clusterService.state().nodes().getLocalNode(), false, false);
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    public class NodeZooKeeperStatusRequest extends BaseNodeRequest {

        private TimeValue zooKeeperTimeout;

        private NodeZooKeeperStatusRequest() {

        }

        private NodeZooKeeperStatusRequest(NodesZooKeeperStatusRequest request, String nodeId) {
            super(nodeId);
            zooKeeperTimeout = request.zooKeeperTimeout();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            zooKeeperTimeout = new TimeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            zooKeeperTimeout.writeTo(out);
        }

        public TimeValue timeout() {
            return zooKeeperTimeout;
        }
    }
}
