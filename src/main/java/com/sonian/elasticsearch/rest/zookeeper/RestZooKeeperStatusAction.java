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

package com.sonian.elasticsearch.rest.zookeeper;

import com.sonian.elasticsearch.action.zookeeper.NodesZooKeeperStatusRequest;
import com.sonian.elasticsearch.action.zookeeper.NodesZooKeeperStatusResponse;
import com.sonian.elasticsearch.action.zookeeper.TransportNodesZooKeeperStatusAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 */
public class RestZooKeeperStatusAction implements RestHandler {

    private final TransportNodesZooKeeperStatusAction transportNodesZooKeeperStatusAction;

    @Inject
    public RestZooKeeperStatusAction(Settings settings,  RestController controller, TransportNodesZooKeeperStatusAction transportNodesZooKeeperStatusAction) {

        controller.registerHandler(RestRequest.Method.GET, "/_zookeeper/status", this);
        controller.registerHandler(RestRequest.Method.GET, "/_zookeeper/status/{nodeId}", this);
        this.transportNodesZooKeeperStatusAction = transportNodesZooKeeperStatusAction;
    }



    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {

    }


    List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {

        return Collections.emptyList();
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesZooKeeperStatusRequest zooKeeperStatusRequest = new NodesZooKeeperStatusRequest(nodesIds);
        zooKeeperStatusRequest.zooKeeperTimeout(request.paramAsTime("timeout", TimeValue.timeValueSeconds(10)));
        transportNodesZooKeeperStatusAction.execute(zooKeeperStatusRequest, new RestBuilderListener<NodesZooKeeperStatusResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesZooKeeperStatusResponse result, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, result.toXContent(builder, null));
            }
        });
    }
}
