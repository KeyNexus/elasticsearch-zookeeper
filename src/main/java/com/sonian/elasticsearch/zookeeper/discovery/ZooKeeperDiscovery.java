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

package com.sonian.elasticsearch.zookeeper.discovery;

import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.sonian.elasticsearch.zookeeper.client.AbstractNodeListener;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClient;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClientSessionExpiredException;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperEnvironment;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.*;
import  org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.discovery.zen.PublishClusterStateAction;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;
//import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.elasticsearch.discovery.DiscoverySettings.NO_MASTER_BLOCK_ALL;

/**
 * @author imotov
 */
public class ZooKeeperDiscovery extends AbstractLifecycleComponent implements Discovery, DiscoveryNodesProvider {

    private final TransportService transportService;

   private final ClusterService clusterService;

    private RoutingService routingService;

    private final ClusterName clusterName;

    private final ThreadPool threadPool;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    //private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private final ZooKeeperClient zooKeeperClient;

    private DiscoveryNode localNode;

    private String localNodePath;

    private final StatePublisher statePublisher;

    private volatile boolean master = false;

    private volatile DiscoveryNodes latestDiscoNodes;

    private volatile Thread currentJoinThread;

    private final Lock updateNodeListLock = new ReentrantLock();

    private final MasterNodeListChangedListener masterNodeListChangedListener = new MasterNodeListChangedListener();

    private final SessionStateListener sessionResetListener = new SessionStateListener();

  // private final NodeService discoveryNodeService;

    private final ZooKeeperEnvironment environment;

    private final AtomicBoolean connected = new AtomicBoolean();

    private AllocationService allocationService;

    @Nullable
    private NodeService nodeService;


    @Inject public ZooKeeperDiscovery(Settings settings, ZooKeeperEnvironment environment, ClusterName clusterName, ThreadPool threadPool,
                                      TransportService transportService, ClusterService clusterService,// NodeService discoveryNodeService,
                                      DiscoverySettings discoverySettings, ZooKeeperClient zooKeeperClient) {
        super(settings);
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        this.transportService = transportService;
        //this.discoveryNodeService = discoveryNodeService;
        this.zooKeeperClient = zooKeeperClient;
        this.threadPool = threadPool;
        this.environment = environment;
       // if (settings.getAsBoolean("state_publishing.enabled", false)) {
            statePublisher = new ZooKeeperStatePublisher(settings, environment, zooKeeperClient, this);
       // }
        //else {
         //   statePublisher = new ZenStatePublisher(settings, transportService, this, new NewClusterStateListener(), discoverySettings, clusterName);
       // }
    }

    public void setRoutingService(RoutingService routingService) {
        this.routingService = routingService;
    }

    @Override protected void doStart() throws ElasticsearchException {
        // note, we rely on the fact that its a new id each time we start, see FD and "kill -9" handling
       // String nodeId = "";// Strings.randomBase64UUID();
        localNode = clusterService.localNode();
        localNodePath = nodePath(localNode.getId());
        latestDiscoNodes = clusterService.state().nodes();
        initialStateSent.set(false);
        zooKeeperClient.addSessionStateListener(sessionResetListener);
        zooKeeperClient.start();
        createRootNodes();

        statePublisher.start();

        // do the join on a different thread, the DiscoveryService waits for 30s anyhow till it is discovered
        asyncJoinCluster(true);
    }

    private void createRootNodes() {
        try {
            logger.trace("Creating root nodes in ZooKeeper");
            zooKeeperClient.createPersistentNode(environment.clusterNodePath());
            zooKeeperClient.createPersistentNode(environment.nodesNodePath());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override protected void doStop() throws ElasticsearchException {
        statePublisher.stop();

        zooKeeperClient.removeSessionStateListener(sessionResetListener);
        logger.trace("Stopping zooKeeper client");
        zooKeeperClient.stop();
        logger.trace("Stopped zooKeeper client");
        master = false;
        if (currentJoinThread != null) {
            try {
                currentJoinThread.interrupt();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override protected void doClose() throws ElasticsearchException {
        zooKeeperClient.close();
    }

    @Override public DiscoveryNode localNode() {
        return localNode;
    }

    /*@Override public void addListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.add(listener);
    }
*/
  /*  @Override public void removeListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.remove(listener);
    }
*/

    @Override public String nodeDescription() {
        return clusterName.value() + "/" + localNode.getId();
    }

    @Override
    public void setAllocationService(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

 /*   @Override
    public void setNodeService(@Nullable NodeService nodeService) {
        this.nodeService = nodeService;
    }
*/

    @Override
    public DiscoveryStats stats() {
        return null;
    }

    @Override
    public DiscoverySettings getDiscoverySettings() {
        return null;
    }

    @Override
    public void startInitialJoin() {

    }

    @Override
    public int getMinimumMasterNodes() {
        return 0;
    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, AckListener ackListener) {
        if (!master) {
            logger.warn("Shouldn't publish state when not master");
        }
        if (!lifecycle.started()) {
            return;
        }
        try {
            // Make sure we are still master
            byte[] masterNode = zooKeeperClient.getNode(environment.masterNodePath(), null);
            if (masterNode == null || !new String(masterNode).equals(localNode.getId())) {
                logger.warn("No longer a master, shouldn't publish new state");
                return;
            }
            latestDiscoNodes = clusterChangedEvent.state().nodes();
            statePublisher.publish(clusterChangedEvent.state(), ackListener);
        } catch (ZooKeeperClientSessionExpiredException ex) {
            // Ignore
        } catch (Exception ex) {
            logger.error("Cannot publish state", ex);
        }
    }

 /*
    @Override public void publish(ClusterState clusterState, AckListener ackListener) {
        if (!master) {
            logger.warn("Shouldn't publish state when not master");
        }
        if (!lifecycle.started()) {
            return;
        }
        try {
            // Make sure we are still master
            byte[] masterNode = zooKeeperClient.getNode(environment.masterNodePath(), null);
            if (masterNode == null || !new String(masterNode).equals(localNode.id())) {
                logger.warn("No longer a master, shouldn't publish new state");
                return;
            }
            latestDiscoNodes = clusterState.nodes();
            statePublisher.publish(clusterState, ackListener);
        } catch (ZooKeeperClientSessionExpiredException ex) {
            // Ignore
        } catch (Exception ex) {
            logger.error("Cannot publish state", ex);
        }
    }
*/

    @Override public DiscoveryNodes nodes() {
        DiscoveryNodes latestNodes = this.latestDiscoNodes;
        if (latestNodes != null) {
            return latestNodes;
        }
        // have not decided yet, just send the local node
        return clusterService.state().nodes();
    }

    /*
    @Override
    public NodeService nodeService() {
        return nodeService;
    }
*/
    public boolean verifyConnection(TimeValue timeout) throws InterruptedException {
        if(connected.get()) {
            return zooKeeperClient.verifyConnection(timeout);
        } return false;
    }

    private void asyncJoinCluster(final boolean initial) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
            @Override public void run() {
                currentJoinThread = Thread.currentThread();
                try {
                    innerJoinCluster(initial);
                } finally {
                    currentJoinThread = null;
                }
            }
        });
    }

    private void innerJoinCluster(boolean initial) {
        try {
            if (!initial || register()) {
                // Check if node should propose itself as a master
                if (localNode.isMasterNode()) {
                    electMaster();
                } else {
                    findMaster(initial);
                }
            }
        } catch (InterruptedException ex) {
            // Ignore
        }
    }

    private boolean register() {
        if (lifecycle.stoppedOrClosed()) {
            return false;
        }
        try {
            logger.trace("Registering in ZooKeeper");
            // Create an ephemeral node that contains our nodeInfo
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            localNode.writeTo(streamOutput);
            byte[] buf = streamOutput.bytes().toBytesRef().bytes;
            zooKeeperClient.setOrCreateTransientNode(localNodePath, buf);
            return true;
        } catch (Exception ex) {
            restartDiscovery();
            return false;
        }
    }

    private void findMaster(final boolean initial) throws InterruptedException {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        ZooKeeperClient.NodeListener nodeListener = new AbstractNodeListener() {
            @Override public void onNodeCreated(String id) {
                handleMasterAppeared(initial);
            }

            @Override public void onNodeDeleted(String id) {
                handleMasterGone();
            }
        };

        byte[] masterId = zooKeeperClient.getNode(environment.masterNodePath(), nodeListener);
        if (masterId == null) {
            if (!initial) {
                removeMaster();
            }
        } else {
            addMaster(new String(masterId));
        }
    }

    private void electMaster() throws InterruptedException {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        logger.trace("Electing master");
        ZooKeeperClient.NodeListener nodeListener = new AbstractNodeListener() {
            @Override public void onNodeDeleted(String id) {
                handleMasterGone();
            }
        };
        byte[] masterId = localNode().getId().getBytes();

        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        try {
            byte[] electedMasterId = zooKeeperClient.getOrCreateTransientNode(environment.masterNodePath(), masterId, nodeListener);
            String electedMasterIdStr = new String(electedMasterId);
            if (localNode.getId().equals(electedMasterIdStr)) {
                becomeMaster();
            } else {
                addMaster(electedMasterIdStr);
            }
        } catch (Exception ex) {
            logger.error("Couldn't elect master. Restarting discovery.", ex);
            restartDiscovery();
        }
    }

    private void addMaster(String masterNodeId) throws InterruptedException {
        logger.trace("Found master: {}", masterNodeId);
        master = false;
        statePublisher.addMaster(masterNodeId);
    }

    private void removeMaster() {
        clusterService.submitStateUpdateTask("zoo-keeper-disco-no-master (no_master_found)",  new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData metaData = currentState.metaData();
                RoutingTable routingTable = currentState.routingTable();
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).addGlobalBlock(NO_MASTER_BLOCK_ALL).build();
                // if this is a data node, clean the metadata and routing, since we want to recreate the indices and shards
                if (currentState.nodes().getLocalNode() != null && currentState.nodes().getLocalNode().isDataNode()) {
                    metaData = MetaData.builder().build();
                    routingTable = RoutingTable.builder().build();
                }
                DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes());
                DiscoveryNode masterNode = currentState.nodes().getMasterNode();
                if (masterNode != null) {
                    builder = builder.remove(masterNode.getId());
                }
                // Make sure that local node is present
                if (currentState.nodes().getLocalNode() == null) {
                    builder.add(localNode).localNodeId(localNode.getId());
                }
                latestDiscoNodes = builder.build();
                return ClusterState.builder(currentState)
                        .blocks(clusterBlocks)
                        .nodes(latestDiscoNodes)
                        .metaData(metaData)
                        .routingTable(routingTable)
                        .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("unexpected failure during [{}]", e, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                sendInitialStateEventIfNeeded();
            }


        });
    }

    private void becomeMaster() throws InterruptedException {
        logger.trace("Elected as master ({})", localNode.getId());
        this.master = true;
        statePublisher.becomeMaster();
        clusterService.submitStateUpdateTask("zoo-keeper-disco-join (elected_as_master)",  new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes());
                // Make sure that the current node is present
                if (currentState.nodes().getLocalNode() == null) {
                    builder.add(localNode);
                }
                // update the fact that we are the master...
                builder.localNodeId(localNode.getId()).masterNodeId(localNode.getId());
                latestDiscoNodes = builder.build();
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(NO_MASTER_BLOCK_ALL).build();
                return ClusterState.builder(currentState).nodes(latestDiscoNodes).blocks(clusterBlocks).build();
            }

            @Override public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                sendInitialStateEventIfNeeded();
            }

            @Override public void onFailure(String source, Exception e) {
                logger.error("unexpected failure during [{}]", e, source);
            }
        });

        handleUpdateNodeList();
    }

    private void restartDiscovery() {
        if (!lifecycle.started()) {
            return;
        }
        logger.trace("Restarting ZK Discovery");
        createRootNodes();
        master = false;
        asyncJoinCluster(true);
    }

    private void setSessionDisconnected() {
        logger.trace("Session Disconnected");
        connected.set(false);
    }

    private void setSessionConnected() {
        logger.trace("Session Connected");
        connected.set(true);
    }

    private void updateNodeList(final Set<String> nodes) {
        clusterService.submitStateUpdateTask("zoo-keeper-disco-update-node-list",  new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    Set<String> currentNodes = new HashSet<>();

                    for (Iterator<String> iter = latestDiscoNodes.getNodes().keysIt(); iter.hasNext();) {
                        currentNodes.add(iter.next());
                    }
                    //Sets.newHashSet(latestDiscoNodes.nodes().keysIt());
                    Set<String> deleted = new HashSet<String>(currentNodes);
                    deleted.removeAll(nodes);
                    Set<String> added = new HashSet<String>(nodes);
                    added.removeAll(currentNodes);
                    logger.trace("Current nodes: [{}], new nodes: [{}], deleted: [{}], added[{}]", currentNodes, nodes, deleted, added);
                    if (!deleted.isEmpty() || !added.isEmpty()) {
                        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes());
                        for (String nodeId : deleted) {
                            if (currentState.nodes().nodeExists(nodeId)) {
                                builder.remove(nodeId);
                            } else {
                                logger.warn("Trying to deleted a node that doesn't exist {}", nodeId);
                                return currentState;
                            }
                        }
                        for (String nodeId : added) {
                            if (!nodeId.equals(localNode.getId())) {
                                DiscoveryNode node = nodeInfo(nodeId);
                                if (node != null) {
                                    if (currentState.nodes().nodeExists(node.getId())) {
                                        // the node already exists in the cluster
                                        logger.warn("received a join request for an existing node [{}]", node);
                                    } else {
                                        builder.add(node);
                                    }
                                }
                            }
                        }
                        latestDiscoNodes = builder.build();
                        return ClusterState.builder(currentState).nodes(latestDiscoNodes).build();
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("unexpected failure during [{}]", e, source);
            }
        });
    }


    private void sendInitialStateEventIfNeeded() {
       /* if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }*/
    }

    private void handleNewClusterStateFromMaster(final ClusterState clusterState) {
        //final PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessedHandler) {
        if (!lifecycle.started()) {
            return;
        }
        if (!master) {
            // Make sure that we are part of the state
            if (clusterState.nodes().getLocalNode() != null) {
                clusterService.submitStateUpdateTask("zoo-keeper-disco-receive(from master [" + clusterState.nodes().getMasterNode() + "])",  new ClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        latestDiscoNodes = clusterState.nodes();
                        return clusterState;
                    }

                    /*@Override public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        sendInitialStateEventIfNeeded();
                        if (newStateProcessedHandler != null) {
                            newStateProcessedHandler.onNewClusterStateProcessed();
                        }
                    }*/

                    @Override public void onFailure(String source, Exception e) {

                    }
                });
            } else {
                if (logger.isTraceEnabled()) {
                    StringBuilder sb = new StringBuilder("Received new state, but not part of the state:\nversion [").append(clusterState.version()).append("]\n");
                    sb.append(clusterState.nodes());
                    sb.append(clusterState.routingTable());
                    sb.append(clusterState.getNodes().getDataNodes());
                    logger.trace(sb.toString());
                } else if (logger.isDebugEnabled()) {
                    logger.debug("Received new state, but not part of the state");
                }
            }
        } else {
            logger.warn("Received new state, but node is master");
        }
    }

    private void handleUpdateNodeList() {
        if (!lifecycle.started()) {
            return;
        }
        if (!master) {
            logger.trace("No longer master - shouldn't monitor node changes");
            return;
        }
        logger.trace("Updating node list");
        boolean restart = false;
        updateNodeListLock.lock();
        try {
            Set<String> nodes = zooKeeperClient.listNodes(environment.nodesNodePath(), masterNodeListChangedListener);
            updateNodeList(nodes);
        } catch (ZooKeeperClientSessionExpiredException ex) {
            restart = true;
        } catch (Exception ex) {
            restart = true;
            logger.error("Couldn't update node list.", ex);
        } finally {
            updateNodeListLock.unlock();
        }
        if (restart) {
            restartDiscovery();
        }
    }

    public DiscoveryNode nodeInfo(final String id) throws ElasticsearchException, InterruptedException {
        try {
            byte[] buf = zooKeeperClient.getNode(nodePath(id), null);
            if (buf != null) {
                return new DiscoveryNode(new InputStreamStreamInput(new ByteArrayInputStream(buf)));
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Cannot get node info " + id, e);
        }
    }

    private String nodePath(String id) {
        return environment.nodesNodePath() + "/" + id;
    }


    private void handleMasterGone() {
        if (!lifecycle.started()) {
            return;
        }
        logger.info("Master is gone");
        asyncJoinCluster(false);
    }

    private void handleMasterAppeared(boolean initial) {
        if (!lifecycle.started()) {
            return;
        }
        logger.info("New master appeared");
        asyncJoinCluster(initial);
    }

    private class MasterNodeListChangedListener implements ZooKeeperClient.NodeListChangedListener {

        @Override public void onNodeListChanged() {
            handleUpdateNodeList();
        }
    }

    private class NewClusterStateListener implements ClusterStateListener {
       /* @Override public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
            handleNewClusterStateFromMaster(clusterState, newStateProcessed);
        }
*/
        @Override
        public void clusterChanged(ClusterChangedEvent event) {

            handleNewClusterStateFromMaster(event.state());
        }
    }

    private class NewZooKeeperClusterStateListener implements ZooKeeperClusterState.NewClusterStateListener {

        @Override public void onNewClusterState(ClusterState clusterState) {
            handleNewClusterStateFromMaster(clusterState);
        }
    }

    private class SessionStateListener implements ZooKeeperClient.SessionStateListener {

        @Override
        public void sessionDisconnected() {
            setSessionDisconnected();
        }

        @Override
        public void sessionConnected() {
            setSessionConnected();
        }

        @Override public void sessionExpired() {
            restartDiscovery();
        }



    }

    private interface StatePublisher {
        void start();

        void stop();

        void publish(ClusterState clusterState, AckListener ackListener);

        void addMaster(String masterNodeId) throws InterruptedException;

        void becomeMaster() throws InterruptedException;

    }


    private class ZooKeeperStatePublisher implements StatePublisher {
        private final ZooKeeperClusterState zooKeeperClusterState;

        public ZooKeeperStatePublisher(Settings settings, ZooKeeperEnvironment environment, ZooKeeperClient zooKeeperClient, DiscoveryNodesProvider nodesProvider) {
            zooKeeperClusterState = new ZooKeeperClusterState(settings, environment, zooKeeperClient, nodesProvider, clusterName);

        }


        @Override public void start() {
            zooKeeperClusterState.start();
        }

        @Override public void stop() {
            zooKeeperClusterState.stop();
        }

        @Override public void publish(ClusterState clusterState, AckListener ackListener) {
            try {
                // ignore the ack. rely on zk to handle distribution.
                zooKeeperClusterState.publish(clusterState, ackListener);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }

        }

        @Override public void addMaster(String masterNodeId) throws InterruptedException {
            ClusterState state = zooKeeperClusterState.retrieve(new NewZooKeeperClusterStateListener());
            if (state != null && masterNodeId.equals(state.nodes().getMasterNodeId())) {
                // Check that this state was published by elected master
                handleNewClusterStateFromMaster(state);
            }
        }

        @Override public void becomeMaster() throws InterruptedException {
            zooKeeperClusterState.syncClusterState();
        }
    }

    /*
    private class ZenStatePublisher implements StatePublisher {
        private final PublishClusterStateAction publishClusterState;

        public ZenStatePublisher(Settings settings, TransportService transportService, DiscoveryNodesProvider nodesProvider,
                                 NewClusterStateListener listener, DiscoverySettings discoverySettings, ClusterName clusterName) {

            PublishClusterStateAction.NewPendingClusterStateListener

            publishClusterState = new PublishClusterStateAction(
                    settings,
                    transportService,
                    null,
                    null,
                    listener,
                   // nodesProvider,
                    discoverySettings,
                    clusterName);
        }

        @Override public void start() {
        }

        @Override public void stop() {
        }

        @Override public void publish(ClusterState clusterState, AckListener ackListener) {
            publishClusterState.publish(clusterState, ackListener);
        }

        @Override public void addMaster(String masterNodeId) throws InterruptedException {
        }

        @Override public void becomeMaster() throws InterruptedException {
        }

    }

    public void setAllocationService(AllocationService allocationService) {

    }

    */


}
