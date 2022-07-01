/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.client;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.mpp.rpc.thrift.TAddConsensusGroup;
import org.apache.iotdb.mpp.rpc.thrift.TDisableDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Synchronously send RPC requests to DataNodes. See mpp.thrift for more details. */
public class SyncDataNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncDataNodeClientPool.class);

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;

  private SyncDataNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ConfigNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
  }

  public TSStatus invalidatePartitionCache(
      TEndPoint endPoint, TInvalidateCacheReq invalidateCacheReq) {
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      status = client.invalidatePartitionCache(invalidateCacheReq);
      LOGGER.info("Invalid Schema Cache {} successfully", invalidateCacheReq);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
      status = new TSStatus(TSStatusCode.TIME_OUT.getStatusCode());
    } catch (TException e) {
      LOGGER.error("Invalid Schema Cache on DataNode {} failed", endPoint, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }
    return status;
  }

  public TSStatus invalidateSchemaCache(
      TEndPoint endPoint, TInvalidateCacheReq invalidateCacheReq) {
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      status = client.invalidateSchemaCache(invalidateCacheReq);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
      status = new TSStatus(TSStatusCode.TIME_OUT.getStatusCode());
    } catch (TException e) {
      LOGGER.error("Invalid Schema Cache on DataNode {} failed", endPoint, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }
    return status;
  }

  public void deleteRegions(Set<TRegionReplicaSet> deletedRegionSet) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> regionInfoMap = new HashMap<>();
    deletedRegionSet.forEach(
        (tRegionReplicaSet) -> {
          for (TDataNodeLocation dataNodeLocation : tRegionReplicaSet.getDataNodeLocations()) {
            regionInfoMap
                .computeIfAbsent(dataNodeLocation, k -> new ArrayList<>())
                .add(tRegionReplicaSet.getRegionId());
          }
        });
    LOGGER.info("Current regionInfoMap {} ", regionInfoMap);
    regionInfoMap.forEach(
        (dataNodeLocation, regionIds) ->
            deleteRegions(dataNodeLocation.getInternalEndPoint(), regionIds, deletedRegionSet));
  }

  private void deleteRegions(
      TEndPoint endPoint,
      List<TConsensusGroupId> regionIds,
      Set<TRegionReplicaSet> deletedRegionSet) {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      for (TConsensusGroupId regionId : regionIds) {
        LOGGER.debug("Delete region {} ", regionId);
        final TSStatus status = client.deleteRegion(regionId);
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.info("DELETE Region {} successfully", regionId);
          deletedRegionSet.removeIf(k -> k.getRegionId().equals(regionId));
        }
      }
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
    } catch (TException e) {
      LOGGER.error("Delete Region on DataNode {} failed", endPoint, e);
    }
  }

  /**
   * broadcast the datanode is disabled. when the datanode removed form cluster, will call it.
   *
   * @param targetServer the online target datanode.
   * @param disabledDataNode the disabled datanode, it will be removed
   * @return TSStatus
   */
  public TSStatus disableDataNode(TEndPoint targetServer, TDataNodeLocation disabledDataNode) {
    LOGGER.info(
        "send RPC to data node: {} for disabling the data node: {}",
        targetServer,
        disabledDataNode);
    TDisableDataNodeReq req = new TDisableDataNodeReq(disabledDataNode);
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(targetServer)) {
      status = client.disableDataNode(req);
    } catch (IOException e) {
      LOGGER.error("Can't connect to Data node: {}", targetServer, e);
      status = new TSStatus(TSStatusCode.NO_CONNECTION.getStatusCode());
      status.setMessage(e.getMessage());
    } catch (TException e) {
      LOGGER.error(
          "Disable DataNode {} on target server {} error", disabledDataNode, targetServer, e);
      status = new TSStatus(TSStatusCode.NODE_DELETE_FAILED_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }

  /**
   * stop datanode
   *
   * @param dataNode TDataNodeLocation
   * @return TSStatus
   */
  public TSStatus stopDataNode(TDataNodeLocation dataNode) {
    LOGGER.info("send RPC to Data Node: {} to stop it", dataNode);
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client =
        clientManager.borrowClient(dataNode.getInternalEndPoint())) {
      status = client.stopDataNode();
    } catch (IOException e) {
      LOGGER.error("Can't connect to Data Node: {}", dataNode, e);
      status = new TSStatus(TSStatusCode.NO_CONNECTION.getStatusCode());
      status.setMessage(e.getMessage());
    } catch (TException e) {
      LOGGER.error("Stop Data Node {} error", dataNode, e);
      status = new TSStatus(TSStatusCode.DATANODE_STOP_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }

  /**
   * change a region leader from the datanode to other datanode other datanode should be in same
   * raft group
   *
   * @param regionId the region which will changer leader
   * @param dataNode data node server, change regions leader from it
   * @param newLeaderNode target data node, change regions leader to it
   * @return TSStatus
   */
  public TSStatus changeRegionLeader(
      TConsensusGroupId regionId, TEndPoint dataNode, TDataNodeLocation newLeaderNode) {
    LOGGER.info("send RPC to data node: {} for changing regions leader on it", dataNode);
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(dataNode)) {
      TRegionLeaderChangeReq req = new TRegionLeaderChangeReq(regionId, newLeaderNode);
      status = client.changeRegionLeader(req);
    } catch (IOException e) {
      LOGGER.error("Can't connect to Data node: {}", dataNode, e);
      status = new TSStatus(TSStatusCode.NO_CONNECTION.getStatusCode());
      status.setMessage(e.getMessage());
    } catch (TException e) {
      LOGGER.error("Change regions leader error on Date node: {}", dataNode, e);
      status = new TSStatus(TSStatusCode.REGION_LEADER_CHANGE_FAILED.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }

  public TSStatus addToRegionConsensusGroup(
      List<TDataNodeLocation> regionOriginalPeerNodes,
      TConsensusGroupId regionId,
      TDataNodeLocation newPeerNode,
      String storageGroup,
      long ttl) {
    TSStatus status;
    // do addConsensusGroup in new node locally
    try (SyncDataNodeInternalServiceClient client =
        clientManager.borrowClient(newPeerNode.getInternalEndPoint())) {
      List<TDataNodeLocation> currentPeerNodes = new ArrayList<>(regionOriginalPeerNodes);
      currentPeerNodes.add(newPeerNode);
      TAddConsensusGroup req = new TAddConsensusGroup(regionId, currentPeerNodes, storageGroup);
      req.setTtl(ttl);
      status = client.addToRegionConsensusGroup(req);
    } catch (IOException e) {
      LOGGER.error("Can't connect to Data Node {}.", newPeerNode.getInternalEndPoint(), e);
      status = new TSStatus(TSStatusCode.NO_CONNECTION.getStatusCode());
      status.setMessage(e.getMessage());
    } catch (TException e) {
      LOGGER.error(
          "Add region consensus {} group failed to Date node: {}", regionId, newPeerNode, e);
      status = new TSStatus(TSStatusCode.REGION_MIGRATE_FAILED.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }

  private boolean isSucceed(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private boolean isFailed(TSStatus status) {
    return !isSucceed(status);
  }

  /**
   * migrate a region from the datanode
   *
   * @param dataNode region location
   * @param regionId region id
   * @param newNode migrate region to it
   * @return TSStatus
   */
  public TSStatus migrateRegion(
      TDataNodeLocation dataNode, TConsensusGroupId regionId, TDataNodeLocation newNode) {
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client =
        clientManager.borrowClient(dataNode.getInternalEndPoint())) {
      TMigrateRegionReq req = new TMigrateRegionReq(regionId, dataNode, newNode);
      status = client.migrateRegion(req);
    } catch (IOException e) {
      LOGGER.error("Can't connect to Data Node {}", dataNode, e);
      status = new TSStatus(TSStatusCode.NO_CONNECTION.getStatusCode());
      status.setMessage(e.getMessage());
    } catch (TException e) {
      LOGGER.error("migrate region on Data Node failed{}", dataNode, e);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }

  public TSStatus invalidatePermissionCache(
      TEndPoint endPoint, TInvalidatePermissionCacheReq invalidatePermissionCacheReq) {
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      status = client.invalidatePermissionCache(invalidatePermissionCacheReq);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
      status = new TSStatus(TSStatusCode.TIME_OUT.getStatusCode());
    } catch (TException e) {
      LOGGER.error("Invalid Permission Cache on DataNode {} failed", endPoint, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }
    return status;
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class ClientPoolHolder {

    private static final SyncDataNodeClientPool INSTANCE = new SyncDataNodeClientPool();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static SyncDataNodeClientPool getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
