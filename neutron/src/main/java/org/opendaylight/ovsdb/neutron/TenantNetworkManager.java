/*
 * Copyright (C) 2013 Red Hat, Inc. and others...
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 *
 * Authors : Madhu Venugopal, Brent Salisbury, Dave Tucker
 */
package org.opendaylight.ovsdb.neutron;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.opendaylight.controller.containermanager.ContainerConfig;
import org.opendaylight.controller.containermanager.ContainerFlowConfig;
import org.opendaylight.controller.containermanager.IContainerManager;
import org.opendaylight.controller.networkconfig.neutron.INeutronNetworkCRUD;
import org.opendaylight.controller.networkconfig.neutron.INeutronPortCRUD;
import org.opendaylight.controller.networkconfig.neutron.NeutronNetwork;
import org.opendaylight.controller.networkconfig.neutron.NeutronPort;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.utils.HexEncode;
import org.opendaylight.controller.sal.utils.ServiceHelper;
import org.opendaylight.controller.sal.utils.Status;
import org.opendaylight.controller.sal.utils.StatusCode;
import org.opendaylight.ovsdb.lib.notation.OvsDBSet;
import org.opendaylight.ovsdb.lib.notation.UUID;
import org.opendaylight.ovsdb.lib.table.Bridge;
import org.opendaylight.ovsdb.lib.table.Interface;
import org.opendaylight.ovsdb.lib.table.Open_vSwitch;
import org.opendaylight.ovsdb.lib.table.Port;
import org.opendaylight.ovsdb.lib.table.internal.Table;
import org.opendaylight.ovsdb.neutron.provider.ProviderNetworkManager;
import org.opendaylight.ovsdb.plugin.IConnectionServiceInternal;
import org.opendaylight.ovsdb.plugin.OVSDBConfigService;
import org.opendaylight.ovsdb.plugin.StatusWithUuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantNetworkManager {
    static final Logger logger = LoggerFactory.getLogger(TenantNetworkManager.class);

    public static final String EXTERNAL_ID_VM_ID = "vm-id";
    public static final String EXTERNAL_ID_INTERFACE_ID = "iface-id";
    public static final String EXTERNAL_ID_VM_MAC = "attached-mac";
    private static TenantNetworkManager tenantHelper = new TenantNetworkManager();
    private ConcurrentMap<String, NodeConfiguration> nodeConfigurationCache = new ConcurrentHashMap<>();

    private boolean enableContainer = false;
    private TenantNetworkManager() {
        String isTenantContainer = System.getProperty("TenantIsContainer");
        if (isTenantContainer != null && isTenantContainer.equalsIgnoreCase("true")) {
            enableContainer =  true;
        }
    }

    public static TenantNetworkManager getManager() {
        return tenantHelper;
    }

    public int getInternalVlan(Node node, String networkId) {
        String nodeUuid = getNodeUUID(node);
        if (nodeUuid == null) {
            logger.error("Unable to get UUID for Node {}", node);
            return 0;
        }

        NodeConfiguration nodeConfiguration = nodeConfigurationCache.get(nodeUuid);

        if (nodeConfiguration == null) {
            nodeConfiguration = addNodeConfigurationToCache(node);
        }
        Integer vlan = nodeConfiguration.getInternalVlan(networkId);
        if (vlan == null) return 0;
        return vlan.intValue();
    }

    private NodeConfiguration addNodeConfigurationToCache(Node node) {
        NodeConfiguration nodeConfiguration = new NodeConfiguration(node);
        String nodeUuid = getNodeUUID(node);
        if (nodeUuid == null) {
            logger.error("Cannot get Node UUID for Node {}", node);
            return null;
        }
        this.nodeConfigurationCache.put(nodeUuid, nodeConfiguration);
        return nodeConfigurationCache.get(nodeUuid);
    }

    public void reclaimTennantNetworkInternalVlan(Node node, String portUUID, NeutronNetwork network) {
        String nodeUuid = getNodeUUID(node);
        if (nodeUuid == null) {
            logger.error("Unable to get UUID for Node {}", node);
            return;
        }

        NodeConfiguration nodeConfiguration = nodeConfigurationCache.get(nodeUuid);

        // Cache miss
        if (nodeConfiguration == null)
        {
            logger.error("Configuration data unavailable for Node {} ", node);
            return;
        }

        int vlan = nodeConfiguration.reclaimInternalVlan(network.getID());
        if (vlan <= 0) {
            logger.error("Unable to get an internalVlan for Network {}", network);
            return;
        }
        logger.debug("Removed Vlan {} on {}", vlan, portUUID);
    }

    public void networkCreated (String networkId) {
        IConnectionServiceInternal connectionService = (IConnectionServiceInternal)ServiceHelper.getGlobalInstance(IConnectionServiceInternal.class, this);
        List<Node> nodes = connectionService.getNodes();

        for (Node node : nodes) {
            this.networkCreated(node, networkId);
        }

    }

    private String getNodeUUID(Node node) {
        String nodeUuid = new String();
        OVSDBConfigService ovsdbConfigService = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        try {
            Map<String, Table<?>> ovsTable = ovsdbConfigService.getRows(node, Open_vSwitch.NAME.getName());
            nodeUuid = (String)ovsTable.keySet().toArray()[0];
        }
        catch (Exception e) {
            logger.error("Unable to get the Open_vSwitch table for Node {}: {}", node, e);
        }

        return nodeUuid;
    }

    public int networkCreated (Node node, String networkId) {
        String nodeUuid = getNodeUUID(node);
        if (nodeUuid == null) {
            logger.error("Unable to get UUID for Node {}", node);
            return 0;
        }

        NodeConfiguration nodeConfiguration = nodeConfigurationCache.get(nodeUuid);

        // Cache miss
        if (nodeConfiguration == null)
        {
            nodeConfiguration = addNodeConfigurationToCache(node);
        }

        int internalVlan = nodeConfiguration.assignInternalVlan(networkId);
        logger.debug("networkCreated: node {}, networkId {}, internalVlan {}", node, networkId, internalVlan);
        if (enableContainer && internalVlan != 0) {
            IContainerManager containerManager = (IContainerManager)ServiceHelper.getGlobalInstance(IContainerManager.class, this);
            if (containerManager == null) {
                logger.error("ContainerManager is null. Failed to create Container for {}", networkId);
                return 0;
            }

            ContainerConfig config = new ContainerConfig();
            config.setContainer(BaseHandler.convertNeutronIDToKey(networkId));
            Status status = containerManager.addContainer(config);
            logger.debug("Container Creation Status for {} : {}", networkId, status.toString());

            ContainerFlowConfig flowConfig = new ContainerFlowConfig("InternalVlan", internalVlan+"",
                    null, null, null, null, null);
            List<ContainerFlowConfig> containerFlowConfigs = new ArrayList<ContainerFlowConfig>();
            containerFlowConfigs.add(flowConfig);
            containerManager.addContainerFlows(BaseHandler.convertNeutronIDToKey(networkId), containerFlowConfigs);
        }
        return internalVlan;
    }

    /**
     * Are there any TenantNetwork VM present on this Node ?
     * This method uses Interface Table's external-id field to locate the VM.
     */
    public boolean isTenantNetworkPresentInNode(Node node, String segmentationId) {
        String networkId = this.getNetworkIdForSegmentationId(segmentationId);
        if (networkId == null) {
            logger.debug("Tenant Network not found with Segmenation-id {}",segmentationId);
            return false;
        }
        if (ProviderNetworkManager.getManager().hasPerTenantTunneling()) {
            String nodeUuid = getNodeUUID(node);
            if (nodeUuid == null) {
                logger.debug("Unable to get UUID for Node {}", node);
                return false;
            }

            NodeConfiguration nodeConfiguration = nodeConfigurationCache.get(nodeUuid);

            // Cache miss
            if (nodeConfiguration == null)
            {
                logger.error("Configuration data unavailable for Node {} ", node);
                return false;
            }

            int internalVlan = nodeConfiguration.getInternalVlan(networkId);
            if (internalVlan == 0) {
                logger.debug("No InternalVlan provisioned for Tenant Network {}",networkId);
                return false;
            }
        }
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        try {
            /*
            // Vlan Tag based identification
            Map<String, Table<?>> portTable = ovsdbTable.getRows(node, Port.NAME.getName());
            if (portTable == null) {
                logger.debug("Port table is null for Node {} ", node);
                return false;
            }

            for (Table<?> row : portTable.values()) {
                Port port = (Port)row;
                Set<BigInteger> tags = port.getTag();
                if (tags.contains(internalVlan)) {
                    logger.debug("Tenant Network {} with Segmenation-id {} is present in Node {} / Port {}",
                                  networkId, segmentationId, node, port);
                    return true;
                }
            }
             */
            // External-id based more accurate VM Location identification
            Map<String, Table<?>> ifTable = ovsdbTable.getRows(node, Interface.NAME.getName());
            if (ifTable == null) {
                logger.debug("Interface table is null for Node {} ", node);
                return false;
            }

            for (Table<?> row : ifTable.values()) {
                Interface intf = (Interface)row;
                Map<String, String> externalIds = intf.getExternal_ids();
                if (externalIds != null && externalIds.get(EXTERNAL_ID_INTERFACE_ID) != null) {
                    if (this.isInterfacePresentInTenantNetwork(externalIds.get(EXTERNAL_ID_INTERFACE_ID), networkId)) {
                        logger.debug("Tenant Network {} with Segmentation-id {} is present in Node {} / Interface {}",
                                      networkId, segmentationId, node, intf);
                        return true;
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Error while trying to determine if network is present on node", e);
            return false;
        }

        logger.debug("Tenant Network {} with Segmentation-id {} is NOT present in Node {}",
                networkId, segmentationId, node);

        return false;
    }

    public String getNetworkIdForSegmentationId (String segmentationId) {
        INeutronNetworkCRUD neutronNetworkService = (INeutronNetworkCRUD)ServiceHelper.getGlobalInstance(INeutronNetworkCRUD.class, this);
        List <NeutronNetwork> networks = neutronNetworkService.getAllNetworks();
        for (NeutronNetwork network : networks) {
            if (network.getProviderSegmentationID().equalsIgnoreCase(segmentationId)) return network.getNetworkUUID();
        }
        return null;
    }

    public String getNetworkNameForSegmentationId(String segmentationId){
        INeutronNetworkCRUD neutronNetworkService = (INeutronNetworkCRUD)ServiceHelper.getGlobalInstance(INeutronNetworkCRUD.class, this);
        List <NeutronNetwork> networks = neutronNetworkService.getAllNetworks();
        for (NeutronNetwork network : networks) {
            if (network.getProviderSegmentationID().equalsIgnoreCase(segmentationId)) return network.getNetworkName();
        }
        return null;
    }

    private boolean isInterfacePresentInTenantNetwork (String portId, String networkId) {
        INeutronPortCRUD neutronPortService = (INeutronPortCRUD)ServiceHelper.getGlobalInstance(INeutronPortCRUD.class, this);
        NeutronPort neutronPort = neutronPortService.getPort(portId);
        if (neutronPort != null && neutronPort.getNetworkUUID().equalsIgnoreCase(networkId)) return true;
        return false;
    }

    public NeutronNetwork getTenantNetworkForInterface (Interface intf) {
        logger.trace("getTenantNetworkForInterface for {}", intf);
        if (intf == null) return null;
        Map<String, String> externalIds = intf.getExternal_ids();
        logger.trace("externalIds {}", externalIds);
        if (externalIds == null) return null;
        String neutronPortId = externalIds.get(EXTERNAL_ID_INTERFACE_ID);
        if (neutronPortId == null) return null;
        INeutronPortCRUD neutronPortService = (INeutronPortCRUD)ServiceHelper.getGlobalInstance(INeutronPortCRUD.class, this);
        NeutronPort neutronPort = neutronPortService.getPort(neutronPortId);
        logger.trace("neutronPort {}", neutronPort);
        // FIXME: there can be a bug here, when neutronPort is null. this silently fails, and there might be no more events to trigger the functionality
        if (neutronPort == null) return null;
        INeutronNetworkCRUD neutronNetworkService = (INeutronNetworkCRUD)ServiceHelper.getGlobalInstance(INeutronNetworkCRUD.class, this);
        NeutronNetwork neutronNetwork = neutronNetworkService.getNetwork(neutronPort.getNetworkUUID());
        logger.debug("getTenantNetworkForInterface: {} mappped to {}", intf, neutronNetwork);
        return neutronNetwork;
    }

    public void programTenantNetworkInternalVlan(Node node, String portUUID, NeutronNetwork network) {

        String nodeUuid = getNodeUUID(node);
        if (nodeUuid == null) {
            logger.error("Unable to get UUID for Node {}", node);
            return;
        }

        NodeConfiguration nodeConfiguration = nodeConfigurationCache.get(nodeUuid);

        // Cache miss
        if (nodeConfiguration == null)
        {
            logger.error("Configuration data unavailable for Node {} ", node);
            return;
        }

        int vlan = nodeConfiguration.getInternalVlan(network.getID());
        logger.debug("Programming Vlan {} on port {} in node {}", vlan, portUUID, node);
        if (vlan <= 0) {
            logger.error("Unable to get an internalVlan for Network {} Port {} Node {}", network, portUUID, node);
            return;
        }
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        Port port = new Port();
        OvsDBSet<BigInteger> tags = new OvsDBSet<BigInteger>();
        tags.add(BigInteger.valueOf(vlan));
        port.setTag(tags);
        ovsdbTable.updateRow(node, Port.NAME.getName(), null, portUUID, port);
        if (enableContainer) this.addPortToTenantNetworkContainer(node, portUUID, network);
    }

    private void addPortToTenantNetworkContainer(Node node, String portUUID, NeutronNetwork network) {
        logger.debug("addPortToTenantNetworkContainer Node {}, portUUID {}, network {}", node, portUUID, network);

        IContainerManager containerManager = (IContainerManager)ServiceHelper.getGlobalInstance(IContainerManager.class, this);
        if (containerManager == null) {
            logger.error("ContainerManager is not accessible");
            return;
        }
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        try {
            Port port = (Port)ovsdbTable.getRow(node, Port.NAME.getName(), portUUID);
            if (port == null) {
                logger.trace("Unable to identify Port with UUID {}", portUUID);
                return;
            }
            Set<UUID> interfaces = port.getInterfaces();
            if (interfaces == null) {
                logger.trace("No interfaces available to fetch the OF Port");
                return;
            }
            Bridge bridge = this.getBridgeIdForPort(node, portUUID);
            if (bridge == null) {
                logger.debug("Unable to spot Bridge for Port {} in node {}", port, node);
                return;
            }
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));

            for (UUID intfUUID : interfaces) {
                Interface intf = (Interface)ovsdbTable.getRow(node, Interface.NAME.getName(), intfUUID.toString());
                if (intf == null) continue;
                Set<BigInteger> of_ports = intf.getOfport();
                if (of_ports == null) continue;
                for (BigInteger of_port : of_ports) {
                    ContainerConfig config = new ContainerConfig();
                    config.setContainer(BaseHandler.convertNeutronIDToKey(network.getID()));
                    logger.debug("Adding Port {} to Container : {}", port.toString(), config.getContainer());
                    List<String> ncList = new ArrayList<String>();
                    Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
                    NodeConnector nc = NodeConnector.fromStringNoNode(Node.NodeIDType.OPENFLOW.toString(),
                                                                      Long.valueOf(of_port.longValue()).intValue()+"",
                                                                      ofNode);
                    ncList.add(nc.toString());
                    config.addNodeConnectors(ncList);

                    Status status = containerManager.addContainerEntry(BaseHandler.convertNeutronIDToKey(network.getID()), ncList);

                    if (!status.isSuccess()) {
                        logger.error(" Failed {} : to add port {} to container - {}",
                                status, nc, network.getID());
                    } else {
                        logger.error(" Successfully added port {} to container - {}",
                                       nc, network.getID());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Exception in addPortToTenantNetworkContainer", e);
        }
    }

    private Bridge getBridgeIdForPort (Node node, String uuid) {
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        try {
            Map<String, Table<?>> bridges = ovsdbTable.getRows(node, Bridge.NAME.getName());
            if (bridges == null) return null;
            for (String bridgeUUID : bridges.keySet()) {
                Bridge bridge = (Bridge)bridges.get(bridgeUUID);
                Set<UUID> portUUIDs = bridge.getPorts();
                logger.trace("Scanning Bridge {} to identify Port : {} ",bridge, uuid);
                for (UUID portUUID : portUUIDs) {
                    if (portUUID.toString().equalsIgnoreCase(uuid)) {
                        logger.trace("Found Port {} -> ", uuid, bridgeUUID);
                        return bridge;
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to get BridgeId port {} in Node {}", uuid, node);
        }
        return null;
    }

    public void networkDeleted(String id) {
        if (!enableContainer) return;

        IContainerManager containerManager = (IContainerManager)ServiceHelper.getGlobalInstance(IContainerManager.class, this);
        if (containerManager == null) {
            logger.error("ContainerManager is not accessible");
            return;
        }

        String networkID = BaseHandler.convertNeutronIDToKey(id);
        ContainerConfig config = new ContainerConfig();
        config.setContainer(networkID);
        containerManager.removeContainer(config);
    }

    /*
     * Creating dedicated bridges for a tenant's network
     * Tenant network UUID: 93409XX
     *
    Bridge "brint-93409"
        Controller "tcp:192.168.1.234:6633"
            is_connected: true
        Port "p-tun-93409"
            Interface "p-tun-93409"
                type: patch
                options: {peer="p-int-93409"}
        Port "brint-93409"
            Interface "brint-93409"
    Bridge "brtun-93409"
        Controller "tcp:192.168.1.234:6633"
            is_connected: true
        Port "brtun-93409"
            Interface "brtun-93409"
        Port "p-int-93409"
            Interface "p-int-93409"
                type: patch
                options: {peer="p-tun-93409"}
        Port "p-c-t-93409"
            Interface "p-c-t-93409"
                type: patch
                options: {peer="p-t-c-93409"}
    Bridge br-tun
        Controller "tcp:192.168.1.234:6633"
            is_connected: true
        Port "p-t-c-93409"
            Interface "p-t-c-93409"
                type: patch
                options: {peer="p-c-t-93409"}
        Port br-tun
            Interface br-tun
        Port patch-int
            Interface patch-int
                type: patch
                options: {peer=patch-tun}

     */
    private void createDedicatedBridgesForNetwork(Node node, NeutronNetwork network) throws Exception {

        logger.debug("createDedicatedBridgesForNetwork Node {}, Network {}", node, network);
        String networkBrTun = getDedicatedTunBridgeNameForNetwork(network);
        String networkBrInt = getDedicatedIntBridgeNameForNetwork(network);

        String networkPatchTun = getPatchToDedicatedTunForNetwork(network);
        String networkPatchInt = getPatchToDedicatedIntForNetwork(network);

        String externalPortName = getExInfNameForNetwork(network);

        String networkBrTunUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, networkBrTun);
        String networkBrIntUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, networkBrInt);

        Status status;
        //TODO: It might be better to check the patch port as well, and create them if missing
        if (networkBrIntUUID == null){
            status = InternalNetworkManager.getManager().addInternalBridge(node, networkBrInt, networkPatchTun, networkPatchInt);
            if (!status.isSuccess()) logger.error("Dedicated Integration Bridge Creation Status {}, for networkBrInt {}, networkPatchTun {}, networkPatchInt {}", status.toString(), networkBrInt, networkPatchTun, networkPatchInt);
            logger.debug("Dedicated Integration Bridge Creation Status {}, for networkBrInt {}, networkPatchTun {}, networkPatchInt {}", status.toString(), networkBrInt, networkPatchTun, networkPatchInt);
        } else {
            logger.debug("Dedicated Integration Bridge already exists {}", networkBrIntUUID);
        }

        //TODO: It might be better to check the patch port as well, and create them if missing
        if (networkBrTunUUID == null){
            status = InternalNetworkManager.getManager().addInternalBridge(node, networkBrTun, networkPatchInt, networkPatchTun);
            if (!status.isSuccess()) logger.error("Dedicated Tunnel Bridge Creation Status {}, for networkBrTun {}, networkPatchInt {}, networkPatchTun {}", status.toString(), networkBrTun, networkPatchInt, networkPatchTun);
            logger.debug("Dedicated Tunnel Bridge Creation Status {}, for networkBrTun {}, networkPatchInt {}, networkPatchTun {}", status.toString(), networkBrTun, networkPatchInt, networkPatchTun);

            // Adding patch ports for br-tun and brtun-XY (tenant network bridge)
            String brTun = AdminConfigManager.getManager().getTunnelBridgeName();
            String brTunUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, brTun);

            networkBrTunUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, networkBrTun);

            String networkPatchTunCPU = getPatchToDedicatedTunFromCPUForNetwork(network);
            String networkPatchCPUTun = getPatchToCPUFromDedicatedTunForNetwork(network);

//            status = InternalNetworkManager.getManager().addPatchPort(node, networkBrTunUUID, networkPatchCPUTun, networkPatchTunCPU);
//            if (!status.isSuccess())
//                logger.error("Adding patch port failed {}, for networkBrTunUUID {}, networkPatchCPUTun {}, networkPatchTunCPU {}", status.toString(), networkBrTunUUID, networkPatchCPUTun, networkPatchTunCPU);
//
//            status = InternalNetworkManager.getManager().addPatchPort(node, brTunUUID, networkPatchTunCPU, networkPatchCPUTun);
//            if (!status.isSuccess())
//                logger.error("Adding patch port failed {}, for brTunUUID {}, networkPatchTunCPU {}, networkPatchCPUTun {}", status.toString(), brTunUUID, networkPatchTunCPU, networkPatchCPUTun);

            // adding external interface to dedicated tenant tunnel bridge (THIS WILL MAKE LOOP)
//            status = addExInternalInterfaceToBridge(node, networkBrTunUUID, externalPortName);
//            if (!status.isSuccess())
//                logger.error("Adding external port failed {}, for networkBrTunUUID {}, externalPortName {}", status.toString(), networkBrTunUUID, externalPortName);

            // adding external interface to br-tun to avoid loop
            status = addExInternalInterfaceToBridge(node, brTunUUID, externalPortName);
            if (!status.isSuccess())
                logger.error("Adding external port failed {}, for brTunUUID {}, externalPortName {}", status.toString(), brTunUUID, externalPortName);


        } else {
            logger.debug("Dedicated Tunnel Bridge already exists {}", networkBrIntUUID);
        }

    }

    public String getExInfNameForNetwork(NeutronNetwork network){
        return ("ex-"+network.getNetworkName()).substring(0, Math.min(11, 3+network.getNetworkName().length()));
    }
    public String getExInfPrefix(){
        return "ex-";
    }

    /*
     * TODO: Move these to AdminConfigurationManager
     * Linux bridge length is 14, and for OpenStack compatibility we restrict it to 11
     */
    public String getDedicatedTunBridgeNameForNetwork(NeutronNetwork network){
        return ("brtun-"+network.getNetworkUUID()).substring(0, 11);
    }

    public String getDedicatedTunBridgeNameForNetwork(String networkUUID){
        return ("brtun-"+networkUUID).substring(0, 11);
    }

    /*
     * TODO: Move these to AdminConfigurationManager
     * Linux bridge length is 14, and for OpenStack compatibility we restrict it to 11
     */
    private String getDedicatedIntBridgeNameForNetwork(NeutronNetwork network){
        return ("brint-"+network.getNetworkUUID()).substring(0, 11);
    }

    public String getDedicatedIntBridgeNameForNetwork(String networkUUID){
        return ("brint-"+networkUUID).substring(0, 11);
    }

    private String getPatchToDedicatedIntForNetwork(NeutronNetwork network){
        return ("p-int-"+network.getNetworkUUID()).substring(0, 11);
    }

    public String getPatchToDedicatedIntForNetwork(String networkUUID){
        return ("p-int-"+networkUUID).substring(0, 11);
    }

    private String getPatchToDedicatedTunForNetwork(NeutronNetwork network){
        return ("p-tun-"+network.getNetworkUUID()).substring(0, 11);
    }

    private String getPatchToDedicatedTunFromCPUForNetwork(NeutronNetwork network){
        return ("p-t-c-"+network.getNetworkUUID()).substring(0, 11);
    }

    private String getPatchToCPUFromDedicatedTunForNetwork(NeutronNetwork network){
        return ("p-c-t-"+network.getNetworkUUID()).substring(0, 11);
    }

    public void prepareTenantNetworkBridges(Node node, NeutronNetwork network) {
// FIXME: For the sake of simplicity not using    network.isDedicatedBridges()
//        if (!network.isDedicatedBridges() && !network.getNetworkName().toLowerCase().startsWith("secnet")) {
        if (!network.getNetworkName().toLowerCase().startsWith("secnet")) {
            logger.debug("prepareTenantNetworkBridges Network {} doesn't require dedicated bridges. Skipping.", network);
            return;
        }
        try {
            this.createDedicatedBridgesForNetwork(node, network);
        } catch (Exception e) {
            logger.error("Error creating dedicated tenant network bridges "+node.toString(), e);
        }
        // FIXME: Initialized flow accordingly
        // ProviderNetworkManager.getManager().initializeFlowRules(node);
    }

    /**
     * Check if network requires dedicated bridges and
     * move port to the network integration bridge if needed
     * @param intf
     * @param port
     * @param portUUID
     * @param network
     * @param node
     */
    public void adjustPortBridgeAttachment(Node node, NeutronNetwork network, String portUUID, Interface intf){
      logger.debug("adjustPortBridgeAttachment node {} network {} portUUID {} interface {}", node, network.getNetworkName(), portUUID, intf.getName());
        // FIXME: For the sake of simplicity not using    network.isDedicatedBridges()
        // if (!network.isDedicatedBridges() && !network.getNetworkName().toLowerCase().startsWith("secnet")) {
      if (!network.getNetworkName().toLowerCase().startsWith("secnet")) {
          logger.debug("adjustPortBridgeAttachment: Network={} doesn't require port adjustment. Skipping.", network.getNetworkName());
          return;
      }
      // if port is (attached to br-int && belongs to this network && type is tap) move it
      // 1- Retrieve br-int ports and check if this port is there
      // 2- Port already belongs to the network, otherwise we won't ended up here
      // 3- All ports in br-int are of tap type
      String brIntUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, AdminConfigManager.getManager().getIntegrationBridgeName());
      if (brIntUUID == null) {
          logger.error("adjustPortBridgeAttachment: Failed to retrieve Integration Bridge in Node {}", node);
          return;
      }

      String networkIntBrName = getDedicatedIntBridgeNameForNetwork(network);
      String networkIntBrUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, networkIntBrName);

      try {
          boolean portInIntBr = isPortPresentInBridge(node, brIntUUID, portUUID);
          boolean portInNetworkIntBr = isPortPresentInBridge(node, networkIntBrUUID, portUUID);

          if (portInIntBr && !portInNetworkIntBr){
//              Status status = attachPortToBridge(node, networkIntBrUUID, portUUID);
//              if (!status.isSuccess()){
//                  logger.error("adjustPortBridgeAttachment: attachPortToBridge was not successful {}", status.toString());
//              } else{
//                  logger.debug("adjustPortBridgeAttachment: attachPortToBridge was successful {}", status.toString());
//              }
//
//              status = detachPortFromBridge(node, brIntUUID, portUUID);
//              if (!status.isSuccess()){
//                  logger.error("adjustPortBridgeAttachment: detachPortFromBridge was not successful {}", status.toString());
//              } else{
//                  logger.debug("adjustPortBridgeAttachment: detachPortFromBridge was successful {}", status.toString());
//              }
              Status status = updatePortBridge(node, brIntUUID, networkIntBrUUID, portUUID, intf);
              if (!status.isSuccess()){
                  logger.error("adjustPortBridgeAttachment: updatePortBridge was not successful {} on node {}", status.toString(), node);
                  return;
              } else{
                  logger.info("adjustPortBridgeAttachment: updatePortBridge for portUUID {} interface {} network {} was successful {} on node {}", portUUID, intf, network, status.toString(), node);
              }
          } else {
              logger.debug("adjustPortBridgeAttachment: Skipping adjustment, port isn't attached to int bridge, or is also attached to network int br, or network int bridge doesn't exist");
              logger.debug("adjustPortBridgeAttachment: node {}, portInIntBr {}, portInNetworkIntBr {}", node, portInIntBr, portInNetworkIntBr);
              if (!portInIntBr && !portInNetworkIntBr){
                  logger.error("adjustPortBridgeAttachment: port on node {} is not in br-int {} nor brint-xy {}", node, brIntUUID, networkIntBrName);
              }
          }
      } catch (Exception e) {
          logger.error("adjustPortBridgeAttachment: Can not adjust port bridge attachment", e);
      }

    }

    /**
     * Move a port from a bridge (brIntUUID) to another bridge (networkIntBrUUID),
     *  by deleting from the first one and reinserting to the second one.
     * @param node
     * @param brIntUUID
     * @param networkIntBrUUID
     * @param portUUID
     * @param intf
     * @return Status
     * @throws Exception
     */
    private Status updatePortBridge(Node node, String brIntUUID, String networkIntBrUUID, String portUUID, Interface intf) throws Exception {
        logger.debug("updatePortBridge Node {}, NetworkIntBrUUID {}, portUUID {}, Interface {}", node, networkIntBrUUID, portUUID, intf);
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        // TODO Might be better to re-use the port object , received in SouthBoundHandler
        Port port = (Port)ovsdbTable.getRow(node, Port.NAME.getName(), portUUID);
        Status status = ovsdbTable.deleteRow(node, Port.NAME.getName(), portUUID);
        if (!status.isSuccess()){
            logger.error("updatePortBridge: Port delete failed for Node {}, Port {}, Status {}", node, port, status.toString());
            return status;
        } else {
            logger.debug("updatePortBridge: Port delete succeed for Node {}, Port {}, Status {}", node, port, status.toString());
        }

        Port newPort = new Port();
        newPort.setExternal_ids(port.getExternal_ids());
//        newPort.setMac(port.getMac());
        newPort.setName(port.getName());
        newPort.setOther_config(port.getOther_config());
        newPort.setTag(port.getTag());
        newPort.setTrunks(port.getTrunks());
        StatusWithUuid statusWithUuid = ovsdbTable.insertRow(node, Port.NAME.getName(), networkIntBrUUID, newPort);

        if (!statusWithUuid.isSuccess()){
            logger.error("updatePortBridge: Port insert failed for Node {}, Bridge {}, Port {}, Status {}", node, networkIntBrUUID, newPort, statusWithUuid.toString());
            return statusWithUuid;
        } else {
            logger.debug("updatePortBridge: Port insert succeed for Node {}, Bridge {}, Port {}, Status {}", node, networkIntBrUUID, newPort, statusWithUuid.toString());
        }

        String newPortUUID = statusWithUuid.getUuid().toString();

        String newInterfaceUUID = null;
        int timeout = 6;
        while ((newInterfaceUUID == null) && (timeout > 0)) {
            newPort = (Port)ovsdbTable.getRow(node, Port.NAME.getName(), newPortUUID);
            OvsDBSet<UUID> interfaces = newPort.getInterfaces();
            if (interfaces == null || interfaces.size() == 0) {
                // Wait for the OVSDB update to sync up the Local cache.
                Thread.sleep(500);
                timeout--;
                continue;
            }
            newInterfaceUUID = interfaces.toArray()[0].toString();
        }

        if (newInterfaceUUID == null) {
            logger.error("updatePortBridge: newInterfaceUUID is null for newPortUUID {}", newPortUUID);
            return new Status(StatusCode.INTERNALERROR);
        }

        Interface newInterface = new Interface();
        newInterface.setExternal_ids(intf.getExternal_ids());
        if (newInterface.getExternal_ids() == null || newInterface.getExternal_ids().size() == 0){
            logger.error("updatePortBridge External_ids is missing for received interface {}", intf);
        }
        status = ovsdbTable.updateRow(node, Interface.NAME.getName(), newPortUUID, newInterfaceUUID, newInterface);
        return status;
    }

    private Status addExInternalInterfaceToBridge(Node node, String bridgeUUID, String exPortName) throws Exception{
        logger.debug("addExInternalInterfaceToBridge: Node {}, Bridge {}, RequestedPortName {}", node, bridgeUUID, exPortName);

        StatusWithUuid statusWithUuid = addPortToBridge(node, bridgeUUID, exPortName);
        if (!statusWithUuid.isSuccess()){
            logger.error("addExInternalInterfaceToBridge: Port insert failed for Node {}, Bridge {}, Port {}, Status {}", node, bridgeUUID, exPortName, statusWithUuid.toString());
            return statusWithUuid;
        } else {
            logger.debug("addExInternalInterfaceToBridge: Port insert succeed for Node {}, Bridge {}, Port {}, Status {}", node, bridgeUUID, exPortName, statusWithUuid.toString());
        }
        String exPortUUID = statusWithUuid.getUuid().toString();
        String exInterfaceUUID = getAddedInterfaceUUID(node, exPortUUID);
        if (exInterfaceUUID == null) {
            logger.error("addExInternalInterfaceToBridge: exInterfaceUUID is null for exPortUUID {}", exPortUUID);
            return new Status(StatusCode.INTERNALERROR);
        }
        Status status = setInterfaceInternalType(node, exPortUUID, exInterfaceUUID);
        return status;
    }

    private Status setInterfaceInternalType(Node node, String portUUID, String interfaceUUID) {
        logger.debug("setInterfaceInternalType: Node {}, portUUID {}, interfaceUUID {}", node, portUUID, interfaceUUID);
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        Interface newInterface = new Interface();
        newInterface.setType("internal");
        Status status = ovsdbTable.updateRow(node, Interface.NAME.getName(), portUUID, interfaceUUID, newInterface);
        return status;
    }

    public String getAddedInterfaceUUID(Node node, String addedPortUUID) throws Exception {
        logger.debug("getAddedInterfaceUUID: Node {}, addedPortUUID {}", node, addedPortUUID);

        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        String newInterfaceUUID = null;
        Port addedPort = null;
        int timeout = 6;
        while ((newInterfaceUUID == null) && (timeout > 0)) {
            addedPort = (Port)ovsdbTable.getRow(node, Port.NAME.getName(), addedPortUUID);
            OvsDBSet<UUID> interfaces = addedPort.getInterfaces();
            if (interfaces == null || interfaces.size() == 0) {
                Thread.sleep(500);
                timeout--;
                continue;
            }
            newInterfaceUUID = interfaces.toArray()[0].toString();
        }

        if (newInterfaceUUID == null) {
            logger.error("getAddedInterfaceUUID: TIMEOUT newInterfaceUUID is null for addedPortUUID {}", addedPortUUID);
        }
        return newInterfaceUUID;
    }

    public StatusWithUuid addPortToBridge(Node node, String bridgeUUID, String portName) {
        logger.debug("addPortToBridge: Node {}, BridgeUUID {}, exPortName {}", node, bridgeUUID, portName);

        Port port = new Port();
        port.setName(portName);
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        StatusWithUuid statusWithUuid = ovsdbTable.insertRow(node, Port.NAME.getName(), bridgeUUID, port);
        return statusWithUuid;
    }

    private Status attachPortToBridge(Node node, String networkIntBrUUID, String portUUID) throws Exception {
        logger.debug("attachPortToBridge Node {}, NetworkIntBrUUID {}, PortUUID {}", node, networkIntBrUUID, portUUID);

        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        Bridge networkIntBr = (Bridge)ovsdbTable.getRow(node, Bridge.NAME.getName(), networkIntBrUUID);

        if(networkIntBr == null){
            logger.error("attachPortToBridge: networkIntBr is null, UUID: {}", networkIntBrUUID);
            return new Status(StatusCode.NOTFOUND);
        }
        OvsDBSet<UUID> ports = networkIntBr.getPorts();
        logger.debug("attachPortToBridge: Number of ports in networkIntBr before updating {} is {}", networkIntBr, ports.size());
        ports.add(new UUID(portUUID));

        Bridge newNetworkIntBr = new Bridge();
        newNetworkIntBr.setPorts(ports);
        Status status = ovsdbTable.updateRow(node, Bridge.NAME.getName(), null, networkIntBrUUID, newNetworkIntBr);
        //TODO For debugging purpose only, must be removed
        Bridge newNetworkIntBr2 = (Bridge)ovsdbTable.getRow(node, Bridge.NAME.getName(), networkIntBrUUID);
        logger.debug("attachPortToBridge: Number of ports in networkIntBr after updating {} is {}", newNetworkIntBr2, newNetworkIntBr2.getPorts().size());
        return status;
    }

    private Status detachPortFromBridge(Node node, String brUUID, String portUUID) throws Exception{
        logger.debug("detachPortFromBridge Node {}, brUUID {}, PortUUID {}", node, brUUID, portUUID);

        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        Bridge intBr = (Bridge)ovsdbTable.getRow(node, Bridge.NAME.getName(), brUUID);

        if (intBr == null){
            logger.error("detachPortFromBridge: bridge is null, UUID: {}", brUUID);
            return new Status(StatusCode.NOTFOUND);
        }

        OvsDBSet<UUID> ports = intBr.getPorts();
        UUID port = new UUID(portUUID);
        if (ports.contains(port)){
            ports.remove(port);
        } else {
            logger.error("detachPortFromBridge: Port {} is not in Bridge {} ports.", portUUID, brUUID);
        }
        Bridge newBr = new Bridge();
        newBr.setPorts(ports);
        Status status = ovsdbTable.updateRow(node, Bridge.NAME.getName(), null, brUUID, newBr);
        return status;
    }

    private boolean isPortPresentInBridge(Node node, String bridgeUUID, String portUUID) throws Exception{
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        Bridge bridge = (Bridge)ovsdbTable.getRow(node, Bridge.NAME.getName(), bridgeUUID);
        if (bridge != null) {
            Set<UUID> ports = bridge.getPorts();
            for (UUID portID : ports) {
                if (portID.toString().equalsIgnoreCase(portUUID)) return true;
            }
        } else {
            logger.error("isPortPresentInBridge Bridge {} doesn't exist", bridgeUUID);
        }
        return false;
    }

}
