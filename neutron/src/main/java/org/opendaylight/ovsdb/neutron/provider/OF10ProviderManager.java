/*
 * Copyright (C) 2013 Red Hat, Inc.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 *
 * Authors : Madhu Venugopal, Brent Salisbury
 */
package org.opendaylight.ovsdb.neutron.provider;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opendaylight.controller.forwardingrulesmanager.FlowConfig;
import org.opendaylight.controller.forwardingrulesmanager.IForwardingRulesManager;
import org.opendaylight.controller.networkconfig.neutron.NeutronNetwork;
import org.opendaylight.controller.sal.action.ActionType;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.utils.EtherTypes;
import org.opendaylight.controller.sal.utils.HexEncode;
import org.opendaylight.controller.sal.utils.ServiceHelper;
import org.opendaylight.controller.sal.utils.Status;
import org.opendaylight.controller.sal.utils.StatusCode;
import org.opendaylight.controller.switchmanager.ISwitchManager;
import org.opendaylight.controller.switchmanager.Switch;
import org.opendaylight.ovsdb.lib.notation.OvsDBMap;
import org.opendaylight.ovsdb.lib.notation.OvsDBSet;
import org.opendaylight.ovsdb.lib.notation.UUID;
import org.opendaylight.ovsdb.lib.table.Bridge;
import org.opendaylight.ovsdb.lib.table.Interface;
import org.opendaylight.ovsdb.lib.table.Port;
import org.opendaylight.ovsdb.lib.table.internal.Table;
import org.opendaylight.ovsdb.neutron.AdminConfigManager;
import org.opendaylight.ovsdb.neutron.InternalNetworkManager;
import org.opendaylight.ovsdb.neutron.TenantNetworkManager;
import org.opendaylight.ovsdb.plugin.IConnectionServiceInternal;
import org.opendaylight.ovsdb.plugin.OVSDBConfigService;
import org.opendaylight.ovsdb.plugin.StatusWithUuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class OF10ProviderManager extends ProviderNetworkManager {
    private static final Logger logger = LoggerFactory.getLogger(OF10ProviderManager.class);
    private static final int INGRESS_TUNNEL_FLOW_PRIORITY = 100;
    private static final int EGRESS_TUNNEL_FLOW_PRIORITY = 100;
    private static final int FLOOD_TUNNEL_FLOW_PRIORITY = 1;

    @Override
    public boolean hasPerTenantTunneling() {
        return true;
    }

    private Status getTunnelReadinessStatus (Node node, String tunnelKey) {
        InetAddress srcTunnelEndPoint = AdminConfigManager.getManager().getTunnelEndPoint(node);
        if (srcTunnelEndPoint == null) {
            logger.error("Tunnel Endpoint not configured for Node {}", node);
            return new Status(StatusCode.NOTFOUND, "Tunnel Endpoint not configured for "+ node);
        }

        if (!InternalNetworkManager.getManager().isInternalNetworkOverlayReady(node)) {
            logger.warn("{} is not Overlay ready. It might be an OpenStack Controller Node", node);
            return new Status(StatusCode.NOTACCEPTABLE, node+" is not Overlay ready");
        }

        if (!TenantNetworkManager.getManager().isTenantNetworkPresentInNode(node, tunnelKey)) {
            logger.debug(node+" has no VM corresponding to segment "+ tunnelKey);
            return new Status(StatusCode.NOTACCEPTABLE, node+" has no VM corresponding to segment "+ tunnelKey);
        }
        return new Status(StatusCode.SUCCESS);
    }

    private Status getDedicatedNetworkTunnelReadinessStatus (Node node, NeutronNetwork network, String tunnelKey) {
        logger.debug("getDedicatedNetworkTunnelReadinessStatus Node {}, Network {}, tunnelKey {}", node, network, tunnelKey);
        InetAddress tunnelEndPoint = AdminConfigManager.getManager().getDedicatedNetworkTunnelEndPoint(node, network);
        if (tunnelEndPoint == null) {
            logger.error("Dedicated Network Tunnel Endpoint not configured for Node {} Network {}", node, network.getNetworkName()+" "+network.getNetworkUUID());
            return new Status(StatusCode.NOTFOUND, "Tunnel Endpoint not configured for Node: "+ node+", Network: "+network);
        }
        // Not really important
        if (!InternalNetworkManager.getManager().isInternalNetworkOverlayReady(node)) {
            logger.error(node+" is not Overlay ready");
            return new Status(StatusCode.NOTACCEPTABLE, node+" is not Overlay ready");
        }
        if (!TenantNetworkManager.getManager().isTenantNetworkPresentInNode(node, tunnelKey)) {
            logger.debug(node+" has no VM corresponding to segment "+ tunnelKey);
            return new Status(StatusCode.NOTACCEPTABLE, node+" has no VM corresponding to segment "+ tunnelKey);
        }
        logger.debug("getDedicatedNetworkTunnelReadinessStatus True: Node {} Network {} tunnelKey {}", node, network.getNetworkName(), tunnelKey);
        return new Status(StatusCode.SUCCESS);

    }
    /**
     * Program OF1.0 Flow rules on br-tun on the ingress direction from the network towards the br-int.
     * The logic is to simply match on the incoming tunnel OF-Port (which carries the TenantNetwork GRE-Key)
     * and rewrite the Corresponding internal Vlan and pass it on to br-int via the patch port.
     */
    private void programLocalIngressTunnelBridgeRules(Node node, int tunnelOFPort, int internalVlan, int patchPort) {
        String brIntId = InternalNetworkManager.getManager().getInternalBridgeUUID(node, AdminConfigManager.getManager().getTunnelBridgeName());
        if (brIntId == null) {
            logger.error("Failed to initialize LocalIngressTunnelBridge Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), brIntId);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepMatch"+tunnelOFPort+""+internalVlan;
            FlowConfig flow = new FlowConfig();
            flow.setName(flowName);
            flow.setNode(ofNode);
            flow.setInstallInHw(true);
            flow.setPriority(INGRESS_TUNNEL_FLOW_PRIORITY+"");
            flow.setIngressPort(tunnelOFPort+"");
            List<String> actions = new ArrayList<String>();
            actions.add(ActionType.SET_VLAN_ID+"="+internalVlan);
            actions.add(ActionType.OUTPUT.toString()+"="+patchPort);
            flow.setActions(actions);
            Status status = this.addStaticFlow(ofNode, flow);
            logger.debug("Local Ingress Flow Programming Status {} for Flow {} on {} / {}", status, flow, ofNode, node);
        } catch (Exception e) {
            logger.error("Failed to initialize LocalIngressTunnelBridge Flow Rules for {}", node, e);
        }
    }

    private void removeLocalIngressTunnelBridgeRules(Node node, int tunnelOFPort, int internalVlan, int patchPort) {
        String brIntId = InternalNetworkManager.getManager().getInternalBridgeUUID(node, AdminConfigManager.getManager().getTunnelBridgeName());
        if (brIntId == null) {
            logger.error("Failed to remove Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), brIntId);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepMatch"+tunnelOFPort+""+internalVlan;

            Status status = this.deleteStaticFlow(ofNode, flowName);
            logger.debug("Local Ingress Flow Removal Status {} for Flow {} on {} / {}", status, flowName, ofNode, node);
        } catch (Exception e) {
            logger.error("Failed to Remove Flow Rules for {}", node, e);
        }
    }

    /**
     * Program OF1.0 Flow rules on brtun-xy on the ingress direction from the network towards the brint-xy.
     * The logic is to simply match on the incoming tunnel OF-Port (which carries the TenantNetwork GRE-Key)
     * and rewrite the Corresponding internal Vlan and pass it on to brint-xy via the patch port.
     */
    private void programLocalIngressDedicatedTunnelBridgeRules(Node node, int networkTunnelOFPort, int internalVlan, int networkPatchPort, String networkUUID) {
        String networkBrTunName = TenantNetworkManager.getManager().getDedicatedTunBridgeNameForNetwork(networkUUID);
        String networkBrTunUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, networkBrTunName);
        if (networkBrTunUUID == null) {
            logger.error("Failed to initialize LocalIngressDedicatedTunnelBridge Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), networkBrTunUUID);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepMatch"+networkTunnelOFPort+""+internalVlan;
            FlowConfig flow = new FlowConfig();
            flow.setName(flowName);
            flow.setNode(ofNode);
            flow.setInstallInHw(true);
            flow.setPriority(INGRESS_TUNNEL_FLOW_PRIORITY+"");
            flow.setIngressPort(networkTunnelOFPort+"");
            List<String> actions = new ArrayList<String>();
            actions.add(ActionType.SET_VLAN_ID+"="+internalVlan);
            actions.add(ActionType.OUTPUT.toString()+"="+networkPatchPort);
            flow.setActions(actions);
            Status status = this.addStaticFlow(ofNode, flow);
            logger.debug("Dedicated Local Ingress Flow Programming Status {} for Flow {} on {} / {}", status, flow, ofNode, node);
        } catch (Exception e) {
            logger.error("Failed to initialize LocalIngressDedicatedTunnelBridge Flow Rules for {}", node, e);
        }

    }

    /**
     * Program OF1.0 Flow rules on br-tun on the remote Node on its egress direction towards the overlay network
     * for a VM (with the attachedMac).
     * The logic is to simply match on the incoming vlan, mac from the patch-port connected to br-int (patch-int)
     * and output the traffic to the appropriate GRE Tunnel (which carries the GRE-Key for that Tenant Network).
     * Also perform the Strip-Vlan action.
     */
    private void programRemoteEgressTunnelBridgeRules(Node node, int patchPort, String attachedMac,
            int internalVlan, int tunnelOFPort) {
        String brIntId = InternalNetworkManager.getManager().getInternalBridgeUUID(node, AdminConfigManager.getManager().getTunnelBridgeName());
        if (brIntId == null) {
            logger.error("Failed to initialize RemoteEgressTunnelBridge Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), brIntId);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepMatch"+tunnelOFPort+""+internalVlan+""+HexEncode.stringToLong(attachedMac);
            FlowConfig flow = new FlowConfig();
            flow.setName(flowName);
            flow.setNode(ofNode);
            flow.setInstallInHw(true);
            flow.setPriority(EGRESS_TUNNEL_FLOW_PRIORITY+"");
            flow.setDstMac(attachedMac);
            flow.setIngressPort(patchPort+"");
            flow.setVlanId(internalVlan+"");
            List<String> actions = new ArrayList<String>();
            actions.add(ActionType.POP_VLAN.toString());
            actions.add(ActionType.OUTPUT.toString()+"="+tunnelOFPort);
            flow.setActions(actions);
            Status status = this.addStaticFlow(ofNode, flow);
            logger.debug("Remote Egress Flow Programming Status {} for Flow {} on {} / {}", status, flow, ofNode, node);
        } catch (Exception e) {
            logger.error("Failed to initialize RemoteEgressTunnelBridge Flow Rules for {}", node, e);
        }
    }

    /**
     * Program OF1.0 Flow rules on br-tun on the local Node on its egress direction towards the overlay network.
     * This will retrieve all attachedMacs for the tenant network's VMs and other interfaces (e.g. tap interface in
     * the network namespace in the networking node), and program local br-tun to use the proper tunnel, instead
     * of sending over all tenant tunnels.
     * The logic is to simply match on the incoming vlan, mac from the patch-port connected to br-int (p-int)
     * and output the traffic to the appropriate GRE Tunnel (which carries the GRE-Key for that Tenant Network).
     * Also perform the Strip-Vlan action.
     */
    private void programLocalEgressTunnelBridgeRules(Node localNode, Node remoteNode, int patchPort, int internalVlan, int tunnelOFPort, String networkUUID) {
        logger.debug("programLocalEgressTunnelBridgeRules: localNode {}, remoteNode {}, patchPort {}, internalVlan {}, tunnelOFPort {}, networkUUID {}",
                localNode, remoteNode, patchPort, internalVlan, tunnelOFPort, networkUUID);
        String brTunName = AdminConfigManager.getManager().getTunnelBridgeName();
        String brIntName = AdminConfigManager.getManager().getIntegrationBridgeName();
        String localBrTunUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(localNode, brTunName);
        String remoteBrIntUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(remoteNode, brIntName);

        if (localBrTunUUID == null) {
            logger.error("Failed to initialize LocalEgressTunnelBridge Flow Rules for {} localBrTunUUID is null", localNode);
            return;
        }
        if (remoteBrIntUUID == null) {
            logger.error("Failed to initialize LocalEgressTunnelBridge Flow Rules for {} remoteBrIntUUID is null", remoteNode);
            return;
        }

        int remoteInternalVlan = TenantNetworkManager.getManager().getInternalVlan(remoteNode, networkUUID);
        if (remoteInternalVlan == 0) {
            logger.debug("programLocalEgressTunnelBridgeRules: No RemoteInternalVlan provisioned for Tenant Network {} on Node {}",networkUUID, remoteNode);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge remoteBrInt = (Bridge) ovsdbTable.getRow(remoteNode, Bridge.NAME.getName(), remoteBrIntUUID);
            OvsDBSet<UUID> ports = remoteBrInt.getPorts();
            Port port = null;
            Interface remoteIntf= null;
            Set<String> attachedMacs = new HashSet<String>();
            for (UUID portUUID : ports) {
                port = (Port) ovsdbTable.getRow(remoteNode, Port.NAME.getName(), portUUID.toString());
                if (port.getTag() == null || port.getTag().size() == 0){
                    logger.debug("programLocalEgressTunnelBridgeRules: port {} doesn't have any tags in br-int on Node {}", port, remoteNode);
                    continue;
                }
                int portTag = port.getTag().toArray(new BigInteger[0])[0].intValue();
                if (portTag != remoteInternalVlan){
                    if (portTag == 0) {
                        logger.debug("programLocalEgressTunnelBridgeRules: port {} is not tagged in br-int on Node {}", port, remoteNode);
                    } else {
                        logger.debug("programLocalEgressTunnelBridgeRules: port {} tag {} doesn't belong to network {} in br-int on Node {}", port, portTag, networkUUID, remoteNode);
                    }
                    continue;
                }
                OvsDBSet<UUID> interfaces = port.getInterfaces();
                for (UUID interfaceUUID : interfaces) {
                    remoteIntf = (Interface) ovsdbTable.getRow(remoteNode, Interface.NAME.getName(), interfaceUUID.toString());
                    Map<String, String> externalIds = remoteIntf.getExternal_ids();

                    if (externalIds == null) {
                        logger.debug("No external_ids seen in {}", remoteIntf);
                        continue;
                    }

                    String attachedMac = externalIds.get(TenantNetworkManager.EXTERNAL_ID_VM_MAC);
                    if (attachedMac == null) {
                        logger.debug("No AttachedMac seen in {}", remoteIntf);
                        continue;
                    }
                    logger.debug("programLocalEgressTunnelBridgeRules: adding mac {} for interface {} in network {} from brint {} on remoteNode {}", attachedMac, remoteIntf, networkUUID, remoteBrInt, remoteNode);
                    attachedMacs.add(attachedMac);
                }
            }
            logger.debug("programLocalEgressTunnelBridgeRules: retrieved macs {} for network {} on remoteNode {}", attachedMacs, networkUUID, remoteNode);

            Bridge localBrTun = (Bridge) ovsdbTable.getRow(localNode, Bridge.NAME.getName(), localBrTunUUID);
            Set<String> dpids = localBrTun.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);

            Status status = null;
            for (String attachedMac : attachedMacs) {
                String flowName = "TepMatch"+tunnelOFPort+""+internalVlan+""+HexEncode.stringToLong(attachedMac);
                logger.debug("programLocalEgressTunnelBridgeRules: programming flowname {} with mac {} for ofNode {} on Node {}", flowName, attachedMac, ofNode, localNode);
                FlowConfig flow = new FlowConfig();
                flow.setName(flowName);
                flow.setNode(ofNode);
                flow.setInstallInHw(true);
                flow.setPriority(EGRESS_TUNNEL_FLOW_PRIORITY+"");
                flow.setDstMac(attachedMac);
                flow.setIngressPort(patchPort+"");
                flow.setVlanId(internalVlan+"");
                List<String> actions = new ArrayList<String>();
                actions.add(ActionType.POP_VLAN.toString());
                actions.add(ActionType.OUTPUT.toString()+"="+tunnelOFPort);
                flow.setActions(actions);
                status = this.addStaticFlow(ofNode, flow);
                logger.debug("programLocalEgressTunnelBridgeRules: status {} for flow {} in Node {}", status, flow, ofNode);
            }


        } catch (Exception e) {
            logger.error("Failed to initialize LocalEgressTunnelBridgeRules Flow Rules for localNode {}, remoteNode {}, exception {}", localNode, remoteNode, e);
        }
    }


    /**
     * Program OF1.0 Flow rules on brtun-xy on the remote Node on its egress direction towards the overlay network
     * for a VM (with the attachedMac).
     * The logic is to simply match on the incoming vlan, mac from the patch-port connected to brint-xy (p-int-xy)
     * and output the traffic to the appropriate GRE Tunnel (which carries the GRE-Key for that Tenant Network).
     * Also perform the Strip-Vlan action.
     */
    private void programRemoteEgressDedicatedTunnelBridgeRules(Node node, int networkPatchPort, String attachedMac, int internalVlan, int networkTunnelOFPort, String networkUUID) {
        String networkBrTunName = TenantNetworkManager.getManager().getDedicatedTunBridgeNameForNetwork(networkUUID);
        String networkBrTunUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, networkBrTunName);

        if (networkBrTunUUID == null) {
            logger.error("Failed to initialize RemoteEgressDedicatedTunnelBridge Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), networkBrTunUUID);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepMatch"+networkTunnelOFPort+""+internalVlan+""+HexEncode.stringToLong(attachedMac);
            FlowConfig flow = new FlowConfig();
            flow.setName(flowName);
            flow.setNode(ofNode);
            flow.setInstallInHw(true);
            flow.setPriority(EGRESS_TUNNEL_FLOW_PRIORITY+"");
            flow.setDstMac(attachedMac);
            flow.setIngressPort(networkPatchPort+"");
            flow.setVlanId(internalVlan+"");
            List<String> actions = new ArrayList<String>();
            actions.add(ActionType.POP_VLAN.toString());
            actions.add(ActionType.OUTPUT.toString()+"="+networkTunnelOFPort);
            flow.setActions(actions);
            Status status = this.addStaticFlow(ofNode, flow);
            logger.debug("Remote Egress Flow Programming Status {} for Flow {} on {} / {}", status, flow, ofNode, node);
        } catch (Exception e) {
            logger.error("Failed to initialize RemoteEgressTunnelBridge Flow Rules for {}", node, e);
        }
    }

    /**
     * Program OF1.0 Flow rules on brtun-xy on the local Node on its egress direction towards the overlay network.
     * This will retrieve all attachedMacs for the tenant network's VMs and other interfaces (e.g. tap interface in
     * the network namespace in the networking node), and program local brtun-xy to use the proper tunnel, instead
     * of sending over all tenant tunnels.
     * The logic is to simply match on the incoming vlan, mac from the patch-port connected to brint-xy (p-int-xy)
     * and output the traffic to the appropriate GRE Tunnel (which carries the GRE-Key for that Tenant Network).
     * Also perform the Strip-Vlan action.
     */
    private void programLocalEgressDedicatedTunnelBridgeRules(Node localNode, Node remoteNode, int networkPatchPort, int internalVlan, int networkTunnelOFPort, String networkUUID) {
        logger.debug("programLocalEgressDedicatedTunnelBridgeRules: localNode {}, remoteNode {}, networkPatchPort {}, internalVlan {}, networkTunnelOFPort {}, networkUUID {}",
                localNode, remoteNode, networkPatchPort, internalVlan, networkTunnelOFPort, networkUUID);
        String networkBrTunName = TenantNetworkManager.getManager().getDedicatedTunBridgeNameForNetwork(networkUUID);
        String networkBrIntName = TenantNetworkManager.getManager().getDedicatedIntBridgeNameForNetwork(networkUUID);
        String localNetworkBrTunUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(localNode, networkBrTunName);
        String remoteNetworkBrIntUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(remoteNode, networkBrIntName);

        if (localNetworkBrTunUUID == null) {
            logger.error("Failed to initialize LocalEgressDedicatedTunnelBridge Flow Rules for {} localNetworkBrTunUUID is null", localNode);
            return;
        }
        if (remoteNetworkBrIntUUID == null) {
            logger.error("Failed to initialize LocalEgressDedicatedTunnelBridge Flow Rules for {} remoteNetworkBrIntUUID is null", remoteNode);
            return;
        }

        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge remoteBrInt = (Bridge) ovsdbTable.getRow(remoteNode, Bridge.NAME.getName(), remoteNetworkBrIntUUID);
            OvsDBSet<UUID> ports = remoteBrInt.getPorts();
            Port port = null;
            Interface remoteIntf= null;
            Set<String> attachedMacs = new HashSet<String>();
            for (UUID portUUID : ports) {
                port = (Port) ovsdbTable.getRow(remoteNode, Port.NAME.getName(), portUUID.toString());
                OvsDBSet<UUID> interfaces = port.getInterfaces();
                for (UUID interfaceUUID : interfaces) {
                    remoteIntf = (Interface) ovsdbTable.getRow(remoteNode, Interface.NAME.getName(), interfaceUUID.toString());
                    remoteIntf.getExternal_ids();
                    Map<String, String> externalIds = remoteIntf.getExternal_ids();

                    if (externalIds == null) {
                        logger.debug("No external_ids seen in {}", remoteIntf);
                        continue;
                    }

                    String attachedMac = externalIds.get(TenantNetworkManager.EXTERNAL_ID_VM_MAC);
                    if (attachedMac == null) {
                        logger.debug("No AttachedMac seen in {}", remoteIntf);
                        continue;
                    }
                    logger.debug("programLocalEgressDedicatedTunnelBridgeRules: adding mac {} for interface {} in network {} from brint {} on remoteNode {}", attachedMac, remoteIntf, networkUUID, remoteBrInt, remoteNode);
                    attachedMacs.add(attachedMac);
                }
            }
            logger.debug("programLocalEgressDedicatedTunnelBridgeRules: retrieved macs {} for network {} on remoteNode {}", attachedMacs, networkUUID, remoteNode);

            Bridge localBrTun = (Bridge) ovsdbTable.getRow(localNode, Bridge.NAME.getName(), localNetworkBrTunUUID);
            Set<String> dpids = localBrTun.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);

            Status status = null;
            for (String attachedMac : attachedMacs) {
                String flowName = "TepMatch"+networkTunnelOFPort+""+internalVlan+""+HexEncode.stringToLong(attachedMac);
                logger.debug("programLocalEgressDedicatedTunnelBridgeRules: programming flowname {} with mac {} for ofNode {} on Node {}", flowName, attachedMac, ofNode, localNode);
                FlowConfig flow = new FlowConfig();
                flow.setName(flowName);
                flow.setNode(ofNode);
                flow.setInstallInHw(true);
                flow.setPriority(EGRESS_TUNNEL_FLOW_PRIORITY+"");
                flow.setDstMac(attachedMac);
                flow.setIngressPort(networkPatchPort+"");
                flow.setVlanId(internalVlan+"");
                List<String> actions = new ArrayList<String>();
                actions.add(ActionType.POP_VLAN.toString());
                actions.add(ActionType.OUTPUT.toString()+"="+networkTunnelOFPort);
                flow.setActions(actions);
                status = this.addStaticFlow(ofNode, flow);
                logger.debug("programLocalEgressDedicatedTunnelBridgeRules: status {} for flow {} in Node {}", status, flow, ofNode);
            }


        } catch (Exception e) {
            logger.error("Failed to initialize LocalEgressDedicatedTunnelBridgeRules Flow Rules for localNode {}, remoteNode {}, exception {}", localNode, remoteNode, e);
        }
    }

    private void removeRemoteEgressTunnelBridgeRules(Node node, int patchPort, String attachedMac,
            int internalVlan, int tunnelOFPort) {
        String brIntId = InternalNetworkManager.getManager().getInternalBridgeUUID(node, AdminConfigManager.getManager().getTunnelBridgeName());
        if (brIntId == null) {
            logger.error("Failed to initialize Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), brIntId);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepMatch"+tunnelOFPort+""+internalVlan+""+HexEncode.stringToLong(attachedMac);
            Status status = this.deleteStaticFlow(ofNode, flowName);
            logger.debug("Remote Egress Flow Removal Status {} for Flow {} on {} / {}", status, flowName, ofNode, node);
        } catch (Exception e) {
            logger.error("Failed to Remove Flow Rules for {}", node, e);
        }
    }


    /**
     * Program OF1.0 Flow rules to flood the broadcast & unknown-unicast traffic over br-tun on the egress direction
     * towards the network on all the overlay tunnels that corresponds to the tenant network.
     * The logic is to simply match on the incoming vlan, mac from the patch-port connected to br-int (patch-int)
     * and output the traffic to all the GRE-Tunnels for this Tenant Network (which carries the GRE-Key).
     * Also perform the Strip-Vlan action.
     */
    private void programFloodEgressTunnelBridgeRules(Node node, int patchPort, int internalVlan, int tunnelOFPort) {
        String brIntId = InternalNetworkManager.getManager().getInternalBridgeUUID(node, AdminConfigManager.getManager().getTunnelBridgeName());
        if (brIntId == null) {
            logger.error("Failed to initialize FloodEgressTunnelBridge Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), brIntId);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepFlood"+internalVlan;
            IForwardingRulesManager frm = (IForwardingRulesManager) ServiceHelper.getInstance(
                    IForwardingRulesManager.class, "default", this);
            FlowConfig existingFlowConfig = frm.getStaticFlow(flowName, ofNode);
            FlowConfig flow = existingFlowConfig;
            Status status = null;
            if (flow == null) {
                flow = new FlowConfig();
                flow.setName(flowName);
                flow.setNode(ofNode);
                flow.setPriority(FLOOD_TUNNEL_FLOW_PRIORITY+"");
                flow.setIngressPort(patchPort+"");
                flow.setVlanId(internalVlan+"");
                List<String> actions = new ArrayList<String>();
                actions.add(ActionType.POP_VLAN.toString());
                actions.add(ActionType.OUTPUT.toString()+"="+tunnelOFPort);
                flow.setActions(actions);
                status = frm.addStaticFlow(flow);
                logger.debug("Add Flood Egress Flow Programming Status {} for Flow {} on {} / {}",
                              status, flow, ofNode, node);
            } else {
                flow = new FlowConfig(existingFlowConfig);
                List<String> actions = flow.getActions();
                String outputPort = ActionType.OUTPUT.toString()+"="+tunnelOFPort;
                if (actions != null && !actions.contains(outputPort)) {
                    actions.add(outputPort);
                    flow.setActions(actions);
                } else {
                    logger.debug("Flood Egress Flow already exists. Skipping modify for Flow {} on {} / {}",
                                 flow, ofNode, node);
                    return;
                }
                status = frm.modifyStaticFlow(flow);
                logger.debug("Modify Flood Egress Flow Programming Status {} for Flow {} on {} / {}",
                              status, flow, ofNode, node);
            }
        } catch (Exception e) {
            logger.error("Failed to initialize FloodEgressTunnelBridge Flow Rules for {}", node, e);
        }
    }

    /**
     * Program OF1.0 Flow rules to flood the broadcast & unknown-unicast traffic over brtun-xy on the egress direction
     * towards the network on all the overlay tunnels that corresponds to the tenant network.
     * The logic is to simply match on the incoming vlan, mac from the patch-port connected to brint-xy (p-int-xy)
     * and output the traffic to all the GRE-Tunnels for this Tenant Network (which carries the GRE-Key).
     * Also perform the Strip-Vlan action.
     */
    private void programFloodEgressDedicatedTunnelBridgeRules(Node node, int networkPatchPort, int internalVlan, int networkTunnelOFPort, String networkUUID) {
        String networkBrTunName = TenantNetworkManager.getManager().getDedicatedTunBridgeNameForNetwork(networkUUID);
        String networkBrTunUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, networkBrTunName);

        if (networkBrTunUUID == null) {
            logger.error("Failed to initialize FloodEgressDedicatedTunnelBridge Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), networkBrTunUUID);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepFlood"+internalVlan;
            IForwardingRulesManager frm = (IForwardingRulesManager) ServiceHelper.getInstance(
                    IForwardingRulesManager.class, "default", this);
            FlowConfig existingFlowConfig = frm.getStaticFlow(flowName, ofNode);
            FlowConfig flow = existingFlowConfig;
            Status status = null;
            if (flow == null) {
                flow = new FlowConfig();
                flow.setName(flowName);
                flow.setNode(ofNode);
                flow.setPriority(FLOOD_TUNNEL_FLOW_PRIORITY+"");
                flow.setIngressPort(networkPatchPort+"");
                flow.setVlanId(internalVlan+"");
                List<String> actions = new ArrayList<String>();
                actions.add(ActionType.POP_VLAN.toString());
                actions.add(ActionType.OUTPUT.toString()+"="+networkTunnelOFPort);
                flow.setActions(actions);
                status = frm.addStaticFlow(flow);
                logger.debug("Add Dedicated Flood Egress Flow Programming Status {} for Flow {} on {} / {}",
                              status, flow, ofNode, node);
            } else {
                flow = new FlowConfig(existingFlowConfig);
                List<String> actions = flow.getActions();
                String outputPort = ActionType.OUTPUT.toString()+"="+networkTunnelOFPort;
                if (actions != null && !actions.contains(outputPort)) {
                    actions.add(outputPort);
                    flow.setActions(actions);
                } else {
                    logger.debug("Dedicated Flood Egress Flow already exists. Skipping modify for Flow {} on {} / {}",
                                 flow, ofNode, node);
                    return;
                }
                status = frm.modifyStaticFlow(flow);
                logger.debug("Modify Dedicated Flood Egress Flow Programming Status {} for Flow {} on {} / {}",
                              status, flow, ofNode, node);
            }
        } catch (Exception e) {
            logger.error("Failed to initialize FloodEgressDedicatedTunnelBridge Flow Rules for {}", node, e);
        }
    }


    private void removeFloodEgressTunnelBridgeRules(Node node, int patchPort, int internalVlan, int tunnelOFPort) {
        String brIntId = InternalNetworkManager.getManager().getInternalBridgeUUID(node, AdminConfigManager.getManager().getTunnelBridgeName());
        if (brIntId == null) {
            logger.error("Failed to remove Flow Rules for {}", node);
            return;
        }
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), brIntId);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            String flowName = "TepFlood"+internalVlan;
            IForwardingRulesManager frm = (IForwardingRulesManager) ServiceHelper.getInstance(
                    IForwardingRulesManager.class, "default", this);
            FlowConfig flow = frm.getStaticFlow(flowName, ofNode);
            Status status = null;
            if (flow != null) {
                status = frm.removeStaticFlow(flowName, ofNode);
                logger.debug("Remove Flood Egress Flow Programming Status {} for Flow {} on {} / {}",
                              status, flow, ofNode, node);

            } else {
                logger.debug("Flood Egress Flow already removed. Skipping removal for Flow {} on {} / {}",
                             flow, ofNode, node);
                return;
            }
        } catch (Exception e) {
            logger.error("Failed to remove Flow Rules for {}", node, e);
        }
    }


    private void programTunnelRules (String tunnelType, String segmentationId, InetAddress dst, Node dstNode, Node node,
                                     Interface intf, boolean local) {
        String networkId = TenantNetworkManager.getManager().getNetworkIdForSegmentationId(segmentationId);
        if (networkId == null) {
            logger.debug("Tenant Network not found with Segmenation-id {}",segmentationId);
            return;
        }
        int internalVlan = TenantNetworkManager.getManager().getInternalVlan(node, networkId);
        if (internalVlan == 0) {
            logger.debug("No InternalVlan provisioned for Tenant Network {}",networkId);
            return;
        }
        Map<String, String> externalIds = intf.getExternal_ids();
        if (externalIds == null) {
            logger.error("No external_ids seen in {}", intf);
            return;
        }

        String attachedMac = externalIds.get(TenantNetworkManager.EXTERNAL_ID_VM_MAC);
        if (attachedMac == null) {
            logger.error("No AttachedMac seen in {}", intf);
            return;
        }
        String patchInt = AdminConfigManager.getManager().getPatchToIntegration();

        int patchOFPort = -1;
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Map<String, Table<?>> intfs = ovsdbTable.getRows(node, Interface.NAME.getName());
            if (intfs != null) {
                for (Table<?> row : intfs.values()) {
                    Interface patchIntf = (Interface)row;
                    if (patchIntf.getName().equalsIgnoreCase(patchInt)) {
                        Set<BigInteger> of_ports = patchIntf.getOfport();
                        if (of_ports == null || of_ports.size() <= 0) {
                            logger.error("Could NOT Identified Patch port {} on {}", patchInt, node);
                            continue;
                        }
                        patchOFPort = Long.valueOf(((BigInteger)of_ports.toArray()[0]).longValue()).intValue();
                        logger.debug("Identified Patch port {} -> OF ({}) on {}", patchInt, patchOFPort, node);
                        break;
                    }
                }
                if (patchOFPort == -1) {
                    logger.error("Cannot identify {} interface on {}", patchInt, node);
                }
                for (Table<?> row : intfs.values()) {
                    Interface tunIntf = (Interface)row;
                    if (tunIntf.getName().equals(this.getTunnelName(tunnelType, segmentationId, dst))) {
                        Set<BigInteger> of_ports = tunIntf.getOfport();
                        if (of_ports == null || of_ports.size() <= 0) {
                            logger.warn("Could not Identify Tunnel port {} on {}. Don't panic. It might get converged soon...", tunIntf.getName(), node);
                            continue;
                        }
                        int tunnelOFPort = Long.valueOf(((BigInteger)of_ports.toArray()[0]).longValue()).intValue();

                        if (tunnelOFPort == -1) {
                            logger.warn("Tunnel Port {} on node {}: OFPort = -1 . Don't panic. It might get converged soon...", tunIntf.getName(), node);
                            return;
                        }
                        logger.debug("Identified Tunnel port {} -> OF ({}) on {}", tunIntf.getName(), tunnelOFPort, node);

                        if (!local) {
                            programRemoteEgressTunnelBridgeRules(node, patchOFPort, attachedMac, internalVlan, tunnelOFPort);
                        }
                        programLocalIngressTunnelBridgeRules(node, tunnelOFPort, internalVlan, patchOFPort);
                        programFloodEgressTunnelBridgeRules(node, patchOFPort, internalVlan, tunnelOFPort);
                        if (local){
                            programLocalEgressTunnelBridgeRules(node, dstNode, patchOFPort, internalVlan, tunnelOFPort, networkId);
                        }
                        return;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private void removeTunnelRules (String tunnelType, String segmentationId, InetAddress dst, Node node, Interface intf, boolean local) {
        String networkId = TenantNetworkManager.getManager().getNetworkIdForSegmentationId(segmentationId);
        if (networkId == null) {
            logger.debug("Tenant Network not found with Segmenation-id {}",segmentationId);
            return;
        }
        int internalVlan = TenantNetworkManager.getManager().getInternalVlan(node,networkId);
        if (internalVlan == 0) {
            logger.debug("No InternalVlan provisioned for Tenant Network {}",networkId);
            return;
        }
        Map<String, String> externalIds = intf.getExternal_ids();
        if (externalIds == null) {
            logger.error("No external_ids seen in {}", intf);
            return;
        }

        String attachedMac = externalIds.get(TenantNetworkManager.EXTERNAL_ID_VM_MAC);
        if (attachedMac == null) {
            logger.error("No AttachedMac seen in {}", intf);
            return;
        }
        String patchInt = AdminConfigManager.getManager().getPatchToIntegration();

        int patchOFPort = -1;
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Map<String, Table<?>> intfs = ovsdbTable.getRows(node, Interface.NAME.getName());
            if (intfs != null) {
                for (Table<?> row : intfs.values()) {
                    Interface patchIntf = (Interface)row;
                    if (patchIntf.getName().equalsIgnoreCase(patchInt)) {
                        Set<BigInteger> of_ports = patchIntf.getOfport();
                        if (of_ports == null || of_ports.size() <= 0) {
                            logger.error("Could NOT Identified Patch port {} on {}", patchInt, node);
                            continue;
                        }
                        patchOFPort = Long.valueOf(((BigInteger)of_ports.toArray()[0]).longValue()).intValue();
                        logger.debug("Identified Patch port {} -> OF ({}) on {}", patchInt, patchOFPort, node);
                        break;
                    }
                }
                if (patchOFPort == -1) {
                    logger.error("Cannot identify {} interface on {}", patchInt, node);
                }
                for (Table<?> row : intfs.values()) {
                    Interface tunIntf = (Interface)row;
                    if (tunIntf.getName().equals(this.getTunnelName(tunnelType, segmentationId, dst))) {
                        Set<BigInteger> of_ports = tunIntf.getOfport();
                        if (of_ports == null || of_ports.size() <= 0) {
                            logger.error("Could NOT Identify Tunnel port {} on {}", tunIntf.getName(), node);
                            continue;
                        }
                        int tunnelOFPort = Long.valueOf(((BigInteger)of_ports.toArray()[0]).longValue()).intValue();

                        if (tunnelOFPort == -1) {
                            logger.error("Could NOT Identify Tunnel port {} -> OF ({}) on {}", tunIntf.getName(), tunnelOFPort, node);
                            return;
                        }
                        logger.debug("Identified Tunnel port {} -> OF ({}) on {}", tunIntf.getName(), tunnelOFPort, node);

                        if (!local) {
                            removeRemoteEgressTunnelBridgeRules(node, patchOFPort, attachedMac, internalVlan, tunnelOFPort);
                        }
                        removeLocalIngressTunnelBridgeRules(node, tunnelOFPort, internalVlan, patchOFPort);
                        removeFloodEgressTunnelBridgeRules(node, patchOFPort, internalVlan, tunnelOFPort);
                        return;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private void programDedicatedTunnelRules(String tunnelType, String segmentationId, InetAddress dst, Node dstNode, Node node, Interface intf, boolean local) {
        logger.debug("programDedicatedTunnelRules tunnelType {}, segmentationId {}, dst {}, node {}, intf {}, local {}", tunnelType, segmentationId, dst, node, intf, local);
        String networkId = TenantNetworkManager.getManager().getNetworkIdForSegmentationId(segmentationId);
        if (networkId == null) {
            logger.debug("Tenant Network not found with Segmenation-id {}",segmentationId);
            return;
        }
        int internalVlan = TenantNetworkManager.getManager().getInternalVlan(node, networkId);
        if (internalVlan == 0) {
            logger.debug("No InternalVlan provisioned for Tenant Network {}",networkId);
            return;
        }
        Map<String, String> externalIds = intf.getExternal_ids();
        if (externalIds == null) {
            logger.error("No external_ids seen in {}", intf);
            return;
        }

        String attachedMac = externalIds.get(TenantNetworkManager.EXTERNAL_ID_VM_MAC);
        if (attachedMac == null) {
            logger.error("No AttachedMac seen in {}", intf);
            return;
        }
//        String patchInt = AdminConfigManager.getManager().getPatchToIntegration();
        String networkPatchInt = TenantNetworkManager.getManager().getPatchToDedicatedIntForNetwork(networkId);

        int networkPatchOFPort = -1;
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Map<String, Table<?>> intfs = ovsdbTable.getRows(node, Interface.NAME.getName());
            if (intfs != null) {
                for (Table<?> row : intfs.values()) {
                    Interface networkPatchIntf = (Interface)row;
                    if (networkPatchIntf.getName().equalsIgnoreCase(networkPatchInt)) {
                        Set<BigInteger> of_ports = networkPatchIntf.getOfport();
                        if (of_ports == null || of_ports.size() <= 0) {
                            logger.error("Could NOT Identified Patch port {} on {}", networkPatchInt, node);
                            continue;
                        }
                        networkPatchOFPort = Long.valueOf(((BigInteger)of_ports.toArray()[0]).longValue()).intValue();
                        logger.debug("Identified Patch port {} -> OF ({}) on {}", networkPatchInt, networkPatchOFPort, node);
                        break;
                    }
                }
                if (networkPatchOFPort == -1) {
                    logger.error("Cannot identify {} interface on {}", networkPatchInt, node);
                }
                for (Table<?> row : intfs.values()) {
                    Interface networkTunIntf = (Interface)row;
                    if (networkTunIntf.getName().equals(this.getTunnelName(tunnelType, segmentationId, dst))) {
                        Set<BigInteger> of_ports = networkTunIntf.getOfport();
                        if (of_ports == null || of_ports.size() <= 0) {
                            logger.error("Could NOT Identify Tunnel port {} on {}", networkTunIntf.getName(), node);
                            continue;
                        }
                        int networkTunnelOFPort = Long.valueOf(((BigInteger)of_ports.toArray()[0]).longValue()).intValue();

                        if (networkTunnelOFPort == -1) {
                            logger.error("Could NOT Identify Tunnel port {} -> OF ({}) on {}", networkTunIntf.getName(), networkTunnelOFPort, node);
                            return;
                        }
                        logger.debug("Identified Tunnel port {} -> OF ({}) on {}", networkTunIntf.getName(), networkTunnelOFPort, node);

                        if (!local) {
                            programRemoteEgressDedicatedTunnelBridgeRules(node, networkPatchOFPort, attachedMac, internalVlan, networkTunnelOFPort, networkId);
                        }
                        programLocalIngressDedicatedTunnelBridgeRules(node, networkTunnelOFPort, internalVlan, networkPatchOFPort, networkId);
                        programFloodEgressDedicatedTunnelBridgeRules(node, networkPatchOFPort, internalVlan, networkTunnelOFPort, networkId);
                        if (local){
                            programLocalEgressDedicatedTunnelBridgeRules(node, dstNode, networkPatchOFPort, internalVlan, networkTunnelOFPort, networkId);
                        }
                        return;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("", e);
        }

    }


    @Override
    public Status handleInterfaceUpdate(String tunnelType, String tunnelKey, Node srcNode, Interface intf) {
        Status status = getTunnelReadinessStatus(srcNode, tunnelKey);
        if (!status.isSuccess()) return status;

        IConnectionServiceInternal connectionService = (IConnectionServiceInternal)ServiceHelper.getGlobalInstance(IConnectionServiceInternal.class, this);
        List<Node> nodes = connectionService.getNodes();
        nodes.remove(srcNode);
        for (Node dstNode : nodes) {
            status = getTunnelReadinessStatus(dstNode, tunnelKey);
            if (!status.isSuccess()) continue;
            InetAddress src = AdminConfigManager.getManager().getTunnelEndPoint(srcNode);
            InetAddress dst = AdminConfigManager.getManager().getTunnelEndPoint(dstNode);
            status = addTunnelPort(srcNode, tunnelType, src, dst, tunnelKey);
            if (status.isSuccess()) {
                this.programTunnelRules(tunnelType, tunnelKey, dst, dstNode, srcNode, intf, true);
            }
            status = addTunnelPort(dstNode, tunnelType, dst, src, tunnelKey);
            if (status.isSuccess()) {
                this.programTunnelRules(tunnelType, tunnelKey, src, srcNode, dstNode, intf, false);
            }
        }
        return new Status(StatusCode.SUCCESS);
    }

    @Override
    public Status handleInterfaceDelete(String tunnelType, String tunnelKey, Node srcNode, Interface intf, boolean isLastInstanceOnNode) {
        Status status = new Status(StatusCode.SUCCESS);

        IConnectionServiceInternal connectionService = (IConnectionServiceInternal)ServiceHelper.getGlobalInstance(IConnectionServiceInternal.class, this);
        List<Node> nodes = connectionService.getNodes();
        nodes.remove(srcNode);
        for (Node dstNode : nodes) {
            InetAddress src = AdminConfigManager.getManager().getTunnelEndPoint(srcNode);
            InetAddress dst = AdminConfigManager.getManager().getTunnelEndPoint(dstNode);
            this.removeTunnelRules(tunnelType, tunnelKey, dst, srcNode, intf, true);
            if (isLastInstanceOnNode) {
                status = deleteTunnelPort(srcNode, tunnelType, src, dst, tunnelKey);
            }
            this.removeTunnelRules(tunnelType, tunnelKey, src, dstNode, intf, false);
            if (status.isSuccess() && isLastInstanceOnNode) {
                deleteTunnelPort(dstNode, tunnelType, dst, src, tunnelKey);
            }
        }
        return status;
    }

    @Override
    public Status createDedicatedNetworkTunnels(NeutronNetwork network, String tunnelType, String tunnelKey, Node srcNode, Interface intf) {
        Status status = getDedicatedNetworkTunnelReadinessStatus(srcNode, network, tunnelKey);
        if (!status.isSuccess()) return status;

        IConnectionServiceInternal connectionService = (IConnectionServiceInternal)ServiceHelper.getGlobalInstance(IConnectionServiceInternal.class, this);
        List<Node> nodes = connectionService.getNodes();
        nodes.remove(srcNode);
        InetAddress src = AdminConfigManager.getManager().getDedicatedNetworkTunnelEndPoint(srcNode, network);

        for (Node dstNode : nodes) {
            status = getDedicatedNetworkTunnelReadinessStatus(dstNode, network, tunnelKey);
            if (!status.isSuccess()) continue;
            logger.debug("createDedicatedNetworkTunnels: creating tunnels between {} and {} for network {} with tunnelKey {}", srcNode,dstNode, network.getNetworkName(), tunnelKey);

            InetAddress dst = AdminConfigManager.getManager().getDedicatedNetworkTunnelEndPoint(dstNode, network);
            logger.debug("createDedicatedNetworkTunnels: Tunnel endpoints are {} and {}", src, dst);

            status = addDedicatedNetworkTunnelPort(srcNode, network, tunnelType, src, dst, tunnelKey);
            if (status.isSuccess()){
                logger.debug("createDedicatedNetworkTunnels succeed: add dedicated tunnel port, srcNode {}, network {}, tunnelType {}, src {}, dst {}, tunnelKey {}", srcNode, network, tunnelType, src, dst, tunnelKey);
                programDedicatedTunnelRules(tunnelType, tunnelKey, dst, dstNode, srcNode, intf, true);
            } else {
                logger.error("createDedicatedNetworkTunnels failed {}: add dedicated tunnel port, srcNode {}, network {}, tunnelType {}, src {}, dst {}, tunnelKey {}", status.toString(), srcNode, network, tunnelType, src, dst, tunnelKey);
            }
            status = addDedicatedNetworkTunnelPort(dstNode, network, tunnelType, dst, src, tunnelKey);
            if (status.isSuccess()){
                logger.debug("createDedicatedNetworkTunnels succeed: add dedicated tunnel port, srcNode {}, network {}, tunnelType {}, src {}, dst {}, tunnelKey {}", dstNode, network, tunnelType, dst, src, tunnelKey);
                programDedicatedTunnelRules(tunnelType, tunnelKey, src, srcNode, dstNode, intf, false);
            } else {
                logger.error("createDedicatedNetworkTunnels failed {}: add dedicated tunnel port, srcNode {}, network {}, tunnelType {}, src {}, dst {}, tunnelKey {}", status.toString(), dstNode, network, tunnelType, dst, src, tunnelKey);
            }
        }
        return null;
    }



    private String getTunnelName(String tunnelType, String key, InetAddress dst) {
        return tunnelType+"-"+key+"-"+dst.getHostAddress();
    }

    private Interface getTunnelInterface (Node node, String tunnelType, InetAddress dst, String key) {
        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            String portName = getTunnelName(tunnelType, key, dst);

            Map<String, Table<?>> tunIntfs = ovsdbTable.getRows(node, Interface.NAME.getName());
            if (tunIntfs != null) {
                for (Table<?> row : tunIntfs.values()) {
                    Interface tunIntf = (Interface)row;
                    if (tunIntf.getName().equals(portName)) return tunIntf;
                }

            }
        } catch (Exception e) {
            logger.error("", e);
        }
        return null;
    }

    private boolean isTunnelPresent(Node node, String tunnelName, String bridgeUUID) throws Exception {
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        Bridge bridge = (Bridge)ovsdbTable.getRow(node, Bridge.NAME.getName(), bridgeUUID);
        if (bridge != null) {
            Set<UUID> ports = bridge.getPorts();
            for (UUID portUUID : ports) {
                Port port = (Port)ovsdbTable.getRow(node, Port.NAME.getName(), portUUID.toString());
                if (port != null && port.getName().equalsIgnoreCase(tunnelName)) return true;
            }
        }
        return false;
    }


    private Status addDedicatedNetworkTunnelPort (Node node, NeutronNetwork network, String tunnelType, InetAddress src, InetAddress dst, String key){
        logger.debug("addDedicatedNetworkTunnelPort {} {} {} {} {} {}", node, network, tunnelType, src, dst, key);
        String networkTunBrName = TenantNetworkManager.getManager().getDedicatedTunBridgeNameForNetwork(network);
        String networkTunBrUUID = InternalNetworkManager.getManager().getInternalBridgeUUID(node, networkTunBrName);
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        if (networkTunBrUUID == null){
            logger.error("addDedicatedNetworkTunnelPort: Could not find Network Tunnel Bridge {} in {}", networkTunBrUUID, node);
            return new Status(StatusCode.NOTFOUND, "Could not find "+networkTunBrUUID+" in "+node);
        }
        String networkTunPortName = getTunnelName(tunnelType, key, dst);
        try {
            if(isTunnelPresent(node, networkTunPortName, networkTunBrUUID)){
                logger.debug("addDedicatedNetworkTunnelPort: Tunnel {} in Bridge {} exists on Node {}", networkTunBrName, networkTunBrUUID, node);
                return new Status(StatusCode.SUCCESS);
            }
            StatusWithUuid statusWithUuid = TenantNetworkManager.getManager().addPortToBridge(node, networkTunBrUUID, networkTunPortName);
            if (!statusWithUuid.isSuccess()) {
                logger.error("addDedicatedNetworkTunnelPort: Failed to insert Tunnel port {} in {}", networkTunPortName, networkTunBrUUID);
                return statusWithUuid;
            } else {
                logger.debug("addDedicatedNetworkTunnelPort: Succeed to insert Tunnel port {} in {}", networkTunPortName, networkTunBrUUID);
            }
            String tunnelPortUUID = statusWithUuid.getUuid().toString();
            String interfaceUUID = TenantNetworkManager.getManager().getAddedInterfaceUUID(node, tunnelPortUUID);
            if (interfaceUUID == null) {
                logger.error("addDedicatedNetworkTunnelPort: Cannot retrieve Tunnel Interface for port {}/{}", networkTunPortName, tunnelPortUUID);
                return new Status(StatusCode.INTERNALERROR);
            }

            Interface tunInterface = new Interface();
            tunInterface.setType(tunnelType);
            OvsDBMap<String, String> options = new OvsDBMap<String, String>();
            options.put("key", key);
            options.put("local_ip", src.getHostAddress());
            options.put("remote_ip", dst.getHostAddress());
            tunInterface.setOptions(options);
            Status status = ovsdbTable.updateRow(node, Interface.NAME.getName(), tunnelPortUUID, interfaceUUID, tunInterface);
            logger.debug("Tunnel {} add status : {}", tunInterface, status);
            return status;

        } catch (Exception e) {
            logger.error("addDedicatedNetworkTunnelPort Exception", e);
            return new Status(StatusCode.INTERNALERROR, e.toString());
        }
    }

    private String getTunnelPortUuid(Node node, String tunnelName, String bridgeUUID) throws Exception {
        OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
        Bridge bridge = (Bridge)ovsdbTable.getRow(node, Bridge.NAME.getName(), bridgeUUID);
        if (bridge != null) {
            Set<UUID> ports = bridge.getPorts();
            for (UUID portUUID : ports) {
                Port port = (Port)ovsdbTable.getRow(node, Port.NAME.getName(), portUUID.toString());
                if (port != null && port.getName().equalsIgnoreCase(tunnelName)) return portUUID.toString();
            }
        }
        return null;
    }


    private Status addTunnelPort (Node node, String tunnelType, InetAddress src, InetAddress dst, String key) {
        logger.info("addTunnelPort {} {} {} {} {}", node, tunnelType, src, dst, key);
        try {
            String bridgeUUID = null;
            String tunnelBridgeName = AdminConfigManager.getManager().getTunnelBridgeName();
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Map<String, Table<?>> bridgeTable = ovsdbTable.getRows(node, Bridge.NAME.getName());
            if (bridgeTable != null) {
                for (String uuid : bridgeTable.keySet()) {
                    Bridge bridge = (Bridge)bridgeTable.get(uuid);
                    if (bridge.getName().equals(tunnelBridgeName)) {
                        bridgeUUID = uuid;
                        break;
                    }
                }
            }
            if (bridgeUUID == null) {
                logger.error("Could not find Bridge {} in {}", tunnelBridgeName, node);
                return new Status(StatusCode.NOTFOUND, "Could not find "+tunnelBridgeName+" in "+node);
            }
            String portName = getTunnelName(tunnelType, key, dst);

            if (this.isTunnelPresent(node, portName, bridgeUUID)) {
                logger.trace("Tunnel {} is present in {} of {}", portName, tunnelBridgeName, node);
                return new Status(StatusCode.SUCCESS);
            }

            Port tunnelPort = new Port();
            tunnelPort.setName(portName);
            StatusWithUuid statusWithUuid = ovsdbTable.insertRow(node, Port.NAME.getName(), bridgeUUID, tunnelPort);
            if (!statusWithUuid.isSuccess()) {
                logger.error("Failed to insert Tunnel port {} in {}", portName, bridgeUUID);
                return statusWithUuid;
            }

            String tunnelPortUUID = statusWithUuid.getUuid().toString();
            String interfaceUUID = null;
            int timeout = 6;
            while ((interfaceUUID == null) && (timeout > 0)) {
                tunnelPort = (Port)ovsdbTable.getRow(node, Port.NAME.getName(), tunnelPortUUID);
                OvsDBSet<UUID> interfaces = tunnelPort.getInterfaces();
                if (interfaces == null || interfaces.size() == 0) {
                    // Wait for the OVSDB update to sync up the Local cache.
                    Thread.sleep(500);
                    timeout--;
                    continue;
                }
                interfaceUUID = interfaces.toArray()[0].toString();
                Interface intf = (Interface)ovsdbTable.getRow(node, Interface.NAME.getName(), interfaceUUID);
                if (intf == null) interfaceUUID = null;
            }

            if (interfaceUUID == null) {
                logger.error("Cannot identify Tunnel Interface for port {}/{}", portName, tunnelPortUUID);
                return new Status(StatusCode.INTERNALERROR);
            }

            Interface tunInterface = new Interface();
            tunInterface.setType(tunnelType);
            OvsDBMap<String, String> options = new OvsDBMap<String, String>();
            options.put("key", key);
            options.put("local_ip", src.getHostAddress());
            options.put("remote_ip", dst.getHostAddress());
            if (tunnelType.equalsIgnoreCase("ipsec_gre")){
                logger.info("IPSec tunnel is requested for Interface {}", tunInterface);
                String networkName = TenantNetworkManager.getManager().getNetworkNameForSegmentationId(key);
                String ipsecPsk = networkName+"psk";
                logger.info("IPSec tunnel key: {}", ipsecPsk);
                options.put("psk", ipsecPsk);
            }
            tunInterface.setOptions(options);
            Status status = ovsdbTable.updateRow(node, Interface.NAME.getName(), tunnelPortUUID, interfaceUUID, tunInterface);
            logger.debug("Tunnel {} add status : {}", tunInterface, status);
            return status;
        } catch (Exception e) {
            logger.error("Exception in addTunnelPort", e);
            return new Status(StatusCode.INTERNALERROR);
        }
    }

    private Status deleteTunnelPort (Node node, String tunnelType, InetAddress src, InetAddress dst, String key) {
        try {
            String bridgeUUID = null;
            String tunnelBridgeName = AdminConfigManager.getManager().getTunnelBridgeName();
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Map<String, Table<?>> bridgeTable = ovsdbTable.getRows(node, Bridge.NAME.getName());
            if (bridgeTable != null) {
                for (String uuid : bridgeTable.keySet()) {
                    Bridge bridge = (Bridge)bridgeTable.get(uuid);
                    if (bridge.getName().equals(tunnelBridgeName)) {
                        bridgeUUID = uuid;
                        break;
                    }
                }
            }
            if (bridgeUUID == null) {
                logger.debug("Could not find Bridge {} in {}", tunnelBridgeName, node);
                return new Status(StatusCode.SUCCESS);
            }
            String portName = getTunnelName(tunnelType, key, dst);
            String tunnelPortUUID = this.getTunnelPortUuid(node, portName, bridgeUUID);
            Status status = ovsdbTable.deleteRow(node, Port.NAME.getName(), tunnelPortUUID);
            if (!status.isSuccess()) {
                logger.error("Failed to delete Tunnel port {} in {} status : {}", portName, bridgeUUID, status);
                return status;
            }

            logger.debug("Tunnel {} delete status : {}", portName, status);
            return status;
        } catch (Exception e) {
            logger.error("Exception in deleteTunnelPort", e);
            return new Status(StatusCode.INTERNALERROR);
        }
    }

    @Override
    public Status handleInterfaceUpdate(String tunnelType, String tunnelKey) {
        IConnectionServiceInternal connectionService = (IConnectionServiceInternal)ServiceHelper.getGlobalInstance(IConnectionServiceInternal.class, this);
        List<Node> nodes = connectionService.getNodes();
        for (Node srcNode : nodes) {
            this.handleInterfaceUpdate(tunnelType, tunnelKey, srcNode, null);
        }
        return new Status(StatusCode.SUCCESS);
    }

    @Override
    public void initializeFlowRules(Node node) {
        this.initializeFlowRules(node, AdminConfigManager.getManager().getIntegrationBridgeName());
        this.initializeFlowRules(node, AdminConfigManager.getManager().getTunnelBridgeName());
        this.initializeFlowRules(node, AdminConfigManager.getManager().getExternalBridgeName());
    }

    private void initializeFlowRules(Node node, String bridgeName) {
        String brIntId = this.getInternalBridgeUUID(node, bridgeName);
        if (brIntId == null) {

            logger.error("Failed to initialize Flow Rules for Node: {}, BridgeName: {} is null", node, bridgeName);

            if (bridgeName == AdminConfigManager.getManager().getExternalBridgeName()){
                logger.debug("Failed to initialize Flow Rules for bridge {} on node {}. Is the Neutron L3 agent running on this node?");
            }
            else {
                logger.debug("Failed to initialize Flow Rules for bridge {} on node {}", bridgeName, node);
            }

            return;
        }

        try {
            OVSDBConfigService ovsdbTable = (OVSDBConfigService)ServiceHelper.getGlobalInstance(OVSDBConfigService.class, this);
            Bridge bridge = (Bridge) ovsdbTable.getRow(node, Bridge.NAME.getName(), brIntId);
            Set<String> dpids = bridge.getDatapath_id();
            if (dpids == null || dpids.size() ==  0) return;
            Long dpidLong = Long.valueOf(HexEncode.stringToLong((String)dpids.toArray()[0]));
            Node ofNode = new Node(Node.NodeIDType.OPENFLOW, dpidLong);
            ISwitchManager switchManager = (ISwitchManager) ServiceHelper.getInstance(ISwitchManager.class, "default", this);
            List<Switch> nodes = switchManager.getNetworkDevices();
            if (nodes == null) {
                logger.debug("No OF nodes learned yet in {}", node);
                return;
            }
            for (Switch device : nodes) {
                if (device.getNode().equals(ofNode)) {
                    logger.debug("Initialize OF Flows on {}", ofNode);
                    initializeNormalFlowRules(ofNode);
                    return;
                }
            }
            logger.debug("Could not identify OF node {} for bridge {} in {}", ofNode.toString(), bridgeName, node.toString());
        } catch (Exception e) {
            logger.error("Failed to initialize Flow Rules for "+node.toString(), e);
        }
    }

    @Override
    public void initializeOFFlowRules(Node openflowNode) {
        this.initializeNormalFlowRules(openflowNode);
        this.initializeLLDPFlowRules(openflowNode);
    }

    private void initializeNormalFlowRules(Node ofNode) {
        String flowName = ActionType.HW_PATH.toString();
        FlowConfig flow = new FlowConfig();
        flow.setName("NORMAL");
        flow.setNode(ofNode);
        flow.setPriority(NORMAL_PRIORITY+"");
        flow.setInstallInHw(true);
        List<String> normalAction = new ArrayList<String>();
        normalAction.add(flowName);
        flow.setActions(normalAction);
        Status status = this.addStaticFlow(ofNode, flow);
        logger.debug("Flow Programming Add Status {} for Flow {} on {}", status, flow, ofNode);
    }

    private void initializeLLDPFlowRules(Node ofNode) {
        String flowName = "PuntLLDP";
        List<String> puntAction = new ArrayList<String>();
        puntAction.add(ActionType.CONTROLLER.toString());

        FlowConfig allowLLDP = new FlowConfig();
        allowLLDP.setName(flowName);
        allowLLDP.setPriority(LLDP_PRIORITY+"");
        allowLLDP.setNode(ofNode);
        allowLLDP.setInstallInHw(true);
        allowLLDP.setEtherType("0x" + Integer.toHexString(EtherTypes.LLDP.intValue()).toUpperCase());
        allowLLDP.setActions(puntAction);
        Status status = this.addStaticFlow(ofNode, allowLLDP);
        logger.debug("LLDP Flow Add Status {} for Flow {} on {}", status, allowLLDP, ofNode);
    }

    private Status addStaticFlow (Node ofNode, FlowConfig flowConfig) {
        IForwardingRulesManager frm = (IForwardingRulesManager) ServiceHelper.getInstance(
                IForwardingRulesManager.class, "default", this);
        String flowName = flowConfig.getName();
        if (frm.getStaticFlow(flowName, ofNode) != null) {
            logger.debug("Flow already exists {} on {}. Skipping installation.", flowName, ofNode);
            return new Status(StatusCode.CONFLICT, "Flow with name "+flowName+" exists in node "+ofNode.toString());
        }
        return frm.addStaticFlow(flowConfig);
    }

    private Status deleteStaticFlow (Node ofNode, String flowName) {
        IForwardingRulesManager frm = (IForwardingRulesManager) ServiceHelper.getInstance(
                IForwardingRulesManager.class, "default", this);
        if (frm.getStaticFlow(flowName, ofNode) == null) {
            logger.debug("Flow doese not exist {} on {}. Skipping deletion.", flowName, ofNode);
            return new Status(StatusCode.SUCCESS);
        }
        return frm.removeStaticFlow(flowName,ofNode);
    }}
