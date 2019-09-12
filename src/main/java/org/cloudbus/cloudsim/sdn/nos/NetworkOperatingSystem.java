/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2015, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.sdn.nos;

import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.predicates.PredicateType;
import org.cloudbus.cloudsim.sdn.CloudSimEx;
import org.cloudbus.cloudsim.sdn.CloudSimTagsSDN;
import org.cloudbus.cloudsim.sdn.Configuration;
import org.cloudbus.cloudsim.sdn.LogWriter;
import org.cloudbus.cloudsim.sdn.Packet;
import org.cloudbus.cloudsim.sdn.parsers.WorkloadParser;
import org.cloudbus.cloudsim.sdn.physicalcomponents.Link;
import org.cloudbus.cloudsim.sdn.physicalcomponents.Node;
import org.cloudbus.cloudsim.sdn.physicalcomponents.PhysicalTopology;
import org.cloudbus.cloudsim.sdn.physicalcomponents.PhysicalTopologyInterCloud;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNDatacenter;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.physicalcomponents.switches.Switch;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicy;
import org.cloudbus.cloudsim.sdn.policies.vmallocation.overbooking.OverbookingVmAllocationPolicy;
import org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager.Job;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunction;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunctionAutoScaler;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunctionChainPolicy;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunctionForwarder;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunctionForwarderLatencyAware;
import org.cloudbus.cloudsim.sdn.virtualcomponents.FlowConfig;
import org.cloudbus.cloudsim.sdn.virtualcomponents.Channel;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.virtualcomponents.VirtualNetworkMapper;
import org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer.Task;
import org.cloudbus.cloudsim.sdn.workload.Transmission;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.sdn.workload.Workload;

/**
 * NOS calculates and estimates network behaviour. It also mimics SDN Controller functions.  
 * It manages channels between allSwitches, and assigns packages to channels and control their completion
 * Once the transmission is completed, forward the packet to the destination.
 *  
 * @author Jungmin Son
 * @author Rodrigo N. Calheiros
 * @since CloudSimSDN 1.0
 */
public abstract class NetworkOperatingSystem extends SimEntity {
	protected SDNDatacenter datacenter;
	protected static int mockid = 123456;
	protected static boolean printedFinal = false;

	static double hostEnergyConsumption = 0.0;
	static double switchEnergyConsumption = 0.0;
	// Physical topology
	protected PhysicalTopology topology;

	// Virtual topology
	protected VirtualNetworkMapper vnMapper = null;
	protected ChannelManager channelManager = null;
	protected boolean isApplicationDeployed = false;

	// Keep task requests list in nos
	private ArrayList<Job> jobList = new ArrayList<>();

	public ArrayList<Job> getJobList() {
		return jobList;
	}
	public void addTask(Job job) {
		jobList.add(job);
	}
	
	// Map: Vm ID -> VM
	protected HashMap<Integer, Vm> vmMapId2Vm = new HashMap<Integer, Vm>();
	
	// Global map (static): Vm ID -> VM
	protected static HashMap<Integer, Vm> gvmMapId2Vm = new HashMap<Integer, Vm>();
	
	// Vm ID (src or dst) -> all Flow from/to the VM
	protected Multimap<Integer, FlowConfig> flowMapVmId2Flow = HashMultimap.create();
	
	// Global map (static): Flow ID -> VM
	protected static Map<Integer, FlowConfig> gFlowMapFlowId2Flow = new HashMap<Integer, FlowConfig>();
	
	protected ServiceFunctionForwarder sfcForwarder;
	protected ServiceFunctionAutoScaler sfcScaler;

	// Resolution of the result.
	public static final long bandwidthWithinSameHost = 1500000000; // bandwidth between VMs within a same host: 12Gbps = 1.5GBytes/sec
	public static final double latencyWithinSameHost = 0.1; //0.1 msec latency 
	
	private double lastMigration = 0;
	private double lastAdjustAllChannelTime = -1;
	private double nextEventTime = -1;
	
	/**
	 * 1. map VMs and middleboxes to hosts, add the new vm/mb to the vmHostTable, advise host, advise dc
	 * 2. set channels and bws
	 * 3. set routing tables to restrict hops to meet latency
	 * @param sfcPolicy 
	 */
	// FYI note how the child classes override this method
	protected abstract boolean deployApplication(List<Vm> vms, Collection<FlowConfig> links, List<ServiceFunctionChainPolicy> sfcPolicy);

	protected abstract boolean deployTasks(List<Job> jobs);
	protected abstract boolean deployTasksInitial();


	public NetworkOperatingSystem(String name) {
		super(name);
		
		if(Configuration.SFC_LATENCY_AWARE_ENABLE)
			this.sfcForwarder = new ServiceFunctionForwarderLatencyAware(this);
		else
			this.sfcForwarder = new ServiceFunctionForwarder(this);
		
		this.vnMapper = new VirtualNetworkMapper(this);
		this.channelManager = new ChannelManager(this, vnMapper, sfcForwarder);
		
		this.sfcScaler = new ServiceFunctionAutoScaler(this, sfcForwarder);
		
		this.topology = new PhysicalTopologyInterCloud();
	}
	
	public void setLinkSelectionPolicy(LinkSelectionPolicy linkSelectionPolicy) {
		vnMapper.setLinkSelectionPolicy(linkSelectionPolicy);
	}

	public void configurePhysicalTopology(Collection<SDNHost> hosts, Collection<Switch> switches, Collection<Link> links) {
		for(SDNHost sdnHost: hosts) {
			topology.addNode(sdnHost);
		}
		
		for(Switch sw:switches) {
			topology.addNode(sw);
		}

		// AMANDAAAA
		for(Link link:links) {
			topology.addLink(link);
		}
		
		topology.buildDefaultRouting();
	}

	@Override
	public void startEntity() {
		send(this.getId(), Configuration.monitoringTimeInterval, CloudSimTagsSDN.MONITOR_UPDATE_UTILIZATION);
	}

	@Override
	public void shutdownEntity() {
		
	}
	
	@Override
	public void processEvent(SimEvent ev) {
		int tag = ev.getTag();
		
		switch(tag){
			case CloudSimTagsSDN.SDN_INTERNAL_CHANNEL_PROCESS:
				processInternalAdjustChannels();
				break;				
			case CloudSimTagsSDN.SDN_INTERNAL_PACKET_PROCESS: 
				processInternalPacketProcessing(); 
				break;
			case CloudSimTags.VM_CREATE_ACK:
				processVmCreateAck(ev);
				break;
			case CloudSimTagsSDN.SCHEDULE_TASKS:
				deployTasks((ArrayList<Job>)ev.getData());
				break;
			case CloudSimTagsSDN.SCHEDULE_TASKS_INITIAL:
				deployTasksInitial();
				break;
			case CloudSimTags.VM_DESTROY:
				processVmDestroyAck(ev);
				break;
			case CloudSimTagsSDN.DEPLOY_TASK_COMM:
				deployFlows(ev);
				break;
			case CloudSimTagsSDN.SDN_VM_CREATE_DYNAMIC_ACK:
				processVmCreateDynamicAck(ev);
				break;
			case CloudSimTagsSDN.SDN_ACTIVATE_SWITCHES:
				activateSwitches(ev);
				break;
			case CloudSimTagsSDN.SCHEDULE_MIGRATION:
				this.datacenter.startMigration();
				break;
			case CloudSimTagsSDN.MONITOR_UPDATE_UTILIZATION:
				if(this.datacenter != null)
					this.datacenter.processUpdateProcessing();
				channelManager.updatePacketProcessing();

				this.updateBWMonitor(Configuration.monitoringTimeInterval);
				// AMANDAAA
				this.updateHostMonitor(Configuration.monitoringTimeInterval);
				this.updateSwitchMonitor(Configuration.monitoringTimeInterval);				
				// FYI how about adding a link power consumption monitor as well?? Maybe that has to be triggered when link usage is simulated via workloads transmission (energy-workload.csv)

				if(CloudSim.clock() >= lastMigration + Configuration.migrationTimeInterval && this.datacenter != null) {
					sfcScaler.scaleSFC();	// Start SFC Auto Scaling
					// AMANDAAAA
					this.datacenter.startMigrate(); // Start Migration
					
					lastMigration = CloudSim.clock(); 
				}
				this.updateVmMonitor(CloudSim.clock());

				//this.logEnergyConsumption();
				
				if(CloudSimEx.hasMoreEvent(CloudSimTagsSDN.MONITOR_UPDATE_UTILIZATION)) {
					double nextMonitorDelay = Configuration.monitoringTimeInterval;
					double nextEventDelay = CloudSimEx.getNextEventTime() - CloudSim.clock();
					
					// If there's no event between now and the next monitoring time, skip monitoring until the next event time. 
					if(nextEventDelay > nextMonitorDelay) {
						nextMonitorDelay = nextEventDelay;	
					}
					
					long numPackets = channelManager.getTotalNumPackets();

					int noEve = CloudSimEx.getNumFutureEvents();

					System.err.println(CloudSim.clock() + ": Elasped time="+ CloudSimEx.getElapsedTimeString()+", "
					+CloudSimEx.getNumFutureEvents()+" more events,"+" # packets="+numPackets+", next monitoring in "+nextMonitorDelay);
					send(this.getId(), nextMonitorDelay, CloudSimTagsSDN.MONITOR_UPDATE_UTILIZATION);
				}
				break;
			default: System.out.println("Unknown event received by "+super.getName()+". Tag:"+ev.getTag());
		}
	}

	protected void processVmCreateAck(SimEvent ev) {
//		SDNVm vm = (SDNVm) ev.getData();
//		Host host = findHost(vm.getId());
//		vm.setSDNHost(host);
	}

	protected void activateSwitches(SimEvent ev) {
		ArrayList<SDNVm> vmList = (ArrayList<SDNVm>) ev.getData();
		for (SDNVm vm : vmList) {
			SDNHost host = (SDNHost) vm.getHost();
			String name = host.getName();
			String[] taskName = name.split("_");
			int k = Integer.parseInt(taskName[1]);
			int i = Integer.parseInt(taskName[2]);
			String edgeSw = "e_" + k + "_" + i;
			String aggSw = "a_" + k + "_" + i;
			for (Switch sw: getSwitchList()) {
				if (sw.getName().equals(edgeSw))
					sw.setActive(true);
				else if (sw.getName().equals(aggSw))
					sw.setActive(true);
			}
		}
	}

	protected void logEnergyConsumption() {
		int utilizedHosts = 0;
		double accEnergy = 0.0;
		for(Host host:getHostList()) {
			// Actual workload based power consumption
			double consumedEnergy = ((SDNHost)host).getConsumedEnergy();
			SDNHost host_ = (SDNHost)host;

			if (consumedEnergy != 0.0) {
				utilizedHosts++;
			}

			//Log.printLine(host_.getType() + " Host #"+host.getId()+": "+consumedEnergy);
			accEnergy += consumedEnergy;
/*
			if (host_.isActive()) {
				Log.printLine(host_.getType() + " Host #"+host.getId()+": "+consumedEnergy);
				hostEnergyConsumption += consumedEnergy;
			} else {
				unused++;
			}
	*/
		}

		int utilizedSwitches = 0;
		double switchEnergyConsumption = 0.0;
		for(Switch sw:getSwitchList()) {
			// Actual workload based power consumption
			double consumedEnergy = sw.getConsumedEnergy();

			if (consumedEnergy != 0.0) {
				utilizedSwitches++;
			}

			//Log.printLine(host_.getType() + " Host #"+host.getId()+": "+consumedEnergy);
			switchEnergyConsumption += consumedEnergy;
	/*
			if (host_.isActive()) {
				Log.printLine(host_.getType() + " Host #"+host.getId()+": "+consumedEnergy);
				hostEnergyConsumption += consumedEnergy;
			} else {
				unused++;
			}
	*/
		}

		if (hostEnergyConsumption > 0) {
			LogWriter serverUtil = LogWriter.getLogger("host_utilization.csv");
			LogWriter serverEnergy = LogWriter.getLogger("host_energy.csv");
			LogWriter swLogUtil = LogWriter.getLogger("switch_utilization.csv");
			LogWriter swEnergy = LogWriter.getLogger("switch_energy.csv");
			swEnergy.printLine(CloudSim.clock() + "," + switchEnergyConsumption);
			swLogUtil.printLine(CloudSim.clock() + "," + utilizedHosts);
			serverUtil.printLine(CloudSim.clock() + "," + hostEnergyConsumption);
			serverEnergy.printLine(CloudSim.clock() + "," + utilizedHosts);
		}

		hostEnergyConsumption = accEnergy;
	}


	protected void processVmCreateDynamicAck(SimEvent ev) {
		
		Object [] data = (Object []) ev.getData();
		SDNVm newVm = (SDNVm) data[0]; 
		boolean result = (boolean) data[1];
		
		if(result) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ".processVmCreateDynamic: Dynamic VM("+newVm+") creation succesful!");
			if(newVm instanceof ServiceFunction)
				sfcForwarder.processVmCreateDyanmicAck((ServiceFunction)newVm);
		}
		else {
			// VM cannot be created here..
			Log.printLine(CloudSim.clock() + ": " + getName() + ".processVmCreateDynamic: Dynamic VM cannot be created!! :"+newVm);
			System.err.println(CloudSim.clock() + ": " + getName() + ".processVmCreateDynamic: Dynamic VM cannot be created!! :"+newVm);
			sfcForwarder.processVmCreateDyanmicFailed((ServiceFunction)newVm);
		}
	}
	
	// Migrate network flow from previous routing
	public void processVmMigrate(Vm vm, SDNHost oldHost, SDNHost newHost) {
		// Find the virtual route associated with the migrated VM
		// VM is already migrated to the new host
		for(FlowConfig flow:this.flowMapVmId2Flow.get(vm.getId())) {
			SDNHost sender = findHost(flow.getSrcId());	// Sender will be the new host after migrated
			if(flow.getSrcId() == vm.getId())
				sender = oldHost;	// In such case, sender should be changed to the old host
			
			vnMapper.rebuildForwardingTable(flow.getSrcId(), flow.getDstId(), flow.getFlowId(), sender);
		}
		
		// Move the transferring data packets in the old channel to the new one.
		migrateChannel(vm, oldHost, newHost);
		
		// Print all routing tables.
//		for(Node node:this.topology.getAllNodes()) {
//			node.printVMRoute();
//		}
	}

	private void processInternalPacketProcessing() {
		if(channelManager.updatePacketProcessing()) {
			sendInternalEvent();
		}
	}
		
	protected void processVmDestroyAck(SimEvent ev) {
		Vm destroyedVm = (Vm) ev.getData();
		// remove all channels transferring data from or to this vm.
		for(Vm vm:this.vmMapId2Vm.values()) {
			channelManager.removeChannel(vm.getId(), destroyedVm.getId(), -1);
			channelManager.removeChannel(destroyedVm.getId(), vm.getId(), -1);
		}
		sendInternalEvent();
	}
	
	protected void processInternalAdjustChannels() {
		channelManager.adjustAllChannel();
	}

	public boolean startDeployApplicatoin() {
		List<Vm> vms = new ArrayList<Vm>(vmMapId2Vm.values());
		// FYI Check if we will be needing SFC in our work.
		List<ServiceFunctionChainPolicy> sfcPolicies = new ArrayList<ServiceFunctionChainPolicy>(sfcForwarder.getAllPolicies());
		boolean result = deployApplication(vms, this.flowMapVmId2Flow.values(), sfcPolicies);
		
		isApplicationDeployed = result;		
		return result;
	}
	
	public Packet addPacketToChannel(Packet orgPkt) {
		Packet pkt = orgPkt;
		/*
		if(sender.equals(sender.getVMRoute(src, dst, flowId))) {
			// For loopback packet (when src and dst is on the same host)
			//Log.printLine(CloudSim.clock() + ": " + getName() + ".addPacketToChannel: Loopback package: "+pkt +". Send to destination:"+dst);
			sendNow(sender.getAddress(),Constants.SDN_PACKAGE,pkt);
			return;
		}
		*/
		if(Configuration.ENABLE_SFC)
			pkt = sfcForwarder.enforceSFC(pkt); 
		
		channelManager.updatePacketProcessing();
		
		int src = pkt.getOrigin();
		int dst = pkt.getDestination();

		/* FYI should we calculate the power consumed by the packet transmission here???
		if (src == dst) {
			// VMs in same host, so we can ignore power consumption. Otherwise consider??? When we consider power consumed by switches this is automatically taken into account I guess...
		}
		*/
		int flowId = pkt.getFlowId();
		
		// Check if VM is removed by auto-scaling
		if(findVmGlobal(src) == null) {
			src = getSFForwarderOriginalVm(src).getId();
			pkt.changeOrigin(src); 
		}		
		if(findVmGlobal(dst) == null) {
			dst = getSFForwarderOriginalVm(dst).getId();
			pkt.changeDestination(dst); 
		}
		
		Channel channel = channelManager.findChannel(src, dst, flowId);
		SDNVm srcVm = (SDNVm)NetworkOperatingSystem.findVmGlobal(src);
		SDNVm dstVm = (SDNVm)NetworkOperatingSystem.findVmGlobal(dst);
		if(channel == null && srcVm.getHost() != dstVm.getHost()) {
			//No channel established. Create a new channel.
			SDNHost sender = findHost(src);
			channel = channelManager.createChannel(src, dst, flowId, sender);
			
			if(channel == null) {
				// failed to create channel
				System.err.println("ERROR!! Cannot create channel!" + pkt);
				return pkt;
			}
			channelManager.addChannel(src, dst, flowId, channel);
		} else {
			// Intra host communication does not require a channel
			return pkt;
		}
		
		channel.addTransmission(new Transmission(pkt));
//		Log.printLine(CloudSim.clock() + ": " + getName() + ".addPacketToChannel ("+channel
//				+"): Transmission added:" + 
//				NetworkOperatingSystem.getVmName(src) + "->"+
//				NetworkOperatingSystem.getVmName(dst) + ", flow ="+flowId + " / eft="+eft);

		sendInternalEvent();
		
		return pkt;
	}

	public void processCompletePackets(List<Channel> channels){
		for(Channel ch:channels) {
			for (Transmission tr:ch.getArrivedPackets()){
				Packet pkt = tr.getPacket();
				int vmId = pkt.getDestination(); 
				Datacenter dc = SDNDatacenter.findDatacenterGlobal(vmId);
				
				//Log.printLine(CloudSim.clock() + ": " + getName() + ": Packet completed: "+pkt +". Send to destination:"+ch.getLastNode());
				sendPacketCompleteEvent(dc, pkt, ch.getTotalLatency());
			}
			
			for (Transmission tr:ch.getFailedPackets()){
				Packet pkt = tr.getPacket();
				sendPacketFailedEvent(this.datacenter, pkt, ch.getTotalLatency());
			}
		}
	}
	
	private void sendPacketCompleteEvent(Datacenter dc, Packet pkt, double latency){
		send(dc.getId(), latency, CloudSimTagsSDN.SDN_PACKET_COMPLETE, pkt);
	}

	private void sendPacketFailedEvent(Datacenter dc, Packet pkt, double latency){
		send(dc.getId(), latency, CloudSimTagsSDN.SDN_PACKET_FAILED, pkt);
	}

	public void sendAdjustAllChannelEvent() {
		if(CloudSim.clock() != lastAdjustAllChannelTime) {
			send(getId(), 0, CloudSimTagsSDN.SDN_INTERNAL_CHANNEL_PROCESS);
			lastAdjustAllChannelTime = CloudSim.clock();
		}
	}

	private void sendInternalEvent() {
		if(channelManager.getTotalChannelNum() != 0) {
			if(nextEventTime == CloudSim.clock() + CloudSim.getMinTimeBetweenEvents())
				return;
			
			// More to process. Send event again
			double delay = channelManager.nextFinishTime();

			if (delay < CloudSim.getMinTimeBetweenEvents()) { 
				//Log.printLine(CloudSim.clock() + ":Channel: delay is too short: "+ delay);
				delay = CloudSim.getMinTimeBetweenEvents();
			}

			//Log.printLine(CloudSim.clock() + ": " + getName() + ".sendInternalEvent(): delay for next event="+ delay);

			if((nextEventTime > CloudSim.clock() + delay) || nextEventTime <= CloudSim.clock() ) 
			{
				//Log.printLine(CloudSim.clock() + ": " + getName() + ".sendInternalEvent(): next event time changed! old="+ nextEventTime+", new="+(CloudSim.clock()+delay));
				
				CloudSim.cancelAll(getId(), new PredicateType(CloudSimTagsSDN.SDN_INTERNAL_PACKET_PROCESS));
				send(this.getId(), delay, CloudSimTagsSDN.SDN_INTERNAL_PACKET_PROCESS);
				nextEventTime = CloudSim.clock()+delay;
			}
		}
	}

	public void updateChannelBandwidth(int src, int dst, int flowId, long newBandwidth) {
		if(channelManager.updateChannelBandwidth(src, dst, flowId, newBandwidth)) {
			// As the requested bandwidth updates, find alternative path if the current path cannot provide the new bandwidth.
			SDNHost sender = findHost(src);
			vnMapper.updateDynamicForwardingTableRec(sender, src, dst, flowId, false);

			sendAdjustAllChannelEvent();
		}
	}
	
	private void migrateChannel(Vm vm, SDNHost oldHost, SDNHost newHost) {
		for(Channel ch:channelManager.findAllChannels(vm.getId())) {
			List<Node> nodes = new ArrayList<Node>();
			List<Link> links = new ArrayList<Link>();

			SDNHost sender = findHost(ch.getSrcId());	// After migrated
			
			vnMapper.buildNodesLinks(ch.getSrcId(), ch.getDstId(), 
					ch.getChId(), sender, nodes, links);
			
			// update with the new nodes and links
			ch.updateRoute(nodes, links);			
		}
	}

	public void addExtraVm(SDNVm vm, NetworkOperatingSystem callback) {
		vmMapId2Vm.put(vm.getId(), vm);
		gvmMapId2Vm.put(vm.getId(), vm);
		
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Add extra VM #" + vm.getId()
			+ " in " + datacenter.getName() + ", (" + vm.getStartTime() + "~" +vm.getFinishTime() + ")");
		
		Object[] data = new Object[2];
		data[0] = vm;
		data[1] = callback;
		
		send(datacenter.getId(), vm.getStartTime(), CloudSimTagsSDN.SDN_VM_CREATE_DYNAMIC, data);
	}
	
	public void removeExtraVm(SDNVm vm) {
		vmMapId2Vm.remove(vm.getId());
		gvmMapId2Vm.remove(vm.getId());
		
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Remove extra VM #" + vm.getId()
			+ " in " + datacenter.getName() + ", (" + vm.getStartTime() + "~" +vm.getFinishTime() + ")");
		
		send(datacenter.getId(), vm.getStartTime(), CloudSimTags.VM_DESTROY, vm);
	}
	
	public void addExtraPath(int orgVmId, int newVmId) {
		List<FlowConfig> newFlowList = new ArrayList<FlowConfig>();
		// This function finds all Flows involving orgVmId and add another virtual path for newVmId. 
		for(FlowConfig flow:this.flowMapVmId2Flow.get(orgVmId)) {
			int srcId = flow.getSrcId();
			int dstId = flow.getDstId();
			int flowId = flow.getFlowId();
			
			// Replace the source or destination with the new VM
			if(srcId == orgVmId)
				srcId = newVmId;
			if(dstId == orgVmId)
				dstId = newVmId;
			if(findVmGlobal(srcId) == null || findVmGlobal(dstId) == null)
				continue;
			
			FlowConfig extraFlow = new FlowConfig(srcId, dstId, flowId, flow.getBw(), flow.getLatency());
			newFlowList.add(extraFlow);
			
			if(vnMapper.buildForwardingTable(srcId, dstId, flowId) == false) {
				throw new RuntimeException("Cannot build a forwarding table!");
			}
		}
		
		for(FlowConfig flow:newFlowList)
			insertFlowToMap(flow);
	}
	
	public void updateVmMips(SDNVm orgVm, int newPe, double newMips) {
		Host host = orgVm.getHost();
		this.datacenter.getVmAllocationPolicy().deallocateHostForVm(orgVm);
		
		orgVm.updatePeMips(newPe, newMips);
		if(!this.datacenter.getVmAllocationPolicy().allocateHostForVm(orgVm, host)) {
			System.err.println("ERROR!! VM cannot be resized! "+orgVm+" (new Pe "+newPe+", Mips "+newMips+") in host: "+host);
			System.exit(-1);
		}
	}
	
	public long getRequestedBandwidth(int flowId) {
		FlowConfig flow = gFlowMapFlowId2Flow.get(flowId);
		if(flow != null)
			return flow.getBw();

		return 0L;
	}	
	
	public double getRequestedBandwidth(Packet pkt) {
		int src = pkt.getOrigin();
		int dst = pkt.getDestination();
		int flowId = pkt.getFlowId();
		Channel channel=channelManager.findChannel(src, dst, flowId);

		SDNVm srcVm = (SDNVm)NetworkOperatingSystem.findVmGlobal(src);
		SDNVm dstVm = (SDNVm)NetworkOperatingSystem.findVmGlobal(dst);
		if(channel == null && srcVm.getHost() == dstVm.getHost()) {
			// Intra host communication does not require a channel
			return 0;
		}
		double bw = channel.getRequestedBandwidth();
		
		return bw;
	}
	
	public void updateBandwidthFlow(int srcVm, int dstVm, int flowId, long newBw) {
		if(flowId == -1) {
			return;
		}
		
		FlowConfig flow = gFlowMapFlowId2Flow.get(flowId);
		flow.updateReqiredBandwidth(newBw);
	}

	public void setDatacenter(SDNDatacenter dc) {
		this.datacenter = dc;
	}
	
	@Override
	public String toString() {
		return "NOS:"+getName();
	}

	public static Map<String, Integer> getVmNameToIdMap() {
		Map<String, Integer> map = new HashMap<>();
		for(Vm vm:gvmMapId2Vm.values()) {
			SDNVm svm = (SDNVm)vm;
			map.put(svm.getName(), svm.getId());
		}
			
		return map;
	}
	
	public static Map<String, Integer> getFlowNameToIdMap() {
		Map<String, Integer> map = new HashMap<String, Integer>();
		for(FlowConfig flow:gFlowMapFlowId2Flow.values()) {
			map.put(flow.getName(), flow.getFlowId());
		}
		
		map.put("default", -1);

		return map;
	}
	
	public PhysicalTopology getPhysicalTopology() {
		return this.topology;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Host> List<T> getHostList() {
		return (List<T>)topology.getAllHosts();
	}
		
	public List<Switch> getSwitchList() {
		return (List<Switch>) topology.getAllSwitches();
	}

	public boolean isApplicationDeployed() {
		return isApplicationDeployed;
	}

	public Vm findVmLocal(int vmId) {
		return vmMapId2Vm.get(vmId);
	}
	
	public static String getVmName(int vmId) {
		SDNVm vm = (SDNVm) gvmMapId2Vm.get(vmId);
		return vm.getName();
	}
	
	public static Vm findVmGlobal(int vmId) {
		return gvmMapId2Vm.get(vmId);
	}

	// AMANDAAAA this doesn't get updated after vm migration!
	public SDNHost findHost(int vmId) {
		Vm vm = findVmLocal(vmId);
		if(vm != null) {
			// VM is in this NOS (datacenter)
			// AMANDAAAAA alter here
			return (SDNHost)this.datacenter.getTaskVmAllocationPolicy().getHost(vm);
		}
		
		// VM is in another data center. Find the host!
		vm = findVmGlobal(vmId);
		if(vm != null) {
			Datacenter dc = SDNDatacenter.findDatacenterGlobal(vmId);
			if(dc != null)
				return (SDNHost)dc.getVmAllocationPolicy().getHost(vm);
		}
		
		return null;
	}
	
	public void addVm(SDNVm vm) {
		vmMapId2Vm.put(vm.getId(), vm);
		gvmMapId2Vm.put(vm.getId(), vm);
	}
	
	private void insertFlowToMap(FlowConfig flow) {
		flowMapVmId2Flow.put(flow.getSrcId(), flow);
		flowMapVmId2Flow.put(flow.getDstId(), flow);
	}
	
	public void addFlow(FlowConfig flow) {
		insertFlowToMap(flow);
		
		if(flow.getFlowId() != -1) {
			gFlowMapFlowId2Flow.put(flow.getFlowId(), flow);
		}
	}
	
	public void addSFCPolicy(ServiceFunctionChainPolicy policy) {
		sfcForwarder.addPolicy(policy);
		List<FlowConfig> extraFlows = createExtraFlowSFCPolicy(policy);
		for(FlowConfig flow:extraFlows)
			insertFlowToMap(flow);
	}
	
	private List<FlowConfig> createExtraFlowSFCPolicy(ServiceFunctionChainPolicy policy) {
		// Add extra Flow for ServiceFunctionChain
		
		List<FlowConfig> flowList = new LinkedList<FlowConfig>();
		int flowId = policy.getFlowId();
		
		long bw = 0;
		double latency = 0.0;
		
		if(flowId != -1)
		{
			FlowConfig orgFlow = gFlowMapFlowId2Flow.get(flowId);
			bw = orgFlow.getBw();
			latency = orgFlow.getLatency();
		}
		
		List<Integer> vmIds = policy.getServiceFunctionChainIncludeVM();
		for(int i=0; i < vmIds.size()-1; i++) {
			// Build channel chain: SrcVM ---> SF1 ---> SF2 ---> DstVM
			int fromId = vmIds.get(i);
			int toId = vmIds.get(i+1);
			
			FlowConfig sfcFlow = new FlowConfig(fromId, toId, flowId, bw, latency);
			flowList.add(sfcFlow);
		}

		policy.setInitialBandwidth(bw);
		return flowList;
	}

	// for monitoring
	private void updateBWMonitor(double monitoringTimeUnit) {
		double highest=0;
		// Update utilization of all links
		Set<Link> links = new HashSet<Link>(this.topology.getAllLinks());
		for(Link l:links) {
			double util = l.updateMonitor(CloudSim.clock(), monitoringTimeUnit);
			if(util > highest) highest=util;
		}
		//System.err.println(CloudSim.clock()+": Highest utilization of Links = "+highest);
		
		channelManager.updateMonitor(monitoringTimeUnit);
	}

	private void updateHostMonitor(double monitoringTimeUnit) {
		double hostTotalEnergy = 0.0;
		int activeHosts = 0;
		if(datacenter != null)
			for(SDNHost h: datacenter.<SDNHost>getHostList()) {
				hostTotalEnergy += h.updateMonitor(CloudSim.clock(), monitoringTimeUnit);
				if (h.isActive())
					activeHosts++;
			}
		if (activeHosts > 0) {
			LogWriter serverUtil = LogWriter.getLogger("host_utilization.csv");
			LogWriter serverEnergy = LogWriter.getLogger("host_energy.csv");
			serverUtil.printLine(CloudSim.clock() + "," + activeHosts);
			serverEnergy.printLine(CloudSim.clock() + "," + hostTotalEnergy);
		}
		if (CloudSim.clock() > 200 && !printedFinal) {
			LogWriter util = LogWriter.getLogger("total_utilization.txt");
			int utilizedHosts = 0;
			int utilizedSwitches = 0;
			double hostEnergy = 0.0;
			double swEnergy = 0.0;
			for(Host host:getHostList()) {
				// Actual workload based power consumption
				double consumedEnergy = ((SDNHost)host).getConsumedEnergy();
				if (consumedEnergy != 0.0) {
					utilizedHosts++;
				}
				hostEnergy += consumedEnergy;
			}

			for (Switch sw: getSwitchList()) {
				double consumedEnergy = sw.getConsumedEnergy();
				if (consumedEnergy != 0.0) {
					utilizedSwitches++;
				}
				swEnergy += consumedEnergy;
			}
			util.printLine(CloudSim.clock() + " Total Server Energy Consumption: " + hostEnergy);
			util.printLine(CloudSim.clock() + " Total Switch Energy Consumption: " + swEnergy);
			util.printLine(CloudSim.clock() + " Total Switch Utilization: " + utilizedSwitches);
			util.printLine(CloudSim.clock() + " Total Host Utilization: " + utilizedHosts);
			printedFinal = true;
		}
	}
	
	private void updateSwitchMonitor(double monitoringTimeUnit) {
		double switchTotalEnergy = 0.0;
		int activeSwitches = 0;
		for(Switch s:getSwitchList()) {
			double swEnergy = s.updateMonitor(CloudSim.clock(), monitoringTimeUnit);
			switchTotalEnergy += swEnergy;
			if (swEnergy > 0)
				activeSwitches++;
		}
		if (switchTotalEnergy > 0) {
			LogWriter swLogUtil = LogWriter.getLogger("switch_utilization.csv");
			LogWriter swEnergy = LogWriter.getLogger("switch_energy.csv");
			swEnergy.printLine(CloudSim.clock() + "," + switchTotalEnergy);
			swLogUtil.printLine(CloudSim.clock() + "," + activeSwitches);
		}
	}
	
	private void updateVmMonitor(double logTime) {
		if(datacenter == null)
			return;
		
		VmAllocationPolicy vmAlloc = datacenter.getVmAllocationPolicy();
		if(vmAlloc instanceof OverbookingVmAllocationPolicy) {
			for(Vm v: this.vmMapId2Vm.values()) {
				SDNVm vm = (SDNVm)v;
				double mipsOBR = ((OverbookingVmAllocationPolicy)vmAlloc).getCurrentOverbookingRatioMips((SDNVm) vm);
				LogWriter log = LogWriter.getLogger("vm_OBR_mips.csv");
				log.printLine(vm.getName()+","+logTime+","+mipsOBR);
				
				double bwOBR =  ((OverbookingVmAllocationPolicy)vmAlloc).getCurrentOverbookingRatioBw((SDNVm) vm);
				log = LogWriter.getLogger("vm_OBR_bw.csv");
				log.printLine(vm.getName()+","+logTime+","+bwOBR);
			}
		}
	}
		
	public Vm getSFForwarderOriginalVm(int vmId) {
		return this.sfcForwarder.getOriginalSF(vmId);
	}

	public double calculateLatency(int srcVmId, int dstVmId, int flowId) {
		List<Node> nodes = new ArrayList<Node>();
		List<Link> links = new ArrayList<Link>();
		Node srcHost = findHost(srcVmId);
		vnMapper.buildNodesLinks(srcVmId, dstVmId, flowId, srcHost, nodes, links);
		
		double latency = 0;
		// Calculate the latency of the links.
		for(Link l:links) {
			latency += l.getLatencyInSeconds(); 
		}

		return latency;
	}

	/*
	protected void debugPrintMonitoredValues() {
		//////////////////////////////////////////////////////////////		
		//////////////////////////////////////////////////////////////
		// For debug only
		
		Collection<Link> links = this.topology.getAllLinks();
		for(Link l:links) {
			System.err.println(l);
			MonitoringValues mv = l.getMonitoringValuesLinkUtilizationUp();
			System.err.print(mv);
			mv = l.getMonitoringValuesLinkUtilizationDown();
			System.err.print(mv);
		}
//		
//		for(Channel ch:this.allChannels) {
//			System.err.println(ch);
//			MonitoringValues mv = ch.getMonitoringValuesLinkUtilization();
//			System.err.print(mv);
//		}
		
		for(SDNHost h:datacenter.<SDNHost>getHostList()) {
			System.err.println(h);
			MonitoringValues mv = h.getMonitoringValuesHostCPUUtilization();
			System.err.print(mv);			
		}

		for(Vm vm:vmMapId2Vm.values()) {
			SDNVm tvm = (SDNVm)vm;
			System.err.println(tvm);
			MonitoringValues mv = tvm.getMonitoringValuesVmCPUUtilization();
			System.err.print(mv);			
		}
	}
	*/

	public boolean deployFlows(SimEvent ev) {
		List<SDNVm> taskVmList =(List<SDNVm>)ev.getData();


		Task vmTask = datacenter.getTaskVmAllocationPolicy().getTaskIdOfTheInstanceInVm(taskVmList.get(0));
		ArrayList<Task> predecessorTasks = vmTask.getPredecessorTasks();

		/* Use this for Alibaba traces */
		/*
		for (int i = 0; i < predecessorTasks.size(); i++) {
			Task predecessor = predecessorTasks.get(i);
			ArrayList<SDNHost> hostList = new ArrayList<>();
			if (predecessor.getMessageVol() == 0)
				continue;
			predecessor.getInstanceHostMap().forEach((instanceVm,host) -> {
				if (hostList.indexOf(host) == -1)
					hostList.add(host);
			});

			WorkloadParser workParser = new WorkloadParser(this.getId(), new UtilizationModelFull(),
			NetworkOperatingSystem.getVmNameToIdMap(), NetworkOperatingSystem.getFlowNameToIdMap());

			hostList.forEach(host -> {
				// create a mock vm in the host
				SDNVm mockVm = new SDNVm(mockid++, taskVmList.get(0).getUserId(),10,1,100,100,100,"VMM", new CloudletSchedulerTimeShared(), 0, 0);
				mockVm.setHost(host);
				mockVm.setHostName(host.getName());
				addVm(mockVm);
				datacenter.getTaskVmAllocationPolicy().getVmTable().put(mockVm.getUid(), mockVm.getHost());

				taskVmList.forEach(vm -> {
					FlowConfig arc = new FlowConfig(mockVm.getId(), vm.getId(), -1, 0, 0.0);
					addFlow(arc);
					vnMapper.buildForwardingTable(arc.getSrcId(), arc.getDstId(), arc.getFlowId());
					Workload wl = workParser.generateWorkload(mockVm.getId(), vm.getId());
					sendNow(this.datacenter.getId(), CloudSimTagsSDN.REQUEST_SUBMIT, wl.request);
				});
			});
		}
*/

		// Get the input file list
		// For each file, iterate through output files of predecessors and see which one outputs the particular input... if none that means it's a fresh file.. so no comm required
		ArrayList<SDNHost> hostList = new ArrayList<>();
		Map<String, Long> inpFiles = vmTask.getInputFiles();
		for (String file: inpFiles.keySet()) {
			for (int i = 0; i < predecessorTasks.size(); i++) {
				Task predecessor = predecessorTasks.get(i);
				Map<String, Long> outFiles = predecessor.getOutputFiles();
				for (String outfile: outFiles.keySet()) {
					if (file.equals(outfile)) {
						predecessor.getInstanceHostMap().forEach((instanceVm,host) -> {
							if (hostList.indexOf(host) == -1)
								hostList.add(host);
						});
						break;
					}
				}
			}

			WorkloadParser workParser = new WorkloadParser(this.getId(), new UtilizationModelFull(),
					NetworkOperatingSystem.getVmNameToIdMap(), NetworkOperatingSystem.getFlowNameToIdMap());

			hostList.forEach(host -> {
				// create a mock vm in the host
				SDNVm mockVm = new SDNVm(mockid++, taskVmList.get(0).getUserId(),10,1,100,100,100,"VMM", new CloudletSchedulerTimeShared(), 0, 0);
				mockVm.setHost(host);
				mockVm.setHostName(host.getName());
				addVm(mockVm);
				datacenter.getTaskVmAllocationPolicy().getVmTable().put(mockVm.getUid(), mockVm.getHost());

				taskVmList.forEach(vm -> {
					FlowConfig arc = new FlowConfig(mockVm.getId(), vm.getId(), -1, 0, 0.0);
					addFlow(arc);
					vnMapper.buildForwardingTable(arc.getSrcId(), arc.getDstId(), arc.getFlowId());
					Workload wl = workParser.generateWorkload(mockVm.getId(), vm.getId());
					sendNow(this.datacenter.getId(), CloudSimTagsSDN.REQUEST_SUBMIT, wl.request);
				});
			});
		}

		return true;
	}
}
