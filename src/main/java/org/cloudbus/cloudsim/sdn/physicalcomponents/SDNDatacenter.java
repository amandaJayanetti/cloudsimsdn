/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2015, The University of Melbourne, Australia
 */
package org.cloudbus.cloudsim.sdn.physicalcomponents;

import java.util.*;

import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.provisioners.BwProvisioner;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisioner;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.*;
import org.cloudbus.cloudsim.sdn.Packet;
import org.cloudbus.cloudsim.sdn.nos.NetworkOperatingSystem;
import org.cloudbus.cloudsim.sdn.policies.vmallocation.VmAllocationInGroup;
import org.cloudbus.cloudsim.sdn.policies.vmallocation.VmAllocationPolicyPriorityFirst;
import org.cloudbus.cloudsim.sdn.policies.vmallocation.VmGroup;
import org.cloudbus.cloudsim.sdn.virtualcomponents.FlowConfig;
import org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager.Job;
import org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager.VmAllocationPolicyToTasks;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer.Task;
import org.cloudbus.cloudsim.sdn.workload.Activity;
import org.cloudbus.cloudsim.sdn.workload.Processing;
import org.cloudbus.cloudsim.sdn.workload.Request;
import org.cloudbus.cloudsim.sdn.workload.Transmission;

/**
 * Extended class of Datacenter that supports processing SDN-specific events.
 * In addtion to the default Datacenter, it processes Request submission to VM,
 * and application deployment request. 
 * 
 * @author Jungmin Son
 * @author Rodrigo N. Calheiros
 * @since CloudSimSDN 1.0
 */
public class SDNDatacenter extends Datacenter {
	private NetworkOperatingSystem nos;
	private HashMap<Integer,Request> requestsTable = new HashMap<Integer, Request>();
	private static HashMap<Integer,Datacenter> globalVmDatacenterMap = new HashMap<Integer, Datacenter>();

	public void setIsMigrateEnabled(boolean isMigrateEnabled) {
		SDNDatacenter.isMigrateEnabled = isMigrateEnabled;
	}

	private static boolean isMigrateEnabled = false;
	private static int hostIndex = 0;

	/** The vm provisioner. */
	private VmAllocationPolicyToTasks taskVmAllocationPolicy;

	/**
	 * Gets the vm allocation policy.
	 *
	 * @return the vm allocation policy
	 */
	public VmAllocationPolicyToTasks getTaskVmAllocationPolicy() {
		return taskVmAllocationPolicy;
	}

	/**
	 * Sets the vm allocation policy.
	 *
	 * @param vmAllocationPolicy the new vm allocation policy
	 */
	public void setTaskVmAllocationPolicy(VmAllocationPolicyToTasks vmAllocationPolicy) {
		this.taskVmAllocationPolicy =  vmAllocationPolicy;
	}

	// For results
	public static int migrationCompleted = 0;
	public static int migrationAttempted = 0;

	
	public SDNDatacenter(String name, DatacenterCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList, double schedulingInterval, NetworkOperatingSystem nos) throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
		
		this.nos=nos;
		
		//nos.init();
		if(vmAllocationPolicy instanceof VmAllocationPolicyPriorityFirst) {
			((VmAllocationPolicyPriorityFirst)vmAllocationPolicy).setTopology(nos.getPhysicalTopology());
		}
	}

	public static Datacenter findDatacenterGlobal(int vmId) {
		// Find a data center where the VM is placed
		return globalVmDatacenterMap.get(vmId);
	}

	public void addVm(Vm vm){
		getVmList().add(vm);
		if (vm.isBeingInstantiated()) vm.setBeingInstantiated(false);
		vm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(vm).getVmScheduler().getAllocatedMipsForVm(vm));
	}
		
	@Override
	protected void processVmCreate(SimEvent ev, boolean ack) {
		// AMANDAAAA commented this out
		//processVmCreateEvent((SDNVm) ev.getData(), ack);
		if(ack) {
			Vm vm = (Vm)ev.getData();
			send(nos.getId(), 0/*CloudSim.getMinTimeBetweenEvents()*/, CloudSimTags.VM_CREATE_ACK, vm); // This event is redundant as there's mo implementation for it
		}
	}
	
	protected boolean processVmCreateEvent(SDNVm vm, boolean ack) {
		boolean result = getVmAllocationPolicy().allocateHostForVm(vm);

		if (ack) {
			int[] data = new int[3];
			data[0] = getId();
			data[1] = vm.getId();

			if (result) {
				data[2] = CloudSimTags.TRUE;
			} else {
				data[2] = CloudSimTags.FALSE;
			}
			send(vm.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTags.VM_CREATE_ACK, data);
		}

		if (result) {
			globalVmDatacenterMap.put(vm.getId(), this);
			
			getVmList().add(vm);

			if (vm.isBeingInstantiated()) {
				vm.setBeingInstantiated(false);
			}

			vm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(vm).getVmScheduler()
					.getAllocatedMipsForVm(vm));
		}

		return result;
	}
	
	protected boolean processVmCreateDynamic(SimEvent ev) {
		Object[] data = (Object[]) ev.getData();
		SDNVm vm = (SDNVm) data[0];
		NetworkOperatingSystem callbackNOS = (NetworkOperatingSystem) data[1];

		boolean result = processVmCreateEvent(vm, true);
		data[0] = vm;
		data[1] = result;
		send(callbackNOS.getId(), 0/*CloudSim.getMinTimeBetweenEvents()*/, CloudSimTagsSDN.SDN_VM_CREATE_DYNAMIC_ACK, data);
		
		return result;
	}

	protected void processVmCreateInGroup(SimEvent ev, boolean ack) {
		@SuppressWarnings("unchecked")
		List<Object> params =(List<Object>)ev.getData();
		
		Vm vm = (Vm)params.get(0);
		VmGroup vmGroup=(VmGroup)params.get(1);

		boolean result = ((VmAllocationInGroup)getVmAllocationPolicy()).allocateHostForVmInGroup(vm, vmGroup);

		if (ack) {
			int[] data = new int[3];
			data[0] = getId();
			data[1] = vm.getId();

			if (result) {
				data[2] = CloudSimTags.TRUE;
			} else {
				data[2] = CloudSimTags.FALSE;
			}
			send(vm.getUserId(), 0, CloudSimTags.VM_CREATE_ACK, data);
			send(nos.getId(), 0, CloudSimTags.VM_CREATE_ACK, vm);
		}

		if (result) {
			getVmList().add(vm);

			if (vm.isBeingInstantiated()) {
				vm.setBeingInstantiated(false);
			}

			vm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(vm).getVmScheduler()
					.getAllocatedMipsForVm(vm));
		}
	}	
	
	@Override
	protected void processVmMigrate(SimEvent ev, boolean ack) {
		migrationCompleted++;
		
		// Change network routing.
		@SuppressWarnings("unchecked")
		Map<String, Object> migrate = (HashMap<String, Object>) ev.getData();

		Vm vm = (Vm) migrate.get("vm");
		Host newHost = (Host) migrate.get("host");
		Host oldHost = vm.getHost();

		// Migrate the VM to another host.
		//super.processVmMigrate(ev, ack);
		processVmMigrate_(ev, ack);

		// AMANDAAAAAA
		nos.processVmMigrate(vm, (SDNHost)oldHost, (SDNHost)newHost);
	}


	protected void processVmMigrate_(SimEvent ev, boolean ack) {
		Object tmp = ev.getData();
		if (!(tmp instanceof Map<?, ?>)) {
			throw new ClassCastException("The data object must be Map<String, Object>");
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> migrate = (HashMap<String, Object>) tmp;

		Vm vm = (Vm) migrate.get("vm");
		Host currHost = vm.getHost();
		Host host = (Host) migrate.get("host");

		getTaskVmAllocationPolicy().deallocateHostForVm(vm);
		host.removeMigratingInVm(vm);
		boolean result = getTaskVmAllocationPolicy().allocateHostForVm(vm, host);
		if (!result) {
			Log.printLine("[Datacenter.processVmMigrate] VM allocation to the destination host failed");
			// Allocate vm back to prev host
			getTaskVmAllocationPolicy().allocateHostForVm(vm, currHost);
			//System.exit(0);
		} else {
			if (ack) {
				int[] data = new int[3];
				data[0] = getId();
				data[1] = vm.getId();

				if (result) {
					data[2] = CloudSimTags.TRUE;
				} else {
					data[2] = CloudSimTags.FALSE;
				}
				sendNow(ev.getSource(), CloudSimTags.VM_CREATE_ACK, data);
			}

			Log.formatLine(
					"%.2f: Migration of VM #%d to Host #%d is completed",
					CloudSim.clock(),
					vm.getId(),
					host.getId());
			vm.setInMigration(false);
			nos.addVm((SDNVm)vm);
		}
	}
	
	@Override
	public void processOtherEvent(SimEvent ev){
		switch(ev.getTag()){
			case CloudSimTagsSDN.REQUEST_SUBMIT: 
				processRequestSubmit((Request) ev.getData());
				break;
			case CloudSimTagsSDN.SDN_PACKET_COMPLETE: 
				processPacketCompleted((Packet)ev.getData()); 
				break;
			case CloudSimTagsSDN.SDN_PACKET_FAILED: 
				processPacketFailed((Packet)ev.getData()); 
				break;
			case CloudSimTagsSDN.SDN_VM_CREATE_IN_GROUP:
				processVmCreateInGroup(ev, false); 
				break;
			case CloudSimTagsSDN.SDN_VM_CREATE_IN_GROUP_ACK: 
				processVmCreateInGroup(ev, true); 
				break;
			case CloudSimTagsSDN.SDN_VM_CREATE_DYNAMIC:
				processVmCreateDynamic(ev);
				break;
			case CloudSimTagsSDN.TASK_CREATE_ACK:
				processTaskCreate(ev, false);
				break;
			case CloudSimTagsSDN.TASK_VM_CREATE_ACK:
				processTaskCreate(ev, false);
				break;
			case CloudSimTagsSDN.NEW_TASK_ASSIGN:
				allocateHostsToTaskGroup(ev, false);
				break;
			case CloudSimTagsSDN.SDN_VM_CREATE:
				processSDNVmCreate(ev, false);
				break;
			default: 
				System.out.println("Unknown event recevied by SdnDatacenter. Tag:"+ev.getTag());
		}
	}

	protected void allocateHostsToTaskGroup(SimEvent ev, boolean ack) {
		Map<SDNVm, SDNHost> assignmentMap = (Map<SDNVm, SDNHost>) ev.getData();
		ArrayList<Task> failedTasks = new ArrayList<>();
		for (SDNVm vm: assignmentMap.keySet()) {
			Task task = taskVmAllocationPolicy.getTaskIdOfTheInstanceInVm(vm);
			boolean success = taskVmAllocationPolicy.allocateHostForVm(vm, assignmentMap.get(vm));
			if (success) {
				task.getScheduledInstances().add(vm);
				task.getPendingInstances().remove(vm);

				Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
						+ " in " + this.getName() + ", (" + vm.getStartTime() + "~" + vm.getFinishTime() + ")");

				globalVmDatacenterMap.put(vm.getId(), this);

				getVmList().add(vm);
				if (vm.isBeingInstantiated()) {
					vm.setBeingInstantiated(false);
				}

				vm.updateVmProcessing(CloudSim.clock(), getTaskVmAllocationPolicy().getHost(vm).getVmScheduler()
						.getAllocatedMipsForVm(vm));

				send(this.getId(), 0, CloudSimTags.VM_CREATE_ACK, vm);

				ArrayList<SDNVm> successVmList = new ArrayList<>();
				successVmList.add(vm);
				// AMANDAAAA deploy links to initiate communication among these VMs....
				sendNow(nos.getId(), CloudSimTagsSDN.DEPLOY_TASK_COMM, successVmList);
			} else {
				failedTasks.add(task);
			}
		}
		if (failedTasks.size() > 0) {
			send(this.getId(), 0.0, CloudSimTagsSDN.TASK_CREATE_ACK, failedTasks);
		}
	}

	public void enReal(ArrayList<Task> tasks) {
		Map<SDNHost, List<SDNVm>> currAllocMap = new HashMap<>();
		List<SDNHost> hosts = getHostList();
		List<Task> failedTasks = new ArrayList<>();
		Collections.sort(hosts, new Comparator<SDNHost>() {
			public int compare(SDNHost o1, SDNHost o2) {
				return (int) (o1.getBaselineEnergyConsumption() - o2.getBaselineEnergyConsumption());
				//return (int) (o2.getPerformancePerWatt(o2.getTotalMips(), o2) - o1.getPerformancePerWatt(o1.getTotalMips(),o1));
			}
		});

		long totalReqStorage = 0;
		long totalReqBw = 0;
		long totalReqMips = 0;
		int totalReqRam = 0;
		int totalReqPes = 0;

		//Compute total MIS required by new tasks
		ArrayList<Task> total = new ArrayList<>();
		for (int j = 0; j < tasks.size(); j++) {
			totalReqMips += tasks.get(j).getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum();
			taskVmAllocationPolicy.getTaskVmMap().put(tasks.get(j), tasks.get(j).getInstances());
			int finalJ = j;
			tasks.get(j).getInstances().forEach(instance -> {
				taskVmAllocationPolicy.getTaskVmIdMap().put(tasks.get(finalJ), instance.getId());
			});
		}

		long totalMIPS = 0;
		boolean reqMet = false;
		ArrayList<SDNHost> selectedHostList = new ArrayList<>();
		for (int i = 0; i < hosts.size(); i++) {
			SDNHost host = (SDNHost) hosts.get(i);
			if (!host.isActive())
				continue;
			totalMIPS += host.getAvailableMips();
			selectedHostList.add(host);
			if (totalMIPS >= totalReqMips) {
				reqMet = true;
				break;
			}
		}

		if (!reqMet) {
			// Use idle PMs
			for (int i = 0; i < hosts.size(); i++) {
				SDNHost host = (SDNHost) hosts.get(i);
				if (selectedHostList.indexOf(host) == -1) {
					totalMIPS += host.getAvailableMips();
					selectedHostList.add(host);
					if (totalMIPS >= totalReqMips) {
						break;
					}
				}
			}
		}

		//Compute curr active tasks on selected host lists
		ArrayList<Task> currActiveTasks = new ArrayList<>();
		for (int j = 0; j < selectedHostList.size(); j++) {
			selectedHostList.get(j).getVmList().forEach(vm -> {
				Task vmtask = taskVmAllocationPolicy.getTaskIdOfTheInstanceInVm((SDNVm)vm);
				if (currActiveTasks.indexOf(vmtask) == -1) {
					currActiveTasks.add(vmtask);
					/*
					ListIterator<SDNVm> itr = vmtask.getScheduledInstances().listIterator();
					while (itr.hasNext()) {
						if (itr.next() == vm) {
							// Adding the task back to a pending instance...
							vmtask.getPendingInstances().add((SDNVm)vm);
							itr.remove();
						}
					}
					*/
				}
			});
		}

		// Merge curr active tasks with new arriving tasks
		currActiveTasks.addAll(tasks);

		// sort curr active tasks in decreasing order of required mips
		Collections.sort(currActiveTasks, new Comparator<Task>() {
			public int compare(Task o1, Task o2) {
				return (int) (o2.getInstances().stream().mapToLong(SDNVm::getTotalMips).sum() - o1.getInstances().stream().mapToLong(SDNVm::getTotalMips).sum());
			}
		});

		/*
		// deallocate all the vms in selected host list
		selectedHostList.forEach(host -> {
			host.vmDestroyAll();
		});
		*/

		// Create a mock copy of the selected hostlist to find the new mappings

		// Since EnReal mandates that all VM requests of a task should be allocated to the same PM
		// create a mock VM with cumulative capacity... example for two instances of a task each with 1000 MIs and 4 pes... create one instance (vm) with 1000 MIS and 8 pes
		Map <Task, SDNVm> mockVmToActual = new HashMap<>();
		CloudletScheduler clSch = new CloudletSchedulerSpaceSharedMonitor(Configuration.TIME_OUT);

		currActiveTasks.forEach(task -> {
			SDNVm referenceVm = task.getInstances().get(0);
			OptionalDouble maxMipsReq = task.getInstances().stream().mapToDouble(SDNVm::getMips).max();
			int pes = task.getInstances().stream().mapToInt(SDNVm::getNumberOfPes).sum();
			int ram = task.getInstances().stream().mapToInt(SDNVm::getRam).sum();
			long bw = task.getInstances().stream().mapToLong(SDNVm::getBw).sum();
			long storage = task.getInstances().stream().mapToLong(SDNVm::getSize).sum();

			int size = task.getInstances().size();
			SDNVm mockCumulativeVm = new SDNVm(SDNVm.getUniqueVmId(), referenceVm.getUserId(), maxMipsReq.getAsDouble(),
					pes, ram, bw, size,"VMM", clSch, CloudSim.clock(), 0);
			mockVmToActual.put(task, mockCumulativeVm);
		});

		//Now assign newVmList to selectedHostList, if any VMs cannot be assigned, retry assigning them to the idle hosts in hostlist (which is already ordered according to baseline power)
		for (Map.Entry<Task, SDNVm> entry : mockVmToActual.entrySet()) {
			boolean success = false;
			Task key = entry.getKey();
			SDNVm value = entry.getValue();
			success = taskVmAllocationPolicy.allocateHostsForVmEnreal(key, value, key.getInstances(), selectedHostList, currAllocMap);
			if (success) {
                System.out.println();
			    /*
				for (int j = 0; j < value.size(); j++) {
					SDNVm vm = value.get(j);
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
							+ " in " + this.getName() + ", (" + vm.getStartTime() + "~" + vm.getFinishTime() + ")");

					globalVmDatacenterMap.put(vm.getId(), this);

					getVmList().add(vm);
					if (vm.isBeingInstantiated()) {
						vm.setBeingInstantiated(false);
					}

					vm.updateVmProcessing(CloudSim.clock(), getTaskVmAllocationPolicy().getHost(vm).getVmScheduler()
							.getAllocatedMipsForVm(vm));

					send(this.getId(), vm.getStartTime(), CloudSimTags.VM_CREATE_ACK, vm);

					// AMANDAAAA deploy links to initiate communication among these VMs....
					ArrayList<SDNVm> successVmList = new ArrayList<>();
					successVmList.add(vm);
					sendNow(nos.getId(), CloudSimTagsSDN.DEPLOY_TASK_COMM, successVmList);
				}
				*/
			}
			else {
			    failedTasks.add(key);
			    /*
				// deallocate vms already allocated since all vms should be in one node...
				ListIterator<SDNVm> itr = task.getScheduledInstances().listIterator();
				while (itr.hasNext()) {
					taskVmAllocationPolicy.deallocateHostForVm(itr.next());
                    itr.next().getHost().vmDestroy(itr.next());
					task.getPendingInstances().add(itr.next());
					itr.remove();
				}

				// then reallocate all vms to one host by considering more idle PMs in the total host list excluding those that we already tried
                // select idle PMs and use them....
				success = taskVmAllocationPolicy.allocateHostsForVmEnreal(task, key, value, hosts, currAllocMap);
				if (!success) {
					//THIS IS A REAL DRAWBACK OF ENREAL...!
					System.exit(0);
				}
				*/
			}
		}

		// Add failedTasks to active/idle PMs that weren't originally considered.....!
        ArrayList<SDNHost> pmsLeftToBeConsidered = new ArrayList<>();
		hosts.forEach(host -> {
		    //if (selectedHostList.indexOf(host) == -1 && ((SDNHost)host).isActive() == false) {
		    if (selectedHostList.indexOf(host) == -1) {
		        pmsLeftToBeConsidered.add((SDNHost)host);
            }
        });

		Collections.sort(pmsLeftToBeConsidered, new Comparator<SDNHost>() {
			public int compare(SDNHost o1, SDNHost o2) {
				return (int) (o1.getBaselineEnergyConsumption() - o2.getBaselineEnergyConsumption());
			}
		});

		failedTasks.forEach(task -> {
            boolean success = false;
            success = taskVmAllocationPolicy.allocateHostsForVmEnrealAll(task, mockVmToActual.get(task), task.getInstances(), pmsLeftToBeConsidered, currAllocMap);
            if (!success) {
				SDNHost newHost = new HostFactorySimple().createHost(10000, 1000000000, 10000000, 32, 2000, "h" + hostIndex++);
				ArrayList<SDNHost> newHostlist = new ArrayList<>();
				newHostlist.add(newHost);
				getTaskVmAllocationPolicy().getHostList().add(newHost);
				taskVmAllocationPolicy.allocateHostsForVmEnreal(task, mockVmToActual.get(task), task.getInstances(), newHostlist, currAllocMap);
                // THIS IS A REAL DRAWBACK OF ENREAL...!
				// Now need to create new PMS.... and assign the tasks
				// For now let's exit
				System.exit(0);
            }
        });

		// First have to migrate the VMs  -- so find the VMs and call migration event
		// Then send another event to allocate the new VMs to PMs
		Map<SDNVm, SDNHost> assignmentMap = new HashMap<>();
		Map<SDNVm, SDNHost> migrationMap = new HashMap<>();

		for (SDNHost host: currAllocMap.keySet()) {
			List<SDNVm> vmList = currAllocMap.get(host);
			for (int k = 0; k < vmList.size(); k++) {
				SDNVm vm = vmList.get(k);
				if (vm.getHost() != null) {
					// This Vm was actually allocated to a host in previous scheduling period so now check if it should be migrated
					if (vm.getHost() != host) {
						/*
						Map<String, Object> migrate = new HashMap<>();
						migrate.put("vm", vm);
						migrate.put("host", host);
						sendNow(getId(), CloudSimTags.VM_MIGRATE, migrate);
						*/
						migrationMap.put(vm, host);
						//migrateVmToHostEnReal(vm, host, assignmentMap);
					}
				} else {
					assignmentMap.put(vm, host);
				}
			}
		}

		for (SDNVm vm : migrationMap.keySet()) {
			boolean success = migrateVmToHostEnRealDirect(vm, migrationMap.get(vm));
		}

		// Will have to call VM_MIGRATE recursively....
		// arrange assignmentMap in the order of decreasing request size
		send(getId(),0, CloudSimTagsSDN.NEW_TASK_ASSIGN, assignmentMap);
	}

	protected boolean migrateVmToHostEnRealDirect(SDNVm vm, SDNHost host) {
		Map<String, Object> migrate = new HashMap<>();
		migrate.put("vm", vm);
		migrate.put("host", host);
		send(getId(),0, CloudSimTags.VM_MIGRATE, migrate);
		return true;
	}

	protected boolean migrateVmToHostEnReal(SDNVm vm, SDNHost host, Map<SDNVm, SDNHost> migrationMap) {
		migrationMap.remove(vm,host);
		if (vm.getHost() == host)
			return true;
		while(!allVmsMigrated(host, migrationMap) && !taskVmAllocationPolicy.allocateHostForVm(vm, host)) {
			for (int l = 0; l < host.getVmList().size(); l++) {
				SDNVm existingVm = (SDNVm)host.getVmList().get(l);
				if (migrationMap.get(existingVm) != null) {
					migrateVmToHostEnReal((SDNVm) existingVm, migrationMap.get(existingVm), migrationMap);
				}
			}
		}
		// reverse the action since we need to migrate the VM
		if (vm.getHost() == host)
			taskVmAllocationPolicy.deallocateHostForVm(vm);
		Map<String, Object> migrate = new HashMap<>();
		migrate.put("vm", vm);
		migrate.put("host", host);
		send(getId(),0, CloudSimTags.VM_MIGRATE, migrate);
		//migrationMap.remove(vm,host);
		return true;
	}

	protected boolean allVmsMigrated(SDNHost host, Map<SDNVm, SDNHost> assignmentMap) {
		if (assignmentMap == null || assignmentMap.size() == 0)
			return true;
		for (int p = 0; p < host.getVmList().size(); p++) {
			if (assignmentMap.containsKey(host.getVmList().get(p))) {
				return false;
			}
		}
		return true;
	}

	protected void processSDNVmCreate(SimEvent ev, boolean ack) {
		ArrayList<SDNVm> vmListForOccupancy = (ArrayList<SDNVm>) ev.getData();
		for (SDNVm vm : vmListForOccupancy) {
			//getTaskVmAllocationPolicy().allocateHostForVm(vm);
			getTaskVmAllocationPolicy().allocateHostForVmCombinedMostFullFirst(vm);
		}
		sendNow(nos.getId(), CloudSimTagsSDN.SDN_ACTIVATE_SWITCHES, vmListForOccupancy);
		sendNow(nos.getId(), CloudSimTagsSDN.SCHEDULE_TASKS_INITIAL);
	}

	protected void processTaskCreate(SimEvent ev, boolean ack) {
		ArrayList<Task> tasks = (ArrayList<Task>) ev.getData();

		//if (tasks.size() > 0)
			//enReal(tasks);


		ArrayList<Task> tasksWithPendingInstances = new ArrayList<>();

		for (Task task: tasks) {
			if (task.getPendingInstances().size() != 0) {
				// Add to failed
				tasksWithPendingInstances.add(task);
			}
			//ArrayList<SDNVm> successVmList = getTaskVmAllocationPolicy().allocateHostsForTask(task);
			ArrayList<SDNVm> successVmList = getTaskVmAllocationPolicy().allocateHostsForTask_COMP(task);

			if (successVmList == null)
				continue;

			for (SDNVm vm : successVmList) {
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
							+ " in " + this.getName() + ", (" + vm.getStartTime() + "~" + vm.getFinishTime() + ")");

					globalVmDatacenterMap.put(vm.getId(), this);

					getVmList().add(vm);
					if (vm.isBeingInstantiated()) {
						vm.setBeingInstantiated(false);
					}

					vm.updateVmProcessing(CloudSim.clock(), getTaskVmAllocationPolicy().getHost(vm).getVmScheduler()
							.getAllocatedMipsForVm(vm));

					send(this.getId(), 0, CloudSimTags.VM_CREATE_ACK, vm);

				// AMANDAAAA deploy links to initiate communication among these VMs....
				sendNow(nos.getId(), CloudSimTagsSDN.DEPLOY_TASK_COMM, successVmList);
					//send(nos.getId(),CloudSim.getMinTimeBetweenEvents(), CloudSimTagsSDN.DEPLOY_TASK_COMM, successVmList);
			}
		}

		if (tasksWithPendingInstances.size() != 0) {
            send(this.getId(), 0, CloudSimTagsSDN.TASK_CREATE_ACK, tasksWithPendingInstances);
        }
	}



	public void processUpdateProcessing() {
		updateCloudletProcessing(); // Force Processing - TRUE!
		checkCloudletCompletion();
	}

	public void initiateMigration() {

	}
	
	protected void processCloudletSubmit(SimEvent ev, boolean ack) {
		// gets the Cloudlet object
		Cloudlet cl = (Cloudlet) ev.getData();
		
		// Clear out the processed data for the previous time slot before Cloudlet submitted
		updateCloudletProcessing();

		try {
			// checks whether this Cloudlet has finished or not
			if (cl.isFinished()) {
				String name = CloudSim.getEntityName(cl.getUserId());
				Log.printLine(getName() + ": Warning - Cloudlet #" + cl.getCloudletId() + " owned by " + name
						+ " is already completed/finished.");
				Log.printLine("Therefore, it is not being executed again");
				Log.printLine();

				// NOTE: If a Cloudlet has finished, then it won't be processed.
				// So, if ack is required, this method sends back a result.
				// If ack is not required, this method don't send back a result.
				// Hence, this might cause CloudSim to be hanged since waiting
				// for this Cloudlet back.
				if (ack) {
					int[] data = new int[3];
					data[0] = getId();
					data[1] = cl.getCloudletId();
					data[2] = CloudSimTags.FALSE;

					// unique tag = operation tag
					int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
					sendNow(cl.getUserId(), tag, data);
				}

				sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

				return;
			}

			// process this Cloudlet to this CloudResource
			cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
					.getCostPerBw());

			int userId = cl.getUserId();
			int vmId = cl.getVmId();
			// time to transfer the files
			double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());

			// AMANDAAAA alter here
			SDNHost host = (SDNHost)getTaskVmAllocationPolicy().getHost(vmId, userId);
			Vm vm = host.getVm(vmId, userId);
			CloudletScheduler scheduler = vm.getCloudletScheduler();
			
			double estimatedFinishTime = scheduler.cloudletSubmit(cl, fileTransferTime); // This estimated time is useless

			//host.adjustMipsShare();
			//estimatedFinishTime = scheduler.getNextFinishTime(CloudSim.clock(), scheduler.getCurrentMipsShare());

			// Check the new estimated time by using host's update VM processing funciton.
			// This function is called only to check the next finish time
			estimatedFinishTime = host.updateVmsProcessing(CloudSim.clock());
			
			double estimatedFinishDelay = estimatedFinishTime - CloudSim.clock();
			//estimatedFinishTime -= CloudSim.clock();

			// if this cloudlet is in the exec queue
			if (estimatedFinishDelay > 0.0 && estimatedFinishTime < Double.MAX_VALUE) {
				estimatedFinishTime += fileTransferTime;
				//Log.printLine(getName() + ".processCloudletSubmit(): " + "Cloudlet is going to be processed at: "+(estimatedFinishTime + CloudSim.clock()));
				
				// gurantees a minimal interval before scheduling the event
				if (estimatedFinishDelay < CloudSim.getMinTimeBetweenEvents()) {
					estimatedFinishDelay = CloudSim.getMinTimeBetweenEvents();
				}				
				
				send(getId(), estimatedFinishDelay, CloudSimTags.VM_DATACENTER_EVENT);
			}

			if (ack) {
				int[] data = new int[3];
				data[0] = getId();
				data[1] = cl.getCloudletId();
				data[2] = CloudSimTags.TRUE;

				// unique tag = operation tag
				int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
				sendNow(cl.getUserId(), tag, data);
			}
		} catch (ClassCastException c) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
			c.printStackTrace();
		} catch (Exception e) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
			e.printStackTrace();
		}

		checkCloudletCompletion();
	}

	@Override
	protected void checkCloudletCompletion() {
		if(!nos.isApplicationDeployed())
		{
			super.checkCloudletCompletion();
			return;
		}

		Map <Host, Vm> hostVMMap = new HashMap<>();
		//AMANDAAA updated here
		List<? extends Host> list = getTaskVmAllocationPolicy().getHostList();
		for (int i = 0; i < list.size(); i++) {
			Host host = list.get(i);
			for (Vm vm : host.getVmList()) {
				Task task = getTaskVmAllocationPolicy().getTaskIdOfTheInstanceInVm((SDNVm)vm);
				// Check all completed Cloudlets
				while (vm.getCloudletScheduler().isFinishedCloudlets()) {
					Cloudlet cl = vm.getCloudletScheduler().getNextFinishedCloudlet();
					if (cl != null) {
						if (!Log.isDisabled()) {
							Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Cloudlet ",
									cl.getCloudletId(), " has finished execution on #VM ", vm.getId());
						}
						//sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl); // This event has no implementation in CloudSimSDN. It actually refers to event 20 of CloudSim which is handled by
						//DatacenterBroker in function processCloudletReturn....
						// AMANDAAA need to discard the VM and release host resources here
						//host.vmDestroy(vm);
						hostVMMap.put(host, vm);

					}

					// AMANDAAAA commented out here
					/*
					if (cl != null) {
						// For completed cloudlet -> process next activity.
						Request req = requestsTable.remove(cl.getCloudletId());
						req.getPrevActivity().setFinishTime(CloudSim.clock());
					
						if (req.isFinished()){
							// All requests are finished, no more activities to do. Return to user
							send(req.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTagsSDN.REQUEST_COMPLETED, req);
						} else {
							//consume the next activity from request. It should be a transmission.
							processNextActivity(req);
						}
					}
					*/
				}
				
				// Check all failed Cloudlets (time out)
				List<Cloudlet> failedCloudlet = ((CloudletSchedulerMonitor) (vm.getCloudletScheduler())).getFailedCloudlet();
				for(Cloudlet cl:failedCloudlet) {
					processCloudletFailed(cl);
				}

				// AMANDAAA including this condition to prevent dependent tasks from waiting forever....
				if (vm.getCloudletScheduler().getCloudletExecList().size() == 0 &&
						vm.getCloudletScheduler().getCloudletFailedList().size() == 0 && vm.getCloudletScheduler().getCloudletPausedList().size() == 0) {
					// Cloudlets have finished execution... although the loop keeps executing (not sure why)... so to schedule remaining tasks
					if (task != null) {
						task.addCompletedInstance((SDNVm) vm);
						if (task.getInstances().size() == task.getCompletedInstances().size())
							task.setCompleted(true);
						//hostVMMap.put(host,vm);
					}
				}

			}
		}

		for (Map.Entry<Host, Vm> entry : hostVMMap.entrySet()) {
			//entry.getKey().vmDestroy(entry.getValue());
			Task task = taskVmAllocationPolicy.getTaskIdOfTheInstanceInVm((SDNVm)entry.getValue());
			if (task != null) {
				task.addCompletedInstance((SDNVm) entry.getValue());
				SDNHost host = (SDNHost) entry.getKey();
				if (host.getVmList().size() == 0) {
					host.setActive(false);
				}
				if (task.getInstances().size() == task.getCompletedInstances().size())
					task.setCompleted(true);
			}
			getTaskVmAllocationPolicy().deallocateHostForVm(entry.getValue());
			//sendNow(nos.getId(), CloudSimTags.VM_DESTROY, entry.getValue());
		}
	}
	
	private void processRequestSubmit(Request req) {
		Activity ac = req.getNextActivity();
		
		if(ac instanceof Processing) {
			//AMANDAAA need to remove the processing activity here for our flow
			req.removeNextActivity();
			processNextActivity(req);
		}
		else {
			System.err.println("Request should start with Processing!!");
		}
	}

	private void processCloudletFailed(Cloudlet cl) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ".processCloudletFailed(): Cloudlet failed: "+cl);
		
		Request req = requestsTable.remove(cl.getCloudletId());
		Activity prev = req.getPrevActivity();
		if(prev != null)
			prev.setFailedTime(CloudSim.clock()); // Set as finished.
		Activity next = req.getNextActivity();
		if(next != null)
			next.setFailedTime(CloudSim.clock()); // Set as finished.
		
		Request lastReq = req.getTerminalRequest(); 
		send(req.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTagsSDN.REQUEST_FAILED, lastReq);
	}

	
	private void processPacketFailed(Packet pkt) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ".processPacketFailed(): Packet failed: "+pkt);		

		pkt.setPacketFailedTime(CloudSim.clock());
		Request req = pkt.getPayload();
		
		Request lastReq = req.getTerminalRequest(); 
		send(req.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTagsSDN.REQUEST_FAILED, lastReq);
	}

	private void processPacketCompleted(Packet pkt) {
		pkt.setPacketFinishTime(CloudSim.clock());
		Request req = pkt.getPayload();
		// AMANDAAA commenting this out since we don't need the workload processing at destination host... only need transmission
		//processNextActivity(req);
	}
	
	private void processNextActivity(Request req) {
//		Log.printLine(CloudSim.clock() + ": " + getName() + ": Process next activity: " +req);
		Activity ac = req.removeNextActivity();
		ac.setStartTime(CloudSim.clock());

		// FYI see how transmission is simulated ..
		if(ac instanceof Transmission) {
			processNextActivityTransmission((Transmission)ac);
		}
		else if(ac instanceof Processing) {
			processNextActivityProcessing(((Processing) ac), req);
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Activity is unknown..");
		}
	}
	
	private void processNextActivityTransmission(Transmission tr) {
		Packet pkt = tr.getPacket();
		
		//send package to router via channel (NOS)
		pkt = nos.addPacketToChannel(pkt);
		pkt.setPacketStartTime(CloudSim.clock());
		tr.setRequestedBW(nos.getRequestedBandwidth(pkt));
	}
	
	private void processNextActivityProcessing(Processing proc, Request reqAfterCloudlet) {
		// FYI see how cloudlet is used to simulate processing ... See class Processing.java
		Cloudlet cl = proc.getCloudlet();
		proc.clearCloudlet();
		
		requestsTable.put(cl.getCloudletId(), reqAfterCloudlet);
		// AMANDAAA if workloads.csv is empty, then the cloudlet thing won't work as well.
		sendNow(getId(), CloudSimTags.CLOUDLET_SUBMIT, cl);

		// Set the requested MIPS for this cloudlet.
		int userId = cl.getUserId();
		int vmId = cl.getVmId();

		// AMANDAAAAAA alter this method!!!
		Host host = (SDNHost)nos.findHost(vmId);
		if(host == null) {
			Vm orgVm = nos.getSFForwarderOriginalVm(vmId);
			if(orgVm != null) {
				vmId = orgVm.getId();
				cl.setVmId(vmId);
				host = getTaskVmAllocationPolicy().getHost(vmId, userId);
			}
			else {
				throw new NullPointerException("Error! cannot find a host for Workload:"+ proc+". VM="+vmId);
			}
		}
		Vm vm = host.getVm(vmId, userId);
		double mips = vm.getMips();
		proc.setVmMipsPerPE(mips);
	}
	
	public void printDebug() {
		System.err.println(CloudSim.clock()+": # of currently processing Cloudlets: "+this.requestsTable.size());
	}
	
	public void startMigrate() {
		if (isMigrateEnabled) {
			Log.printLine(CloudSim.clock()+": Migration started..");

			List<Map<String, Object>> migrationMap = getVmAllocationPolicy().optimizeAllocation(
					getVmList());

			if (migrationMap != null && migrationMap.size() > 0) {
				migrationAttempted += migrationMap.size();
				
				// Process cloudlets before migration because cloudlets are processed during migration process..
				updateCloudletProcessing();
				checkCloudletCompletion();

				for (Map<String, Object> migrate : migrationMap) {
					Vm vm = (Vm) migrate.get("vm");
					Host targetHost = (Host) migrate.get("host");
//					Host oldHost = vm.getHost();
					
					Log.formatLine(
							"%.2f: Migration of %s to %s is started",
							CloudSim.clock(),
							vm,
							targetHost);
					
					targetHost.addMigratingInVm(vm);


					/** VM migration delay = RAM / bandwidth **/
					// we use BW / 2 to model BW available for migration purposes, the other
					// half of BW is for VM communication
					// around 16 seconds for 1024 MB using 1 Gbit/s network
					send(
							getId(),
							vm.getRam() / ((double) targetHost.getBw() / (2 * 8000)),
							CloudSimTags.VM_MIGRATE,
							migrate);
				}
			}
			else {
				//Log.printLine(CloudSim.clock()+": No VM to migrate");
			}
		}		
	}

	public void startMigration() {
			Log.printLine(CloudSim.clock()+": Migration started..");

			List<Map<String, Object>> migrationMap = getVmAllocationPolicy().optimizeAllocation(
					getVmList());

			if (migrationMap != null && migrationMap.size() > 0) {
				migrationAttempted += migrationMap.size();

				// Process cloudlets before migration because cloudlets are processed during migration process..
				updateCloudletProcessing();
				checkCloudletCompletion();

				for (Map<String, Object> migrate : migrationMap) {
					Vm vm = (Vm) migrate.get("vm");
					Host targetHost = (Host) migrate.get("host");
//					Host oldHost = vm.getHost();

					Log.formatLine(
							"%.2f: Migration of %s to %s is started",
							CloudSim.clock(),
							vm,
							targetHost);

					targetHost.addMigratingInVm(vm);


					/** VM migration delay = RAM / bandwidth **/
					// we use BW / 2 to model BW available for migration purposes, the other
					// half of BW is for VM communication
					// around 16 seconds for 1024 MB using 1 Gbit/s network
					send(
							getId(),
							vm.getRam() / ((double) targetHost.getBw() / (2 * 8000)),
							CloudSimTags.VM_MIGRATE,
							migrate);
				}
			}
		}
	
	public NetworkOperatingSystem getNOS() {
		return this.nos;
	}

	public String toString() {
		return "SDNDataCenter:(NOS="+nos.toString()+")";
	}
}
