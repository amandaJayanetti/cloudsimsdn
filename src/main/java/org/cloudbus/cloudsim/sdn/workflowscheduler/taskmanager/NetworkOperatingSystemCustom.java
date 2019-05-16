/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2015, The University of Melbourne, Australia
 */
package org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.sdn.CloudSimTagsSDN;
import org.cloudbus.cloudsim.sdn.nos.NetworkOperatingSystem;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunctionChainPolicy;
import org.cloudbus.cloudsim.sdn.virtualcomponents.FlowConfig;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

import java.util.*;

/**
 * Simple network operating system class for the example.
 * In this example, network operating system (aka SDN controller) finds shortest path
 * when deploying the application onto the cloud.
 *
 * @author Jungmin Son
 * @since CloudSimSDN 1.0
 */
public class NetworkOperatingSystemCustom extends NetworkOperatingSystem {

    public NetworkOperatingSystemCustom(String name) {
        super(name);
    }

    public NetworkOperatingSystemCustom() {
        super("NOS");
    }

    @Override
    protected boolean deployApplication(List<Vm> vms, Collection<FlowConfig> links, List<ServiceFunctionChainPolicy> sfcPolicy) {
        Log.printLine(CloudSim.clock() + ": " + getName() + ": Starting to deploying scientific workflows..");

        // Sort tasks in ascending order of the start time
        ArrayList<Task> tasks = this.getTaskList();
        tasks.sort((task1, task2) -> {
            return (int) (task1.getStartTime() - task2.getStartTime());
        });
        deployTasks(tasks);


        // Sort VMs in ascending order of the start time
        Collections.sort(vms, new Comparator<Vm>() {
            public int compare(Vm o1, Vm o2) {
                return (int) (o2.getMips() * o2.getNumberOfPes() - o1.getMips() * o1.getNumberOfPes());
            }
        });

/*
        for (Vm vm : vms) {
            SDNVm tvm = (SDNVm) vm;
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + tvm.getId()
                    + " in " + datacenter.getName() + ", (" + tvm.getStartTime() + "~" + tvm.getFinishTime() + ")");
            send(datacenter.getId(), tvm.getStartTime(), CloudSimTags.VM_CREATE_ACK, tvm);

            if (tvm.getFinishTime() != Double.POSITIVE_INFINITY) {
                //System.err.println("VM will be terminated at: "+tvm.getFinishTime());
                send(datacenter.getId(), tvm.getFinishTime(), CloudSimTags.VM_DESTROY, tvm);
                send(this.getId(), tvm.getFinishTime(), CloudSimTags.VM_DESTROY, tvm);
            }
        }
        */
        return true;
    }

    protected boolean deployTasks(List<Task> tasks) {
        // Select tasks that have start times falling within the current time partition (For now we'll just select 2 tasks to schedule in each iteration)
        int MAX_TASKS = 2;
        int taskCount = 0;

        Iterator<Task> iter = tasks.iterator();

        while (iter.hasNext()) {
            Task task = iter.next();
            if (taskCount < MAX_TASKS) {
                send(datacenter.getId(), 0.0, CloudSimTagsSDN.TASK_CREATE_ACK, task);
                iter.remove();
            } else
                break;
            taskCount++;
        }

        if (tasks.isEmpty() != true)
            send(this.getId(), 0.0, CloudSimTagsSDN.SCHEDULE_TASKS, tasks);
        return true;
    }

    @Override
    public void processVmCreateAck(SimEvent ev) {
        super.processVmCreateAck(ev);

        // print the created VM info
        SDNVm vm = (SDNVm) ev.getData();
        Log.printLine(CloudSim.clock() + ": " + getName() + ": VM Created: " + vm + " in " + vm.getHost());
        deployFlow(this.flowMapVmId2Flow.values());
    }

    private boolean deployFlow(Collection<FlowConfig> arcs) {
        // FYI I think there's an error here... Because for each VM_CREATE_ACK event NetworkOperatingSystem receives, it deploys the same 1276 arcs again and again... Check if they are vm specific
        for (FlowConfig arc : arcs) {
            vnMapper.buildForwardingTable(arc.getSrcId(), arc.getDstId(), arc.getFlowId());
        }
		
		/*/ Print all routing tables.
		for(Node node:this.topology.getAllNodes()) {
			node.printVMRoute();
		}
		//*/
        return true;
    }

}
