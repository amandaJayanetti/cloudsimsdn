package org.cloudbus.cloudsim.sdn.workflowscheduler.aco;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

import java.util.*;

public class Ant {
    protected int trailSize;
    //protected int trail[];
    protected ArrayList<Integer> trail = new ArrayList<>();
    protected boolean visited[];
    protected List<SDNVm> unallocatedVmList;
    protected List<SDNVm> allocatedVmList = new ArrayList<>();
    protected List<SDNHost> hostList;
    protected VmAllocationPolicy policy;

    public Multimap<SDNHost, SDNVm> getAllocMap() {
        return allocMap;
    }

    protected Multimap<SDNHost, SDNVm> allocMap;

    public Ant(int tourSize, List<SDNVm> unallocatedVmList, List<SDNHost> hostList, VmAllocationPolicy policy) {
        this.trailSize = tourSize;
        //this.trail = new int[tourSize];
        this.visited = new boolean[tourSize];
        this.unallocatedVmList = unallocatedVmList;
        this.hostList = hostList;
        this.policy = policy;
        this.allocMap = HashMultimap.create();
    }

    protected void visitCity(int city) {
        boolean mappedVmsToHost = false;
        // Check if any of the VMs in unallocated list can be allocated to this host. If so remove them from the unallocated vm list.
        SDNHost currHost = hostList.get(city);
        ListIterator<SDNVm> itr = unallocatedVmList.listIterator();
        while (itr.hasNext()) {
            SDNVm sdnVm = itr.next();
            boolean result = canAllocateVmToHost(sdnVm, currHost);
            if (result) {
                // Add vm to host mapping
                allocMap.put(currHost, sdnVm);
                // remove VM from unallocated list.
                // NOTEEEEEE: There seems to be a bug in this remove() because when it's called, the unallocated vms in ALL ants seem to get removed???
                //itr.remove();
                allocatedVmList.add(sdnVm);
                // Since one or more of the Vms were allocated to this host, mark it as one of the hosts in this Ant's solution set
                mappedVmsToHost = true;
            }
        }

        if (mappedVmsToHost) {
            trail.add(trail.size(), city);
            visited[city] = true;
        } else {
            // Check if visited is of any use for our approach
            visited[city] = false;
        }
    }

    protected boolean canAllocateVmToHost(SDNVm vm, SDNHost host) {
        ArrayList<SDNVm> allocatedVms = new ArrayList<SDNVm>(allocMap.get(host));

        long totalAllocStorage = allocatedVms.stream().mapToLong(Vm::getSize).sum();
        long totalAllocBw = allocatedVms.stream().mapToLong(Vm::getCurrentRequestedBw).sum();
        long totalAllocMips = allocatedVms.stream().mapToLong(SDNVm::getTotalMips).sum();
        int totalAllocRam = allocatedVms.stream().mapToInt(Vm::getCurrentRequestedRam).sum();

        if (host.getStorage() - totalAllocStorage > vm.getSize() && host.getAvailableBandwidth() - totalAllocBw > vm.getCurrentRequestedBw()
                && host.getTotalMips() - totalAllocMips > vm.getTotalMips() && host.getRam() - totalAllocRam  > vm.getCurrentRequestedRam())
            return true;
        else
            return false;
    }

    protected boolean visited(int i) {
        return visited[i];
    }

    protected double trailLength(double graph[][]) {
        //double length = graph[trail.get(trailSize - 1)][trail.get(0)];
        double length = 0;
        for (int i = 1; i < trail.size(); i++) {
            length += graph[trail.get(i - 1)][trail.get(i)];
        }
        return length;
    }

    protected void clear() {
        for (int i = 0; i < trailSize; i++)
            visited[i] = false;
    }
}
