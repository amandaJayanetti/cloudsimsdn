package org.cloudbus.cloudsim.sdn.workflowscheduler.aco.neighbourhood;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class GiantAnt {
    protected SDNHost nestLocus = null;
    protected int trailSize;
    protected ArrayList<Integer> trail = new ArrayList<>();
    protected List<SDNVm> unallocatedVmList;
    protected List<SDNVm> allocatedVmList = new ArrayList<>();
    protected List<SDNHost> hostList;

    public ArrayList<SDNHost> getEligibleHostList() {
        return eligibleHostList;
    }

    ArrayList<SDNHost> eligibleHostList;

    public List<SDNHost> getSelectedHostList() {
        return this.selectedHostList;
    }

    public void setSelectedHostList(List<SDNHost> selectedHostList) {
        this.selectedHostList = selectedHostList;
    }

    protected List<SDNHost> selectedHostList = new ArrayList<>();

    public Multimap<SDNHost, SDNVm> getAllocMap() {
        return allocMap;
    }

    protected Multimap<SDNHost, SDNVm> allocMap;

    public GiantAnt(int tourSize, List<SDNVm> unallocatedVmList, List<SDNHost> hostList) {
        this.trailSize = tourSize;
        this.unallocatedVmList = unallocatedVmList;
        this.hostList = hostList;
        this.allocMap = HashMultimap.create();
        this.eligibleHostList = new ArrayList<>();
        hostList.forEach(host -> {
            eligibleHostList.add(host);
        });
    }

    public GiantAnt(int tourSize, List<SDNVm> unallocatedVmList, List<SDNHost> hostList, List<SDNHost> eligibleHosts, List<SDNHost> currClique) {
        this.trailSize = tourSize;
        this.unallocatedVmList = unallocatedVmList;
        this.hostList = hostList;
        this.allocMap = HashMultimap.create();
        this.eligibleHostList = new ArrayList<>();
        eligibleHosts.forEach(host -> {
            eligibleHostList.add(host);
        });
        this.selectedHostList = new ArrayList<>();
        currClique.forEach(host -> {
            selectedHostList.add(host);
            trail.add(trail.size(),hostList.indexOf(host));
        });
    }

    protected void visitHost(SDNHost host) {
        boolean mappedVmsToHost = false;
        // Check if any of the VMs in unallocated list can be allocated to this host. If so remove them from the unallocated vm list.
        SDNHost currHost = host;
        ListIterator<SDNVm> itr = unallocatedVmList.listIterator();
        while (itr.hasNext()) {
            SDNVm sdnVm = itr.next();
            boolean result = canAllocateVmToHost(sdnVm, currHost);
            if (result) {
                // Add vm to host mapping
                allocMap.put(currHost, sdnVm);
                // remove VM from unallocated list.
                allocatedVmList.add(sdnVm);
                itr.remove();
                // Since one or more of the Vms were allocated to this host, mark it as one of the hosts in this NewAnt's solution set
                mappedVmsToHost = true;
            }
        }

        if (mappedVmsToHost) {
            trail.add(trail.size(), hostList.indexOf(host));
            selectedHostList.add(currHost);
        } else {
            // To prevent these hosts from being reconsidered
            eligibleHostList.remove(host);
        }
    }

    protected boolean canAllocateVmToHost(SDNVm vm, SDNHost host) {
        ArrayList<SDNVm> allocatedVms = new ArrayList<SDNVm>(allocMap.get(host));

        long totalAllocStorage = allocatedVms.stream().mapToLong(Vm::getSize).sum();
        long totalAllocBw = allocatedVms.stream().mapToLong(Vm::getCurrentRequestedBw).sum();
        long totalAllocMips = allocatedVms.stream().mapToLong(SDNVm::getTotalMips).sum();
        int totalAllocRam = allocatedVms.stream().mapToInt(Vm::getCurrentRequestedRam).sum();
        int totalAllocPes = allocatedVms.stream().mapToInt(Vm::getNumberOfPes).sum()  + host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum();

        if (host.getStorage() - totalAllocStorage >= vm.getSize() && host.getAvailableBandwidth() - totalAllocBw >= vm.getCurrentRequestedBw()
                && host.getVmScheduler().getAvailableMips() - totalAllocMips >= vm.getTotalMips() && host.getPeList().size() - totalAllocPes > vm.getNumberOfPes() && host.getPeList().get(0).getMips() > vm.getMips() /* // each virtual PE of a VM must require not more than the capacity of a physical PE */ && host.getRamProvisioner().getAvailableRam() - totalAllocRam  >= vm.getCurrentRequestedRam())
            return true;
        else
            return false;
    }
}
