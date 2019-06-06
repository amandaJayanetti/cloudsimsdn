/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2015, The University of Melbourne, Australia
 */
package org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager;

import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.Host;
//import org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.sdn.monitor.power.PowerUtilizationMaxHostInterface;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workflowscheduler.aco.antcolonyoptimizer.NewAntColonyOptimization;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * VM Allocation Policy - BW and Compute combined, MFF.
 * When select a host to create a new VM, this policy chooses
 * the most full host in terms of both compute power and network bandwidth.
 *
 * @author Jungmin Son
 * @since CloudSimSDN 1.0
 */
public class VmAllocationPolicyToTasks extends VmAllocationPolicy implements PowerUtilizationMaxHostInterface {

    protected final double hostTotalMips;
    protected final double hostTotalBw;
    protected final int hostTotalPes;
    private Map<Long, Map<Task, List<SDNVm>>> jobTaskVMMap = new HashMap<>();

    private static Map<Task, List<SDNVm>> taskVmMap = new HashMap<>();

    private double utilizationThreshold = 0.9;

    /**
     * The vm table.
     */
    private Map<String, Host> vmTable;

    /**
     * The used pes.
     */
    private Map<String, Integer> usedPes;

    /**
     * The free pes.
     */
    private List<Integer> freePes;

    private Map<String, Long> usedMips;
    private List<Long> freeMips;
    private Map<String, Long> usedBw;
    private List<Long> freeBw;

    /**
     * Creates the new VmAllocationPolicySimple object.
     *
     * @param list the list
     * @pre $none
     * @post $none
     */
    public VmAllocationPolicyToTasks(List<? extends Host> list) {
        super(list);

        setFreePes(new ArrayList<Integer>());
        setFreeMips(new ArrayList<Long>());
        setFreeBw(new ArrayList<Long>());

        for (Host host : getHostList()) {
            getFreePes().add(host.getNumberOfPes());
            getFreeMips().add(new Long(host.getTotalMips()));
            getFreeBw().add(host.getBw());
        }
        if (list == null || list.size() == 0) {
            hostTotalMips = 0;
            hostTotalBw = 0;
            hostTotalPes = 0;
        } else {
            hostTotalMips = getHostList().get(0).getTotalMips();
            hostTotalBw = getHostList().get(0).getBw();
            hostTotalPes = getHostList().get(0).getNumberOfPes();
        }

        setVmTable(new HashMap<String, Host>());
        setUsedPes(new HashMap<String, Integer>());
        setUsedMips(new HashMap<String, Long>());
        setUsedBw(new HashMap<String, Long>());
    }

    protected double convertWeightedMetric(double mipsPercent, double bwPercent) {
        double ret = mipsPercent * bwPercent;
        return ret;
    }

    /**
     * Allocates a host for a given VM.
     *
     * @param vm VM specification
     * @return $true if the host could be allocated; $false otherwise
     * @pre $none
     * @post $none
     */
    @Override
    public boolean allocateHostForVm(Vm vm) {
        int requiredPes = vm.getNumberOfPes();
        boolean result = false;
        int tries = 0;
        List<Integer> freePesTmp = new ArrayList<Integer>();
        for (Integer freePes : getFreePes()) {
            freePesTmp.add(freePes);
        }

        if (!getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            do {// we still trying until we find a host or until we try all of them
                int moreFree = Integer.MIN_VALUE;
                int idx = -1;

                // we want the host with less pes in use
                for (int i = 0; i < freePesTmp.size(); i++) {
                    if (freePesTmp.get(i) > moreFree) {
                        moreFree = freePesTmp.get(i);
                        idx = i;
                    }
                }

                Host host = getHostList().get(idx);
                result = host.vmCreate(vm);

                if (result) { // if vm were succesfully created in the host
                    System.out.println("************************************************************************** Allocated VM " + vm.getId() + " to host: " + host.getId());
                    getVmTable().put(vm.getUid(), host);
                    getUsedPes().put(vm.getUid(), requiredPes);
                    getFreePes().set(idx, getFreePes().get(idx) - requiredPes);
                    result = true;
                    break;
                } else {
                    freePesTmp.set(idx, Integer.MIN_VALUE);
                }
                tries++;
            } while (!result && tries < getFreePes().size());

        }

        return result;
    }

    public boolean allocateHostForVmMipsLeastFullFirst(Vm vm) {
        if (getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            return false;
        }

        int numHosts = getHostList().size();

        // 1. Find/Order the best host for this VM by comparing a metric
        int requiredPes = vm.getNumberOfPes();
        double requiredMips = vm.getCurrentRequestedTotalMips();
        long requiredBw = vm.getCurrentRequestedBw();

        boolean result = false;

        double[] freeResources = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            double mipsFreePercent = (double) getFreeMips().get(i) / this.hostTotalMips;

            freeResources[i] = mipsFreePercent;
        }

        ArrayList<Integer> incompatibleHosts = new ArrayList<>();
        for (int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double lessFree = Double.POSITIVE_INFINITY;
            int idx = -1;

            // we want the host with less pes in use
            for (int i = 0; i < numHosts; i++) {
                if (freeResources[i] < lessFree && incompatibleHosts.indexOf(i) == -1) {
                    lessFree = freeResources[i];
                    idx = i;
                }
            }
            freeResources[idx] = Double.POSITIVE_INFINITY;
            Host host = getHostList().get(idx);

            // Check whether the host can hold this VM or not.
            if (getFreeMips().get(idx) < requiredMips ||
                    getFreeBw().get(idx) < requiredBw ||
                    getFreePes().get(idx) < requiredPes) {
                //Cannot host the VM
                incompatibleHosts.add(idx);
                continue;
            }

            System.out.printf("_________________________________________________________ host " + host.getId() + "______________________ vm " + vm.getId());
            result = host.vmCreate(vm);

            if (result) { // if vm were succesfully created in the host
                System.out.println("************************************************************************** Allocated VM " + vm.getId() + " to host: " + host.getId());
                getVmTable().put(vm.getUid(), host);
                getUsedPes().put(vm.getUid(), requiredPes);
                getFreePes().set(idx, getFreePes().get(idx) - requiredPes);

                getUsedMips().put(vm.getUid(), (long) requiredMips);
                getFreeMips().set(idx, (long) (getFreeMips().get(idx) - requiredMips));

                getUsedBw().put(vm.getUid(), (long) requiredBw);
                getFreeBw().set(idx, (long) (getFreeBw().get(idx) - requiredBw));

                break;
            }
        }

        if (result == false) {
            // AMANDAAAA there's an issue with this logic, so VM2 doesn't get a host. So correct this logic...
            System.out.println("*************************************** VM: " + vm.getId());
        }

        logMaxNumHostsUsed();
        return result;
    }

    protected int maxNumHostsUsed = 0;

    public void logMaxNumHostsUsed() {
        // Get how many are used
        int numHostsUsed = 0;
        for (int freePes : getFreePes()) {
            if (freePes < hostTotalPes) {
                numHostsUsed++;
            }
        }
        if (maxNumHostsUsed < numHostsUsed)
            maxNumHostsUsed = numHostsUsed;
        System.out.println("Number of online hosts:" + numHostsUsed + ", max was =" + maxNumHostsUsed);
    }

    public int getMaxNumHostsUsed() {
        return maxNumHostsUsed;
    }

    /**
     * Releases the host used by a VM.
     *
     * @param vm the vm
     * @pre $none
     * @post none
     */
    @Override
    public void deallocateHostForVm(Vm vm) {
        Host host = getVmTable().remove(vm.getUid());
        if (host != null) {
            int idx = getHostList().indexOf(host);
            host.vmDestroy(vm);

            Integer pes = getUsedPes().remove(vm.getUid());
            getFreePes().set(idx, getFreePes().get(idx) + pes);

            Long mips = getUsedMips().remove(vm.getUid());
            getFreeMips().set(idx, getFreeMips().get(idx) + mips);

            Long bw = getUsedBw().remove(vm.getUid());
            getFreeBw().set(idx, getFreeBw().get(idx) + bw);
        }
    }

    /**
     * Gets the host that is executing the given VM belonging to the given user.
     *
     * @param vm the vm
     * @return the Host with the given vmID and userID; $null if not found
     * @pre $none
     * @post $none
     */
    @Override
    public Host getHost(Vm vm) {
        return getVmTable().get(vm.getUid());
    }

    /**
     * Gets the host that is executing the given VM belonging to the given user.
     *
     * @param vmId   the vm id
     * @param userId the user id
     * @return the Host with the given vmID and userID; $null if not found
     * @pre $none
     * @post $none
     */
    @Override
    public Host getHost(int vmId, int userId) {
        return getVmTable().get(Vm.getUid(userId, vmId));
    }

    /**
     * Gets the vm table.
     *
     * @return the vm table
     */
    public Map<String, Host> getVmTable() {
        return vmTable;
    }

    /**
     * Sets the vm table.
     *
     * @param vmTable the vm table
     */
    protected void setVmTable(Map<String, Host> vmTable) {
        this.vmTable = vmTable;
    }

    /**
     * Gets the used pes.
     *
     * @return the used pes
     */
    protected Map<String, Integer> getUsedPes() {
        return usedPes;
    }

    /**
     * Sets the used pes.
     *
     * @param usedPes the used pes
     */
    protected void setUsedPes(Map<String, Integer> usedPes) {
        this.usedPes = usedPes;
    }

    /**
     * Gets the free pes.
     *
     * @return the free pes
     */
    protected List<Integer> getFreePes() {
        return freePes;
    }

    /**
     * Sets the free pes.
     *
     * @param freePes the new free pes
     */
    protected void setFreePes(List<Integer> freePes) {
        this.freePes = freePes;
    }

    protected Map<String, Long> getUsedMips() {
        return usedMips;
    }

    protected void setUsedMips(Map<String, Long> usedMips) {
        this.usedMips = usedMips;
    }

    protected Map<String, Long> getUsedBw() {
        return usedBw;
    }

    protected void setUsedBw(Map<String, Long> usedBw) {
        this.usedBw = usedBw;
    }

    protected List<Long> getFreeMips() {
        return this.freeMips;
    }

    protected void setFreeMips(List<Long> freeMips) {
        this.freeMips = freeMips;
    }

    protected List<Long> getFreeBw() {
        return this.freeBw;
    }

    protected void setFreeBw(List<Long> freeBw) {
        this.freeBw = freeBw;
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.VmAllocationPolicy#optimizeAllocation(double, cloudsim.VmList, double)
     */
    @Override
    public List<Map<String, Object>> optimizeAllocation(List<? extends Vm> vmList) {
        // AMANDAAAAA

        List<SDNHost> hosts = getHostList();
        List<SDNHost> overUtilizedHosts = new ArrayList<>();

        List<SDNVm> vmMigrationList = new ArrayList<>();

        // Identifying over utilized hosts based on a static CPU utilisation threshold. There are other ways of doing it....
        hosts.forEach(host -> {
            if (isOverUtilisedHost(host)) {
                overUtilizedHosts.add(host);
                vmMigrationList.addAll(migrationVmList(host));
            }
        });


        // Compute hosts that are not over-utilized
        List<SDNHost> eligibleHosts = new ArrayList<>();
        hosts.forEach(host -> {
            if (overUtilizedHosts.indexOf(host) == -1)
                eligibleHosts.add(host);
        });

        // New VM to host assignments (Use ACO to find these mappings)
        List<Map<String, Object>> migrationMap = new ArrayList<>();

        vmMigrationList.forEach(vm -> {
            // Recall that each job has multiple tasks and each task has multiple instances. For each instance in a task, a VM was assigned.
            // So getTaskIdOfTheInstanceInVm returns the id of the task, one of whose instances are held by the vm.
            Task vmTask = getTaskIdOfTheInstanceInVm(vm);
            List<SDNVm> connectedVms = vmTask.getInstances();
            List<SDNHost> connectedHosts = new ArrayList<>();
            connectedVms.forEach(conVm -> {
                if (connectedHosts.indexOf((SDNHost) conVm.getHost()) == -1)
                    connectedHosts.add((SDNHost) conVm.getHost());
            });

            // First try to find an eligible host from the connectedHosts ... if unsuccessful initiate ACO from a partial state
            boolean assignedVmToConnectedHost = false;
            for (int i = 0; i < connectedHosts.size(); i++) {
                // AMANDAAAA Don't do this here........ Because it happens later on in processVmMigrate method at /home/student.unimelb.edu.au/amjayanetti/.m2/repository/org/cloudbus/cloudsim/cloudsim/4.0/cloudsim-4.0-sources.jar!/org/cloudbus/cloudsim/Datacenter.java:511
                if (canAllocateVmToHost(vm, connectedHosts.get(i))) {
                    assignedVmToConnectedHost = true;
                    break;
                }
            }


            if (!assignedVmToConnectedHost) {
                // This means it wasn't possible to allocate this VM to any of the hosts which contains instances from the same task.
                // So, initiate ACO with only this VM in vmlist and all hosts excluding over utilized ones in hostlist and the set of connected hosts as the current clique
                List<SDNVm> vmToBeMigrated = new ArrayList<>();
                vmToBeMigrated.add(vm);
                NewAntColonyOptimization antColony = new NewAntColonyOptimization(hosts, eligibleHosts, vmToBeMigrated, connectedHosts);
                Multimap<SDNHost, SDNVm> allocMap = antColony.startAntOptimizationFromPartialState();

                Map<String, Object> vmToHostMap = new HashMap<>();
                Iterator keyIterator = allocMap.keySet().iterator();
                while (keyIterator.hasNext()) {
                    Host host = (Host) keyIterator.next();
                    vmToHostMap.put("host", host);
                }
                vmToHostMap.put("vm", vm);
                migrationMap.add(vmToHostMap);
            }
        });


        return migrationMap;
    }

    protected boolean canAllocateVmToHost(SDNVm vm, SDNHost host) {
        if (host.getStorage() >= vm.getSize() && host.getAvailableBandwidth() >= vm.getCurrentRequestedBw()
                && host.getVmScheduler().getAvailableMips() >= vm.getTotalMips() && host.getRamProvisioner().getAvailableRam() >= vm.getCurrentRequestedRam())
            return true;
        else
            return false;
    }

    protected double getUtilizationThreshold() {
        return utilizationThreshold;
    }

    protected boolean isOverUtilisedHost(SDNHost host) {
        double totalRequestedMips = 0;
        for (Vm vm : host.getVmList()) {
            totalRequestedMips += vm.getCurrentRequestedTotalMips();
        }
        double utilization = totalRequestedMips / host.getTotalMips();
        return utilization > getUtilizationThreshold();
    }

    protected boolean isStillOverUtilisedAfterMigration(SDNHost host, List<SDNVm> migrationVmList) {
        double totalRequestedMips = 0;
        double totalReleasedMips = 0;
        for (Vm vm : host.getVmList()) {
            totalRequestedMips += vm.getCurrentRequestedTotalMips();
        }
        for (Vm vm : migrationVmList) {
            totalReleasedMips += vm.getCurrentRequestedTotalMips();
        }
        double utilization = (totalRequestedMips - totalReleasedMips) / host.getTotalMips();
        return utilization > getUtilizationThreshold();
    }

    // Find the list of VMs to be migrated away from this host to bring it down to a normal utilization level
    protected List<SDNVm> migrationVmList(SDNHost host) {
        List<SDNVm> hostVms = host.getVmList();
        List<SDNVm> migrationVmList = new ArrayList<>();

        // Sort VMs in decending order of the required MIPS
        Collections.sort(hostVms, new Comparator<SDNVm>() {
            public int compare(SDNVm o1, SDNVm o2) {
                return (int) (o2.getMips() * o2.getNumberOfPes() - o1.getMips() * o1.getNumberOfPes());
            }
        });

        int i = 0;
        migrationVmList.add(hostVms.get(i++));

        while (isStillOverUtilisedAfterMigration(host, migrationVmList)) {
            migrationVmList.add(hostVms.get(i++));
        }

        return migrationVmList;
    }


    /*
     * (non-Javadoc)
     * @see org.cloudbus.cloudsim.VmAllocationPolicy#allocateHostForVm(org.cloudbus.cloudsim.Vm,
     * org.cloudbus.cloudsim.Host)
     */
    @Override
    public boolean allocateHostForVm(Vm vm, Host host) {
        if (host.vmCreate(vm)) { // if vm has been successfully created in the host
            getVmTable().put(vm.getUid(), host);

            int pe = vm.getNumberOfPes();
            double requiredMips = vm.getCurrentRequestedTotalMips();
            long requiredBw = vm.getCurrentRequestedBw();

            int idx = getHostList().indexOf(host);

            getUsedPes().put(vm.getUid(), pe);
            getFreePes().set(idx, getFreePes().get(idx) - pe);

            getUsedMips().put(vm.getUid(), (long) requiredMips);
            getFreeMips().set(idx, (long) (getFreeMips().get(idx) - requiredMips));

            getUsedBw().put(vm.getUid(), (long) requiredBw);
            getFreeBw().set(idx, (long) (getFreeBw().get(idx) - requiredBw));

            Log.formatLine(
                    "%.2f: VM #" + vm.getId() + " has been allocated to the host #" + host.getId(),
                    CloudSim.clock());
            return true;
        }

        return false;
    }

    /**
     * Allocates a host for a given VM.
     *
     * @param task the task to allocate hosts to
     * @return $true if the host could be allocated; $false otherwise
     * @pre $none
     * @post $none
     */
    public boolean allocateHostsForTask(Task task) {
        ArrayList<SDNVm> taskVms = (ArrayList<SDNVm>) task.getInstances();

        taskVmMap.put(task, taskVms);
        jobTaskVMMap.put(task.getJobId(), taskVmMap);


        List<SDNHost> hostList = getHostList();
        NewAntColonyOptimization antColony = new NewAntColonyOptimization(hostList, taskVms);
        Multimap<SDNHost, SDNVm> allocMap = antColony.startAntOptimization();

        Set keySet = allocMap.keySet();
        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext()) {
            SDNHost host = (SDNHost) keyIterator.next();
            ArrayList<SDNVm> vms = new ArrayList<SDNVm>(allocMap.get(host));
            Iterator itr = vms.iterator();
            while (itr.hasNext()) {
                allocateHostForVm((Vm) itr.next(), host);
            }
        }

        // IMPORTANT: SAVE THE ASSIGNMENT INFO (Job -> Task -> SDNVm) is some datastructure.

/*
        // Allocation of vms to first fit host --- AMANDAAA Use these methods for comparisons and can implement EnREAL as well here itself because all it does is order the PMs in increasing order of Power something and find the first fit one...
        for (SDNVm vm : taskVms) {
            allocateHostForVm(vm);
            //allocateHostForVmMipsLeastFullFirst(vm);
        }
*/

        return true;
    }


    public Task getTaskIdOfTheInstanceInVm(SDNVm vm) {
        Iterator<Map.Entry<Task, List<SDNVm>>> iter = taskVmMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Task, List<SDNVm>> entry = iter.next();
            List<SDNVm> vmList = entry.getValue();
            for (SDNVm vmInTask : vmList) {
                if (vmInTask.equals(vm))
                    return entry.getKey();
            }
        }
        return null;
    }
}

