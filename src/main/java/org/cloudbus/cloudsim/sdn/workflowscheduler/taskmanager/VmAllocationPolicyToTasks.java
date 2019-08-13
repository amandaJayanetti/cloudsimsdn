/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2015, The University of Melbourne, Australia
 */
package org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager;

import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.*;
//import org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager.Host;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.sdn.CloudSimTagsSDN;
import org.cloudbus.cloudsim.sdn.CloudletSchedulerSpaceSharedMonitor;
import org.cloudbus.cloudsim.sdn.Configuration;
import org.cloudbus.cloudsim.sdn.monitor.power.PowerUtilizationMaxHostInterface;
import org.cloudbus.cloudsim.sdn.physicalcomponents.Link;
import org.cloudbus.cloudsim.sdn.physicalcomponents.Node;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicy;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicyBandwidthAllocation;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workflowscheduler.aco.antcolonyoptimizer.NewAntColonyOptimization;
import org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer.Task;

import java.util.*;

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
    private Map<String, Map<Task, List<SDNVm>>> jobTaskVMMap = new HashMap<>();

    public static Map<Task, List<SDNVm>> getTaskVmMap() {
        return taskVmMap;
    }

    private static Map<Task, List<SDNVm>> taskVmMap;

    private double utilizationThreshold = 0.9;
    private double underUtilizationThreshold = 0.2;

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

        taskVmMap = new HashMap<>();
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

                SDNHost hostSdn = (SDNHost)host;
                hostSdn.setActive(true);
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

    public boolean allocateHostForVmMipsMostFullFirstEnergyAware(Vm vm) {
        // allocate VM to the host which causes the least rise in energy consumption

        int numHosts = getHostList().size();

        double[] perPerWatt = new double[numHosts];
        double[] utilization = new double[numHosts];

        for (int i = 0; i < numHosts; i++) {
            SDNHost host = (SDNHost)getHostList().get(i);
            perPerWatt[i] = host.getPerformancePerWatt(host.getTotalMips(), host);
            // Utilization as a fraction of execution time...higher utilizations are preferred
            utilization[i] = ((host.getNumberOfPes() - host.getNumberOfFreePes())/host.getPeList().size())/(vm.getMips()/(host.getPeList().get(0).getMips()));
        }

        boolean result = false;

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double powerUtilization = 0;
            int idx = -1;
            // we want the host with less pes in use
            for (int i = 0; i < numHosts; i++) {
                if (perPerWatt[i] > powerUtilization) {
                    powerUtilization = perPerWatt[i];
                    idx = i;
                }
            }

            if (idx == -1)
                break;

            ArrayList<SDNHost> eligibleHosts = new ArrayList<>();
            double utilizationMax = 0.0;
            for (int i = 0; i < numHosts; i++) {
                if (perPerWatt[i] == powerUtilization) {
                    eligibleHosts.add((SDNHost)getHostList().get(i));
                    if (utilization[i] > utilizationMax) {
                        idx = i;
                        utilizationMax = utilization[i];
                    }
                }
            }

            perPerWatt[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
            utilization[idx] = 0.0;
            Random rand = new Random();
            Host host = getHostList().get(idx); //eligibleHosts.get(rand.nextInt(eligibleHosts.size()));

            perPerWatt[idx] = 0;

            // Check whether the host can hold this VM or not.
            if( getFreeMips().get(idx) < vm.getMips()) {
                //System.err.println("not enough MIPS");
                //Cannot host the VM
                continue;
            }
            if( getFreeBw().get(idx) < vm.getBw()) {
                //System.err.println("not enough BW");
                //Cannot host the VM
                continue;
            }

            if( getFreePes().get(idx) < vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if( host.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            result = allocateHostForVm(vm, host);
            if (result) {
                ((SDNHost) host).setActive(true);
                break;
            }
        }
        return result;
    }

    public boolean allocateHostForVmCombinedLeastFullFirst(Vm vm) {
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
            double mipsFreePercent = (double)getFreeMips().get(i) / hostTotalMips;
            double bwFreePercent = (double)getFreeBw().get(i) / hostTotalBw;

            freeResources[i] = convertWeightedMetric(mipsFreePercent, bwFreePercent);
        }

        if(vm instanceof SDNVm) {
            SDNVm svm = (SDNVm) vm;
            if(svm.getHostName() != null) {
                // allocate this VM to the specific Host!
                for (int i = 0; i < numHosts; i++) {
                    SDNHost h = (SDNHost)(getHostList().get(i));
                    if(svm.getHostName().equals(h.getName())) {
                        freeResources[i] = Double.MAX_VALUE;
                    }
                }
            }
        }

        for(int tries = 0; tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double moreFree = Double.NEGATIVE_INFINITY;
            int idx = -1;

            // Find the least free host, we want the host with less pes in use
            for (int i = 0; i < numHosts; i++) {
                if (freeResources[i] > moreFree) {
                    moreFree = freeResources[i];
                    idx = i;
                }
            }

            if(idx==-1) {
                System.err.println("Cannot assign the VM to any host:"+tries+"/"+numHosts);
                return false;
            }

            freeResources[idx] = Double.NEGATIVE_INFINITY;

            Host host = getHostList().get(idx);

            // Check whether the host can hold this VM or not.
            if( getFreeMips().get(idx) < requiredMips) {
                //System.err.println("not enough MIPS");
                //Cannot host the VM
                continue;
            }
            if( getFreeBw().get(idx) < requiredBw) {
                //System.err.println("not enough BW");
                //Cannot host the VM
                continue;
            }

            if( getFreePes().get(idx) < vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            result = host.vmCreate(vm);

            if (result) { // if vm were succesfully created in the host
                getVmTable().put(vm.getUid(), host);
                getUsedPes().put(vm.getUid(), requiredPes);
                getFreePes().set(idx, getFreePes().get(idx) - requiredPes);

                getUsedMips().put(vm.getUid(), (long) requiredMips);
                getFreeMips().set(idx,  (long) (getFreeMips().get(idx) - requiredMips));

                getUsedBw().put(vm.getUid(), (long) requiredBw);
                getFreeBw().set(idx,  (long) (getFreeBw().get(idx) - requiredBw));

                ((SDNHost)host).setActive(true);
                break;
            }
        }
        if(!result) {
            System.err.println("Cannot assign this VM("+vm+") to any host. NumHosts="+numHosts);
            //throw new IllegalArgumentException("Cannot assign this VM("+vm+") to any host. NumHosts="+numHosts);
        }
        logMaxNumHostsUsed();
        return result;
    }

    public boolean allocateHostForVmMipsMostFullFirst(Vm vm) {
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
            double mipsFreePercent = (double)getFreeMips().get(i) / this.hostTotalMips;

            freeResources[i] = mipsFreePercent;
        }

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double lessFree = Double.POSITIVE_INFINITY;
            int idx = -1;

            // we want the host with less pes in use
            for (int i = 0; i < numHosts; i++) {
                if (freeResources[i] < lessFree) {
                    lessFree = freeResources[i];
                    idx = i;
                }
            }
            freeResources[idx] = Double.POSITIVE_INFINITY;
            Host host = getHostList().get(idx);

            // Check whether the host can hold this VM or not.
            if( getFreeMips().get(idx) < requiredMips) {
                //System.err.println("not enough MIPS");
                //Cannot host the VM
                continue;
            }
            if( getFreeBw().get(idx) < requiredBw) {
                //System.err.println("not enough BW");
                //Cannot host the VM
                continue;
            }

            if( getFreePes().get(idx) < vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            result = host.vmCreate(vm);

            if (result) { // if vm were succesfully created in the host
                getVmTable().put(vm.getUid(), host);
                getUsedPes().put(vm.getUid(), requiredPes);
                getFreePes().set(idx, getFreePes().get(idx) - requiredPes);

                getUsedMips().put(vm.getUid(), (long) requiredMips);
                getFreeMips().set(idx,  (long) (getFreeMips().get(idx) - requiredMips));

                getUsedBw().put(vm.getUid(), (long) requiredBw);
                getFreeBw().set(idx,  (long) (getFreeBw().get(idx) - requiredBw));

                ((SDNHost)host).setActive(true);
                break;
            }
        }

        logMaxNumHostsUsed();
        return result;
    }

    //Randomly assign host to one of the most power efficient hosts
    public boolean allocateHostForVmEnergyAwareGreedy(Vm vm) {
        // allocate VM to the host which causes the least rise in energy consumption

        int numHosts = getHostList().size();

        double[] perPerWatt = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            SDNHost host = (SDNHost)getHostList().get(i);
            perPerWatt[i] = host.getBaselineEnergyConsumption(); //host.getPerformancePerWatt(host.getTotalMips(), host);
        }

        boolean result = false;

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double powerUtilization = 0;
            int idx = -1;
            // we want the host with less pes in use
            for (int i = 0; i < numHosts; i++) {
                if (perPerWatt[i] > powerUtilization) {
                    powerUtilization = perPerWatt[i];
                    idx = i;
                }
            }

            if (idx == -1)
                break;

            ArrayList<SDNHost> eligibleHosts = new ArrayList<>();
            for (int i = 0; i < numHosts; i++) {
                if (perPerWatt[i] == powerUtilization) {
                    eligibleHosts.add((SDNHost)getHostList().get(i));
                }
            }

            perPerWatt[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
            Random rand = new Random();
            Host host = getHostList().get(idx); //eligibleHosts.get(rand.nextInt(eligibleHosts.size()));

            perPerWatt[idx] = 0;

            // Check whether the host can hold this VM or not.
            if( getFreeMips().get(idx) < vm.getMips()) {
                //System.err.println("not enough MIPS");
                //Cannot host the VM
                continue;
            }
            if( getFreeBw().get(idx) < vm.getBw()) {
                //System.err.println("not enough BW");
                //Cannot host the VM
                continue;
            }

            if( getFreePes().get(idx) < vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if( host.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            result = allocateHostForVm(vm, host);
            if (result) {
                ((SDNHost) host).setActive(true);
                break;
            }
        }
        return result;
    }

    public boolean allocateHostForVmCombinedMostFullFirst(Vm vm, Task task) {

        int numHosts = getHostList().size();
        // 1. Find/Order the best host for this VM by comparing a metric
        int requiredPes = vm.getNumberOfPes();
        double requiredMips = vm.getCurrentRequestedTotalMips();
        long requiredBw = vm.getCurrentRequestedBw();

        boolean result = false;

        if (getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            return false;
        }

        double[] freeResources = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            double mipsFreePercent = (double)getFreeMips().get(i) / this.hostTotalMips;
            double bwFreePercent = (double)getFreeBw().get(i) / this.hostTotalBw;

            freeResources[i] = this.convertWeightedMetric(mipsFreePercent, bwFreePercent);
        }

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double lessFree = Double.POSITIVE_INFINITY;
            int idx = -1;

            // we want the host with less free pes
            for (int i = 0; i < numHosts; i++) {
                if (freeResources[i] < lessFree) {
                    lessFree = freeResources[i];
                    idx = i;
                }
            }
            freeResources[idx] = Double.POSITIVE_INFINITY;
            Host host = getHostList().get(idx);


            // Check whether the host can hold this VM or not.
            if( getFreeMips().get(idx) < requiredMips) {
                System.err.println("not enough MIPS:"+getFreeMips().get(idx)+", req="+requiredMips);
                //Cannot host the VM
                continue;
            }
            if( getFreeBw().get(idx) < requiredBw) {
                System.err.println("not enough BW:"+getFreeBw().get(idx)+", req="+requiredBw);
                //Cannot host the VM
                //continue;
            }

            result = host.vmCreate(vm);

            if (result) { // if vm were succesfully created in the host
                getVmTable().put(vm.getUid(), host);
                getUsedPes().put(vm.getUid(), requiredPes);
                getFreePes().set(idx, getFreePes().get(idx) - requiredPes);

                getUsedMips().put(vm.getUid(), (long) requiredMips);
                getFreeMips().set(idx,  (long) (getFreeMips().get(idx) - requiredMips));

                getUsedBw().put(vm.getUid(), (long) requiredBw);
                getFreeBw().set(idx,  (long) (getFreeBw().get(idx) - requiredBw));

                ((SDNHost)host).setActive(true);
                task.getInstanceHostMap().put((SDNVm)vm, (SDNHost)host);
                break;
            }
        }

        if(!result) {
            System.err.println("VmAllocationPolicy: WARNING:: Cannot create VM!!!!");
        }
        logMaxNumHostsUsed();
        return result;
    }

    public boolean allocateHostForVmCombinedMostFullFirstNetworkAware(Vm vm) {

        int numHosts = getHostList().size();
        // 1. Find/Order the best host for this VM by comparing a metric
        int requiredPes = vm.getNumberOfPes();
        double requiredMips = vm.getCurrentRequestedTotalMips();
        long requiredBw = vm.getCurrentRequestedBw();

        boolean result = false;

        // Compute physical distance from connected hosts to all eligible hosts
        Task vmTask = getTaskIdOfTheInstanceInVm((SDNVm)vm);
        List<SDNVm> connectedVms = vmTask.getInstances();
        List<SDNHost> connectedHosts = new ArrayList<>();
        connectedVms.forEach(conVm -> {
            if (conVm.getHost() != null) {
                if (connectedHosts.indexOf((SDNHost) conVm.getHost()) == -1)
                    connectedHosts.add((SDNHost) conVm.getHost());
            }
        });

        for (int i = 0; i < connectedHosts.size(); i++) {
            Host host = connectedHosts.get(i);
            // Check whether the host can hold this VM or not.
            if(host.getAvailableMips() < requiredMips) {
                System.err.println("not enough MIPS:"+host.getAvailableMips()+", req="+requiredMips);
                //Cannot host the VM
                continue;
            }
            if( ((SDNHost) host).getAvailableBandwidth() < requiredBw) {
                //continue;
            }
            if( host.getNumberOfFreePes() < vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }
            if( host.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            result = host.vmCreate(vm);

            if (result) { // if vm were succesfully created in the host
                int idx = -1;
                for (int j = 0; j < numHosts; j ++) {
                    if (getHostList().get(j) == host)
                        idx = j;
                }
                getVmTable().put(vm.getUid(), host);
                getUsedPes().put(vm.getUid(), requiredPes);
                getFreePes().set(idx, getFreePes().get(idx) - requiredPes);

                getUsedMips().put(vm.getUid(), (long) requiredMips);
                getFreeMips().set(idx,  (long) (getFreeMips().get(idx) - requiredMips));

                getUsedBw().put(vm.getUid(), (long) requiredBw);
                getFreeBw().set(idx,  (long) (getFreeBw().get(idx) - requiredBw));

                ((SDNHost)host).setActive(true);
                return result;
            }
        }


        if (getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            return false;
        }

        double[] freeResources = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            double mipsFreePercent = (double)getFreeMips().get(i) / this.hostTotalMips;
            double bwFreePercent = (double)getFreeBw().get(i) / this.hostTotalBw;

            freeResources[i] = this.convertWeightedMetric(mipsFreePercent, bwFreePercent);
        }

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double lessFree = Double.POSITIVE_INFINITY;
            int idx = -1;

            // we want the host with less free pes
            for (int i = 0; i < numHosts; i++) {
                if (freeResources[i] < lessFree) {
                    lessFree = freeResources[i];
                    idx = i;
                }
            }
            freeResources[idx] = Double.POSITIVE_INFINITY;
            Host host = getHostList().get(idx);


            // Check whether the host can hold this VM or not.
            if( getFreeMips().get(idx) < requiredMips) {
                System.err.println("not enough MIPS:"+getFreeMips().get(idx)+", req="+requiredMips);
                //Cannot host the VM
                continue;
            }
            if( getFreeBw().get(idx) < requiredBw) {
                System.err.println("not enough BW:"+getFreeBw().get(idx)+", req="+requiredBw);
                //Cannot host the VM
                //continue;
            }

            result = host.vmCreate(vm);

            if (result) { // if vm were succesfully created in the host
                getVmTable().put(vm.getUid(), host);
                getUsedPes().put(vm.getUid(), requiredPes);
                getFreePes().set(idx, getFreePes().get(idx) - requiredPes);

                getUsedMips().put(vm.getUid(), (long) requiredMips);
                getFreeMips().set(idx,  (long) (getFreeMips().get(idx) - requiredMips));

                getUsedBw().put(vm.getUid(), (long) requiredBw);
                getFreeBw().set(idx,  (long) (getFreeBw().get(idx) - requiredBw));

                ((SDNHost)host).setActive(true);
                break;
            }
        }

        if(!result) {
            System.err.println("VmAllocationPolicy: WARNING:: Cannot create VM!!!!");
        }
        logMaxNumHostsUsed();
        return result;
    }

    public boolean allocateHostForVmMinDED(Vm vm) {
        // allocate VM to the host which causes the least rise in energy consumption

        int numHosts = getHostList().size();

        double[] perfPerWatt = new double[numHosts];
        double[] networkDist = new double[numHosts];
        double[] utilization = new double[numHosts];
        double[] bwUtil = new double[numHosts];

        // Compute physical distance from connected hosts to all eligible hosts
        Task vmTask = getTaskIdOfTheInstanceInVm((SDNVm)vm);
        ArrayList<Task> predecessorTasks = vmTask.getPredecessorTasks();
        ArrayList<SDNHost> hostList = new ArrayList<>();
        for (int i = 0; i < predecessorTasks.size(); i++) {
            Task predecessor = predecessorTasks.get(i);
            if (predecessor.getMessageVol() == 0)
                continue;
            predecessor.getInstanceHostMap().forEach((instanceVm, host) -> {
                if (hostList.indexOf(host) == -1)
                    hostList.add(host);
            });
        }

        LinkSelectionPolicy linkSelector = new LinkSelectionPolicyBandwidthAllocation();
        for (int i = 0; i < numHosts; i++) {
            SDNHost host = (SDNHost)getHostList().get(i);
            for (int j = 0; j < hostList.size(); j++) {
                networkDist[i] =+ computeMinNetworkHops(linkSelector, host, hostList.get(j), 0);
            }

            // Compute performance per watt
            perfPerWatt[i] = host.getPerformancePerWatt(host.getTotalMips(), host);
            // Utilization as a fraction of execution time...higher utilizations are preferred
            utilization[i] = ((host.getNumberOfPes() - host.getNumberOfFreePes())/host.getPeList().size())/(vm.getMips()/(host.getPeList().get(0).getMips()));
            // Bandwidth utilization as a fraction of execution time...
            bwUtil[i] = (double) getFreeBw().get(i) / this.hostTotalBw;
        }

        boolean result = false;

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double perf = 0;
            double networkDistance = Double.POSITIVE_INFINITY;
            double utilizationCurr = 0;
            double bandwidthUtil = Double.POSITIVE_INFINITY;

            Host currHost = null;
            int idx = -1;
            // we want the host with high ppw in use
            for (int i = 0; i < numHosts; i++) {
                if (perfPerWatt[i] > perf) {
                    perf = perfPerWatt[i];
                    idx = i;
                }
            }

            if (idx == -1)
                break;

            List<SDNHost> eligibleHostsPPW = new ArrayList<>();
            List<SDNHost> eligibleHostsPpwConnect = new ArrayList<>();
            List<SDNHost> eligibleHostsPpwConnectUtil = new ArrayList<>();
            List<SDNHost> eligibleHostsPpwConnectUtilBw = new ArrayList<>();

            for (int i = 0; i < numHosts; i++) {
                if (perf == perfPerWatt[i]) {
                    eligibleHostsPPW.add((SDNHost)getHostList().get(i));
                }
            }

            if (eligibleHostsPPW.size() > 1) {
                // Consider physical location w.r.t
                for (Host host: eligibleHostsPPW) {
                    int index = getHostIndex(getHostList(), host);
                    if (networkDist[index] < networkDistance) {
                        networkDistance = networkDist[index];
                        idx = index;
                    }
                }

                for (Host host: eligibleHostsPPW) {
                    int index = getHostIndex(getHostList(), host);
                    if (networkDist[index] == networkDistance) {
                        eligibleHostsPpwConnect.add((SDNHost)host);
                    }
                }

                if (eligibleHostsPpwConnect.size() > 1) {
                    //Consider utilization
                    for (Host host: eligibleHostsPpwConnect) {
                        int index = getHostIndex(getHostList(), host);
                        if (utilization[index] > utilizationCurr) {
                            utilizationCurr = utilization[index];
                            idx = index;
                        }
                    }

                    for (Host host: eligibleHostsPpwConnect) {
                        int index = getHostIndex(getHostList(), host);
                        if (utilization[index] == utilizationCurr) {
                            eligibleHostsPpwConnectUtil.add((SDNHost)host);
                        }
                    }

                    if (eligibleHostsPpwConnectUtil.size() > 1) {

                        //consider network bandwidth
                        for (Host host: eligibleHostsPpwConnectUtil) {
                            int index = getHostIndex(getHostList(), host);
                            if (bwUtil[index] < bandwidthUtil) {
                                bandwidthUtil = bwUtil[index];
                                idx = index;
                            }
                        }

                        for (Host host: eligibleHostsPpwConnectUtil) {
                            int index = getHostIndex(getHostList(), host);
                            if (bandwidthUtil == bwUtil[index]) {
                                eligibleHostsPpwConnectUtilBw.add((SDNHost)getHostList().get(index));
                            }
                        }

                        //Select any of among the remaining hosts
                        currHost = eligibleHostsPpwConnectUtilBw.get(0);
                        perfPerWatt[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
                        networkDist[idx] = Double.POSITIVE_INFINITY; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
                        utilization[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
                        bwUtil[idx] = Double.POSITIVE_INFINITY; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.

                    } else if (eligibleHostsPpwConnectUtil.size() == 1) {
                        currHost = getHostList().get(idx);
                        perfPerWatt[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
                        networkDist[idx] = Double.POSITIVE_INFINITY; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
                        utilization[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
                    }

                } else if (eligibleHostsPpwConnect.size() == 1) {
                    currHost = getHostList().get(idx);
                    perfPerWatt[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
                    networkDist[idx] = Double.POSITIVE_INFINITY; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
                }

            } else if (eligibleHostsPPW.size() == 1) {
                currHost = getHostList().get(idx);
                perfPerWatt[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
            }

            if( getFreePes().get(idx) < vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if( getFreeBw().get(idx) < vm.getBw()) {
                //System.err.println("not enough BW");
                //Cannot host the VM
                continue;
            }

            if( getFreePes().get(idx) < vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if( currHost.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }
            result = allocateHostForVm(vm, currHost);
            if (result) {
                ((SDNHost)currHost).setActive(true);
                break;
            }
        }
        return result;
    }

    public int getHostIndex(List<Host> hostList, Host host) {
        for (int i = 0; i < hostList.size(); i++) {
            if (hostList.get(i).getId() == host.getId())
                return i;
        }
        return 0;
    }

    protected double computeMinNetworkHops(LinkSelectionPolicy linkSelector, Node src, Node dest, double noHops) {
        if (src.equals(dest))
            return noHops;
        List<Link> nextLinkCandidates = src.getRoute(dest);
        Link nextLink = linkSelector.selectLink(nextLinkCandidates, 0, src, dest, src);
        Node nextHop = nextLink.getOtherNode(src);
        return computeMinNetworkHops(linkSelector, nextHop, dest, ++noHops);
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
        List<SDNHost> underUtilizedHosts = new ArrayList<>();

        List<SDNVm> vmMigrationList = new ArrayList<>();

        /*
        // Identifying over utilized hosts based on a static CPU utilisation threshold. There are other ways of doing it....
        hosts.forEach(host -> {
            if (isOverUtilisedHost(host)) {
                overUtilizedHosts.add(host);
                vmMigrationList.addAll(migrationVmList(host));
            }
        });
        */

        hosts.forEach(host -> {
            if (isUnderUtilisedHost(host) && host.isActive()) {
                if (host.getVmList().size() == 0) {
                    host.setActive(false);

                } else {
                    underUtilizedHosts.add(host);
                    vmMigrationList.addAll(migrationVmList(host));
                }
            }
        });


        // Compute hosts that are not over-utilized
        List<SDNHost> eligibleHosts = new ArrayList<>();
        hosts.forEach(host -> {
            if (overUtilizedHosts.indexOf(host) == -1 && underUtilizedHosts.indexOf(host) == -1)
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
                if (connectedHosts.indexOf((SDNHost) conVm.getHost()) == -1 && eligibleHosts.indexOf(conVm.getHost()) != -1)
                    connectedHosts.add((SDNHost) conVm.getHost());
            });

            // First try to find an eligible host from the connectedHosts ... if unsuccessful initiate ACO from a partial state
            boolean assignedVmToConnectedHost = false;
            for (int i = 0; i < connectedHosts.size(); i++) {
                // AMANDAAAA Don't do this here........ Because it happens later on in processVmMigrate method at /home/student.unimelb.edu.au/amjayanetti/.m2/repository/org/cloudbus/cloudsim/cloudsim/4.0/cloudsim-4.0-sources.jar!/org/cloudbus/cloudsim/Datacenter.java:511
                if (canAllocateVmToHost(vm, connectedHosts.get(i))) {
                    assignedVmToConnectedHost = true;
                    Map<String, Object> vmToHostMap = new HashMap<>();
                    vmToHostMap.put("host", connectedHosts.get(i));
                    vmToHostMap.put("vm", vm);
                    migrationMap.add(vmToHostMap);
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


    public boolean migrateVmToMipsMostFullHost(Vm vm, List<SDNHost> eligibleHosts) {
        if (getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            return false;
        }

        int numHosts = eligibleHosts.size();

        // 1. Find/Order the best host for this VM by comparing a metric
        int requiredPes = vm.getNumberOfPes();
        double requiredMips = vm.getCurrentRequestedTotalMips();
        long requiredBw = vm.getCurrentRequestedBw();

        boolean result = false;

        double[] freeResources = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            double mipsFreePercent = (double)eligibleHosts.get(i).getAvailableMips() / this.hostTotalMips;

            freeResources[i] = mipsFreePercent;
        }

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double lessFree = Double.POSITIVE_INFINITY;
            int idx = -1;

            // we want the host with less pes in use
            for (int i = 0; i < numHosts; i++) {
                if (freeResources[i] < lessFree) {
                    lessFree = freeResources[i];
                    idx = i;
                }
            }
            freeResources[idx] = Double.POSITIVE_INFINITY;
            Host host = eligibleHosts.get(idx);

            // Check whether the host can hold this VM or not.
            if( getFreeMips().get(idx) < requiredMips) {
                //System.err.println("not enough MIPS");
                //Cannot host the VM
                continue;
            }
            if( getFreeBw().get(idx) < requiredBw) {
                //System.err.println("not enough BW");
                //Cannot host the VM
                continue;
            }

            if( getFreePes().get(idx) < vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            result = host.vmCreate(vm);

            if (result) { // if vm were succesfully created in the host
                getVmTable().put(vm.getUid(), host);
                getUsedPes().put(vm.getUid(), requiredPes);
                getFreePes().set(idx, getFreePes().get(idx) - requiredPes);

                getUsedMips().put(vm.getUid(), (long) requiredMips);
                getFreeMips().set(idx,  (long) (getFreeMips().get(idx) - requiredMips));

                getUsedBw().put(vm.getUid(), (long) requiredBw);
                getFreeBw().set(idx,  (long) (getFreeBw().get(idx) - requiredBw));

                ((SDNHost)host).setActive(true);
                break;
            }
        }

        logMaxNumHostsUsed();
        return result;
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


    protected double getUnderUtilizationThreshold() {
        return underUtilizationThreshold;
    }

    protected boolean isOverUtilisedHost(SDNHost host) {
        double totalRequestedMips = 0;
        for (Vm vm : host.getVmList()) {
            totalRequestedMips += vm.getCurrentRequestedTotalMips();
        }
        double utilization = totalRequestedMips / host.getTotalMips();
        return utilization > getUtilizationThreshold();
    }

    protected boolean isUnderUtilisedHost(SDNHost host) {
        double totalRequestedMips = 0;
        for (Vm vm : host.getVmList()) {
            totalRequestedMips += vm.getCurrentRequestedTotalMips();
        }
        double utilization = totalRequestedMips / host.getTotalMips();
        return utilization < getUnderUtilizationThreshold();
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
        Task task = getTaskIdOfTheInstanceInVm((SDNVm)vm);
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
            ((SDNHost)host).setActive(true);
            task.getInstanceHostMap().put((SDNVm)vm, (SDNHost)host);
            return true;
        }

        return false;
    }

    public boolean allocateHostsForVmEnreal(Task task, SDNVm mockVm, List<SDNVm> vmList, List<SDNHost> hosts, Map<SDNHost, List<SDNVm>> currAllocMap) {

        boolean result = false;

        for(int tries = 0; result == false && tries < hosts.size(); tries++) {// we still trying until we find a host or until we try all of them
            SDNHost host = hosts.get(tries);
            long  totalAllocStorage = 0;
            long totalAllocBw = 0;
            long totalAllocMips = 0;
            int totalAllocRam = 0;
            int totalAllocPes = 0;

            List<SDNVm> vmsMappedToHost = currAllocMap.get(host);
            if (vmsMappedToHost != null) {
                totalAllocStorage = vmsMappedToHost.stream().mapToLong(Vm::getSize).sum();
                totalAllocBw = vmsMappedToHost.stream().mapToLong(Vm::getCurrentRequestedBw).sum();
                totalAllocMips = vmsMappedToHost.stream().mapToLong(SDNVm::getTotalMips).sum();
                totalAllocRam = vmsMappedToHost.stream().mapToInt(Vm::getCurrentRequestedRam).sum();
                totalAllocPes = vmsMappedToHost.stream().mapToInt(Vm::getNumberOfPes).sum();
            }


            if (host.getStorage() - totalAllocStorage >= mockVm.getSize() && host.getAvailableBandwidth() - totalAllocBw >= mockVm.getCurrentRequestedBw()
                    && host.getAvailableMips() - totalAllocMips >= mockVm.getTotalMips() && host.getNumberOfFreePes() - totalAllocPes > mockVm.getNumberOfPes()
                    && host.getPeList().get(0).getMips() > mockVm.getMips() /* // each virtual PE of a VM must require not more than the capacity of a physical PE */
                    && host.getRamProvisioner().getAvailableRam() - totalAllocRam  >= mockVm.getCurrentRequestedRam()) {
                // Found a host with adequate capacity
                if (vmsMappedToHost == null)
                    vmsMappedToHost = vmList;
                else
                    vmsMappedToHost.addAll(vmList);
                currAllocMap.put(host, vmsMappedToHost);
                result = true;
            }


            /*
            // Check whether the host can hold this VM or not.
            if( host.getAvailableMips() < mockVm.getTotalMips()) {
                //System.err.println("not enough MIPS");
                //Cannot host the VM
                continue;
            }
            if( host.getAvailableBandwidth() < mockVm.getBw()) {
                //System.err.println("not enough BW");
                //Cannot host the VM
                continue;
            }

            if(host.getNumberOfFreePes() < mockVm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if( host.getPeList().get(0).getMips() < mockVm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            for (int k = 0; k < vmList.size(); k++) {
                result = allocateHostForVm(vmList.get(k), host);
                if (result) {
                    task.getScheduledInstances().add(vmList.get(k));
                    task.getPendingInstances().remove(vmList.get(k));
                } else {
                    break;
                }
            }
            */

        }
        return result;
    }

    /**
     * Allocates a host for a given VM.
     *
     * @param task the task to allocate hosts to
     * @return $true if the host could be allocated; $false otherwise
     * @pre $none
     * @post $none
     */
    public ArrayList<SDNVm> allocateHostsForTask(Task task) {
        ArrayList<SDNVm> taskVms = new ArrayList<>();

        task.getPendingInstances().forEach(vm -> {
            // Since normal copying of of arraylists pass by reference
            taskVms.add(vm);
        });

        ArrayList<SDNVm> scheduledVms = new ArrayList<>();
        getTaskVmMap().put(task, taskVms);
        //jobTaskVMMap.put(task.getJob_id(), taskVmMap);

/*
        List<SDNHost> hostList = getHostList();
        try {
            NewAntColonyOptimization antColony = new NewAntColonyOptimization(hostList, taskVms);
            Multimap<SDNHost, SDNVm> allocMap = antColony.startAntOptimization();

            Set keySet = allocMap.keySet();
            Iterator keyIterator = keySet.iterator();
            while (keyIterator.hasNext()) {
                SDNHost host = (SDNHost) keyIterator.next();
                ArrayList<SDNVm> vms = new ArrayList<SDNVm>(allocMap.get(host));
                Iterator itr = vms.iterator();
                boolean success = false;
                while (itr.hasNext()) {
                    Vm vm = (Vm) itr.next();
                    success = allocateHostForVm(vm, host);
                    if (success)
                        scheduledVms.add((SDNVm)vm);
                }
            }
        } catch (Exception e) {
            return null;
        }
*/

        // Allocation of vms to first fit host --- AMANDAAA Use these methods for comparisons and can implement EnREAL as well here itself because all it does is order the PMs in increasing order of Power something and find the first fit one...
        boolean success = true;
        for (SDNVm vm : taskVms) {
            //boolean result = allocateHostForVm(vm);
            success = allocateHostForVmCombinedMostFullFirst(vm, task);
            //success = allocateHostForVmEnergyAwareGreedy(vm);
            //success = allocateHostForVmMinDED(vm);
            if (success) {
                scheduledVms.add(vm);
            }
        }


        task.getScheduledInstances().addAll(scheduledVms);
        task.getPendingInstances().removeAll(scheduledVms);
        return scheduledVms;
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

