/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2015, The University of Melbourne, Australia
 */
package org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager;

import com.google.common.collect.HashMultimap;
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
import java.util.function.Predicate;
import java.util.stream.IntStream;

/**
 * VM Allocation Policy - BW and Compute combined, MFF.
 * When select a host to create a new VM, this policy chooses
 * the most full host in terms of both compute power and network bandwidth.
 *
 * @author Jungmin Son
 * @since CloudSimSDN 1.0
 */
public class VmAllocationPolicyToTasks extends VmAllocationPolicy implements PowerUtilizationMaxHostInterface {

    public double getHostTotalMips() {
        return hostTotalMips;
    }

    protected final double hostTotalMips;

    public double getHostTotalBw() {
        return hostTotalBw;
    }

    protected final double hostTotalBw;
    protected final int hostTotalPes;

    static ArrayList<String> podList = new ArrayList<>();
    private Map<String, Map<Task, List<SDNVm>>> jobTaskVMMap = new HashMap<>();

    public static Map<Task, List<SDNVm>> getTaskVmMap() {
        return taskVmMap;
    }

    public static Multimap<Task, Integer> getTaskVmIdMap() {
        return taskVmIdMap;
    }

    private static Map<Task, List<SDNVm>> taskVmMap;
    private static Multimap<Task, Integer> taskVmIdMap;

    private double utilizationThreshold = 0.9;
    private double underUtilizationThreshold = 0.2;

    private static List<Host> hostList;
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

    public double[][] getHostMatrix() {
        return hostMatrix;
    }

    private double[][] hostMatrix;

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
        taskVmIdMap = HashMultimap.create();
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
        this.hostMatrix = generateHostMatrix(getHostList());

        hostList = getHostList();
        Collections.shuffle(hostList);
    }

    /**
     * Initialize host matrix based on the number of hops between hosts
     */
    public double[][] generateHostMatrix(List<SDNHost> hostList) {
        int numHosts = hostList.size();
        // hostMatrix stores minimum network hops from every host to every other host
        double[][] hostMatrix = new double[numHosts][numHosts];
        LinkSelectionPolicy linkSelector = new LinkSelectionPolicyBandwidthAllocation();

        IntStream.range(0, numHosts).forEach(i -> IntStream.range(0, numHosts).forEach(j -> {
            hostMatrix[i][j] = computeMinNetworkHops(linkSelector, hostList.get(i), hostList.get(j), 0);
        }));

        return hostMatrix;
    }

    public double convertWeightedMetric(double mipsPercent, double bwPercent) {
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
                    SDNHost hostSdn = (SDNHost)host;
                    hostSdn.setActive(true);
                    if (podList.indexOf(hostSdn.getPodId()) == -1) {
                        podList.add(hostSdn.getPodId());
                    }
                    break;
                } else {
                    freePesTmp.set(idx, Integer.MIN_VALUE);
                }
                tries++;
            } while (!result && tries < getFreePes().size());

        }

        return result;
    }

    public boolean allocateHostForVmFF(Vm vm, Task task) {

        // Trying using like 50 hosts only (for 500 jobs)
        // then shuffle host list before selecting
        if (getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            return false;
        }

        int numHosts = getHostList().size();

        // 1. Find/Order the best host for this VM by comparing a metric
        int requiredPes = vm.getNumberOfPes();
        double requiredMips = vm.getCurrentRequestedTotalMips();
        long requiredBw = vm.getCurrentRequestedBw();

        boolean result = false;

        Collections.shuffle(hostList);

        for (int i = 0; i < 100; i++) {
            Host host = hostList.get(i);
            int idx = i;

            if( host.getBw() - host.getVmList().stream().mapToDouble(Vm::getBw).sum() < requiredBw) {
                System.err.println("not enough BW:"+getFreeBw().get(idx)+", req="+requiredBw);
                //Cannot host the VM
                //continue;
            }

            if( host.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getPeList().size() - host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum()) <= vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getTotalMips() - host.getVmList().stream().mapToDouble(Vm::getMips).sum()) <= vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            //result = host.vmCreate(vm);
            result = allocateHostForVm(vm,host);

            if (result) { // if vm were succesfully created in the host
                int pp = idx;
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

        logMaxNumHostsUsed();
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

        int numHosts = hostList.size();

        double[] perPerWatt = new double[numHosts];

        for (int i = 0; i < numHosts; i++) {
            SDNHost host = (SDNHost)hostList.get(i);
            perPerWatt[i] = host.getPerformancePerWatt(host.getTotalMips(), host);
            double mipsFreePercent = host.getAvailableMips()/ this.hostTotalMips;
            double bwFreePercent = (double)host.getAvailableBandwidth() / this.hostTotalBw;
            host.setFreeResourcePercentage(this.convertWeightedMetric(mipsFreePercent, bwFreePercent));
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


            double utilization = Double.POSITIVE_INFINITY;
            for (int i = 0; i < numHosts; i++) {
                if (perPerWatt[i] == powerUtilization) {
                    if (((SDNHost)hostList.get(i)).getFreeResourcePercentage() < utilization) {
                        idx = i;
                        utilization = ((SDNHost)getHostList().get(i)).getFreeResourcePercentage();
                    }
                }
            }

            /*
            ArrayList<SDNHost> eligibleHosts = new ArrayList<>();
            Multimap<Double, Integer> eligibleHostsResUtil = HashMultimap.create();
            for (int i = 0; i < numHosts; i++) {
                if (perPerWatt[i] == powerUtilization) {
                    SDNHost tHost = (SDNHost)hostList.get(i);
                    eligibleHosts.add(tHost);
                    eligibleHostsResUtil.put(tHost.getFreeResourcePercentage(), i);
                }
            }

            OptionalDouble resUsageLowest = eligibleHosts.stream().mapToDouble(SDNHost::getFreeResourcePercentage).min();
            idx  = eligibleHostsResUtil.get(resUsageLowest.getAsDouble()).iterator().next();
            */

            perPerWatt[idx] = 0; // so if allocation of vm to this host fails, in successive iterations this host will not be reconsidered.
            Host host = hostList.get(idx); //Random rand = new Random(); eligibleHosts.get(rand.nextInt(eligibleHosts.size()));

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

            if((host.getPeList().size() - host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum()) <= vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getTotalMips() - host.getVmList().stream().mapToDouble(Vm::getMips).sum()) <= vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            result = allocateHostForVm(vm, host);
            if (result) {
                ((SDNHost) host).setActive(true);
                Task task = getTaskIdOfTheInstanceInVm((SDNVm)vm);
                task.getInstanceHostMap().put((SDNVm)vm, (SDNHost)host);
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

        /*
        if (getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            return false;
        }*/

        double[] freeResources = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            //double mipsFreePercent = (double)getFreeMips().get(i) / this.hostTotalMips;
            double mipsFreePercent = (getHostList().get(i).getTotalMips() - getHostList().get(i).getVmList().stream().mapToDouble(Vm::getMips).sum())/getHostList().get(i).getTotalMips();
            //double bwFreePercent = (double)getFreeBw().get(i) / this.hostTotalBw;
            double bwFreePercent = (getHostList().get(i).getBw() - getHostList().get(i).getVmList().stream().mapToDouble(Vm::getBw).sum())/ getHostList().get(i).getBw();

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

            if( host.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getPeList().size() - host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum()) <= vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getTotalMips() - host.getVmList().stream().mapToDouble(Vm::getMips).sum()) <= vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            //result = host.vmCreate(vm);
            result = allocateHostForVm(vm,host);

            if (result) { // if vm were succesfully created in the host
                int pp = idx;
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

    public boolean allocateHostForVmCombinedMostFullFirst(Vm vm) {

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
            //double mipsFreePercent = (double)getFreeMips().get(i) / this.hostTotalMips;
            double mipsFreePercent = (getHostList().get(i).getTotalMips() - getHostList().get(i).getVmList().stream().mapToDouble(Vm::getMips).sum())/getHostList().get(i).getTotalMips();
            //double bwFreePercent = (double)getFreeBw().get(i) / this.hostTotalBw;
            double bwFreePercent = (getHostList().get(i).getBw() - getHostList().get(i).getVmList().stream().mapToDouble(Vm::getBw).sum())/ getHostList().get(i).getBw();

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

            if( host.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getPeList().size() - host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum()) <= vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getTotalMips() - host.getVmList().stream().mapToDouble(Vm::getMips).sum()) <= vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            //result = host.vmCreate(vm);
            result = allocateHostForVm(vm,host);

            if (result) { // if vm were succesfully created in the host
                int pp = idx;
                getVmTable().put(vm.getUid(), host);
                getUsedPes().put(vm.getUid(), requiredPes);
                getFreePes().set(idx, getFreePes().get(idx) - requiredPes);

                getUsedMips().put(vm.getUid(), (long) requiredMips);
                getFreeMips().set(idx,  (long) (getFreeMips().get(idx) - requiredMips));

                getUsedBw().put(vm.getUid(), (long) requiredBw);
                getFreeBw().set(idx,  (long) (getFreeBw().get(idx) - requiredBw));

                ((SDNHost)host).setActive(true);
                if (podList.indexOf(((SDNHost)host).getPodId()) == -1) {
                    podList.add(((SDNHost)host).getPodId());
                }
                break;
            }
        }

        if(!result) {
            System.err.println("VmAllocationPolicy: WARNING:: Cannot create VM!!!!");
        }
        logMaxNumHostsUsed();
        return result;
    }

    public boolean herosDAG(Vm vm, Task task, List<SDNHost> currClique) {

        int numHosts = getHostList().size();
        // 1. Find/Order the best host for this VM by comparing a metric
        int requiredPes = vm.getNumberOfPes();
        double requiredMips = vm.getCurrentRequestedTotalMips();
        long requiredBw = vm.getCurrentRequestedBw();

        boolean result = false;

        if (getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            return false;
        }

        double[] utilization = new double[numHosts];
        double[] noHops = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            SDNHost candidateHost = (SDNHost) getHostList().get(i);
            double mipsCurrUtil = candidateHost.getVmList().stream().mapToDouble(Vm::getMips).sum();
            double bwCurrUtil = candidateHost.getVmList().stream().mapToDouble(Vm::getBw).sum();
            double bwUtilPercent = bwCurrUtil/candidateHost.getBandwidth();
            double mipsUtilPercent = mipsCurrUtil/candidateHost.getTotalMips();

            double utilizationIndex = 0.0;
            double h_s = 0.0;
            double q_s = 0.0;
            q_s = Math.exp(-1 * Math.pow(2*bwUtilPercent, 2));

            // 50 is working power dissipation
            h_s = mipsUtilPercent * 50 * (1- (1.2 *(1/(1 + Math.exp(-1 * (110/candidateHost.getTotalMips()) * (mipsCurrUtil - 0.9 * candidateHost.getTotalMips()))))));

            utilizationIndex = h_s * q_s;

            utilization[i] = utilizationIndex;

            double hops = 0.0;
            for (SDNHost currHost : currClique) {
                // Distance from candidate host to all the other hosts
                hops += computeMinNetworkHops(new LinkSelectionPolicyBandwidthAllocation(), candidateHost, currHost, 0);
            }
            noHops[i] = hops;
        }

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double util = Double.NEGATIVE_INFINITY;
            int idx = -1;

            // we want the host with less free pes
            for (int i = 0; i < numHosts; i++) {
                if (utilization[i] > util) {
                    util = utilization[i];
                    idx = i;
                }
            }
            utilization[idx] = Double.NEGATIVE_INFINITY;
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

            if( host.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getPeList().size() - host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum()) <= vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getTotalMips() - host.getVmList().stream().mapToDouble(Vm::getMips).sum()) <= vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            //result = host.vmCreate(vm);
            result = allocateHostForVm(vm,host);

            if (result) { // if vm were succesfully created in the host
                int pp = idx;
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

    public boolean herosDAGNet(Vm vm, Task task, List<SDNHost> currClique) {

        int numHosts = getHostList().size();
        // 1. Find/Order the best host for this VM by comparing a metric
        int requiredPes = vm.getNumberOfPes();
        double requiredMips = vm.getCurrentRequestedTotalMips();
        long requiredBw = vm.getCurrentRequestedBw();

        boolean result = false;

        /*
        if (getVmTable().containsKey(vm.getUid())) { // if this vm was not created
            return false;
        }
*/
        double[] utilization = new double[numHosts];
        double[] inverseHops = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            SDNHost candidateHost = (SDNHost) getHostList().get(i);
            double mipsCurrUtil = candidateHost.getVmList().stream().mapToDouble(Vm::getMips).sum();
            double bwCurrUtil = candidateHost.getVmList().stream().mapToDouble(Vm::getBw).sum();
            double bwUtilPercent = bwCurrUtil/candidateHost.getBandwidth();
            double mipsUtilPercent = mipsCurrUtil/candidateHost.getTotalMips();

            double utilizationIndex = 0.0;
            double h_s = 0.0;
            double q_s = 0.0;
            q_s = Math.exp(-1 * Math.pow(2*bwUtilPercent, 2));

            // 50 is working power dissipation
            h_s = candidateHost.getPerformancePerWatt(mipsCurrUtil, candidateHost)* (1- (1.2 *(1/(1 + Math.exp(-1 * (110/candidateHost.getTotalMips()) * (mipsCurrUtil - 0.9 * candidateHost.getTotalMips()))))));

            utilizationIndex = h_s * q_s;

            utilization[i] = utilizationIndex;

            double hops = 0.0;
            for (SDNHost currHost : currClique) {
                // Distance from candidate host to all the other hosts
                hops += computeMinNetworkHops(new LinkSelectionPolicyBandwidthAllocation(), candidateHost, currHost, 0);
            }
            inverseHops[i] = 1/(hops + 0.0001);
        }

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double util = Double.NEGATIVE_INFINITY;
            int idx = -1;

            // we want the host with less free pes
            for (int i = 0; i < numHosts; i++) {
                if (utilization[i] > util) {
                    util = utilization[i];
                    idx = i;
                }
            }

            double maxHops = Double.NEGATIVE_INFINITY;

            for (int i = 0; i < numHosts; i++) {
                if (utilization[i] == util) {
                    if (inverseHops[i] > maxHops) {
                        idx = i;
                    }
                }
            }

            Host host = getHostList().get(idx); //eligibleHosts.get(rand.nextInt(eligibleHosts.size()));
            utilization[idx] = Double.NEGATIVE_INFINITY;

            // Check whether the host can hold this VM or not.
            if( getFreeMips().get(idx) < requiredMips) {
                //System.err.println("not enough MIPS:"+getFreeMips().get(idx)+", req="+requiredMips);
                //Cannot host the VM
                continue;
            }
            if( getFreeBw().get(idx) < requiredBw) {
                //System.err.println("not enough BW:"+getFreeBw().get(idx)+", req="+requiredBw);
                //Cannot host the VM
                //continue;
            }

            if( host.getPeList().get(0).getMips() < vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getPeList().size() - host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum()) <= vm.getNumberOfPes()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            if((host.getTotalMips() - host.getVmList().stream().mapToDouble(Vm::getMips).sum()) <= vm.getMips()) {
                //System.err.println("not enough PES");
                //Cannot host the VM
                continue;
            }

            //result = host.vmCreate(vm);
            result = allocateHostForVm(vm,host);

            if (result) { // if vm were succesfully created in the host
                int pp = idx;
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

    protected boolean canAllocateVmToHost(SDNVm vm, SDNHost host, Multimap<SDNHost, SDNVm> allocMap) {
        ArrayList<SDNVm> allocatedVms = new ArrayList<SDNVm>(allocMap.get(host));

        long totalAllocStorage = allocatedVms.stream().mapToLong(Vm::getSize).sum();
        long totalAllocBw = allocatedVms.stream().mapToLong(Vm::getCurrentRequestedBw).sum();
        long totalAllocMips = allocatedVms.stream().mapToLong(SDNVm::getTotalMips).sum();
        int totalAllocRam = allocatedVms.stream().mapToInt(Vm::getCurrentRequestedRam).sum();
        int totalAllocPes = allocatedVms.stream().mapToInt(Vm::getNumberOfPes).sum() + host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum(); // because free pes do not properly get computed

        if (host.getStorage() - totalAllocStorage >= vm.getSize() && host.getAvailableBandwidth() - totalAllocBw >= vm.getCurrentRequestedBw()
                && host.getVmScheduler().getAvailableMips() - totalAllocMips >= vm.getTotalMips() && host.getPeList().size()- totalAllocPes >= vm.getNumberOfPes() && host.getPeList().get(0).getMips() >= vm.getMips() /* // each virtual PE of a VM must require not more than the capacity of a physical PE */ && host.getRamProvisioner().getAvailableRam() - totalAllocRam  >= vm.getCurrentRequestedRam())
            return true;
        else
            return false;
    }

    protected double[] computeDesirabilityOfHosts(Task task, List<SDNHost> currClique, ArrayList<SDNHost> candidateHosts, Multimap<SDNHost, SDNVm> allocMap) {
        int numHosts = candidateHosts.size();
        double[] desirabilityIndex = new double[numHosts];
        for (int i = 0; i < numHosts; i++) {
            desirabilityIndex[i] = 0;
        }

        List<Double> numerators = new ArrayList<>();
        double denominator = 0.0;

        for (SDNHost candidateHost : candidateHosts) {
            ArrayList<SDNVm> allocatedVms = new ArrayList<SDNVm>(allocMap.get(candidateHost));
            long totalAllocBw = allocatedVms.stream().mapToLong(Vm::getCurrentRequestedBw).sum();
            long totalAllocMips = allocatedVms.stream().mapToLong(SDNVm::getTotalMips).sum();

            double mipsFreePercent = (candidateHost.getAvailableMips() - totalAllocMips) / this.getHostTotalMips();
            double bwFreePercent = (candidateHost.getAvailableBandwidth() - totalAllocBw) / this.getHostTotalBw();
            candidateHost.setFreeResourcePercentage(this.convertWeightedMetric(mipsFreePercent, bwFreePercent));
        }

        for (SDNHost candidateHost : candidateHosts) {
            // Multiplying pheromone factor by the heuristic factor which is the inverse of estimated rise in power for a unit of mips to be processed.
            double heuristicFactor = 0.0;
            double totalHops = 0.0;
            double resUtil = 0.0;
            for (SDNHost currHost : currClique) {
                // Distance from candidate host to all the other hosts
                totalHops += (1/(computeMinNetworkHops(new LinkSelectionPolicyBandwidthAllocation(), candidateHost, currHost, 0) + 0.000001));
            }

            if (currClique.size() == 0 || totalHops == 0)
                totalHops = 1; // let resource usage determine the best host

            resUtil = 1/(candidateHost.getFreeResourcePercentage() + 0.00000001);

            heuristicFactor = totalHops * resUtil; // Could use addition or multiplication here...
            //heuristicFactor = Math.pow(heuristicFactor, 2);

            double numerator = heuristicFactor;


            // ***************   //
            // phermoneFactor *= Math.pow((1.0 / candidateHost.getEstimatedRiseInPowerConsumptionForNextPeriod(1000, 50)), gamma);

            // Multiplying pheromone factor by the heuristic factor which is the inverse of free mips (i.e. capacity). So in this approach hosts that have less capacity will be
            // favored for successive allocations as well. This was overall host utilisation would improve...
            //phermoneFactor *= Math.pow((1.0 / candidateHost.getVmScheduler().getAvailableMips()), gamma);
            denominator += numerator;
            if (task.getBlacklist().indexOf(candidateHost) != -1)
                numerators.add(0.0);
            else
                numerators.add(numerator);
        }

        for (int i = 0; i < candidateHosts.size(); i++) {
            double probability = numerators.get(i) / denominator;
            int index = candidateHosts.indexOf(candidateHosts.get(i));
            desirabilityIndex [index] = probability;
        }
        return desirabilityIndex;
    }

    public Double desirabilityCoeff(List<SDNHost> currClique, SDNHost candidateHost, Multimap<SDNHost, SDNVm> allocMap) {

        double utilizationIndex = 0.0;
        double h_s = 0.0;
        double q_s = 0.0;

        double mipsCurrUtil = candidateHost.getVmList().stream().mapToDouble(Vm::getMips).sum() + allocMap.get(candidateHost).stream().mapToDouble(Vm::getMips).sum();
        double bwCurrUtil = candidateHost.getVmList().stream().mapToDouble(Vm::getBw).sum() + allocMap.get(candidateHost).stream().mapToDouble(Vm::getBw).sum();
        double bwUtilPercent = bwCurrUtil/candidateHost.getBandwidth();
        double mipsUtilPercent = mipsCurrUtil/candidateHost.getTotalMips();

        q_s = Math.exp(-1 * Math.pow(2*bwUtilPercent, 2));

        // 50 is working power dissipation
        h_s = mipsUtilPercent * 50 * (1- (1.2 *(1/(1 + Math.exp(-1 * (110/candidateHost.getTotalMips()) * (mipsCurrUtil - 0.9 * candidateHost.getTotalMips()))))));

        utilizationIndex = h_s * q_s;
        double totalHops = 0;
        for (SDNHost currHost : currClique) {
            // Distance from candidate host to all the other hosts
            totalHops += computeMinNetworkHops(new LinkSelectionPolicyBandwidthAllocation(), candidateHost, currHost, 0);
        }
        return  (1 - 0.00001) * utilizationIndex + 0.00001 * 1/(totalHops + 0.000000001);
    }

    public Multimap<SDNHost, SDNVm> heuristic(ArrayList<SDNVm> vmList, Task task, List<SDNHost> currClique, ArrayList<SDNHost> candidateHosts) {

        ArrayList<SDNVm> taskVms = new ArrayList<>();
        vmList.forEach(vm -> {
            taskVms.add(vm);
        });


        Multimap<SDNHost, SDNVm> allocMap = HashMultimap.create();
        int numHosts = candidateHosts.size();
        boolean result = false;

        double [] freeResources = new double[numHosts];

        /*
        for (int i = 0; i < numHosts; i++) {
            SDNHost candidate = candidateHosts.get(i);
            double mipsFreePercent = (candidate.getTotalMips() - candidate.getVmList().stream().mapToDouble(Vm::getMips).sum())/ this.hostTotalMips;
            double bwFreePercent = (candidate.getAvailableBandwidth() - candidate.getVmList().stream().mapToDouble(Vm::getBw).sum()) / this.hostTotalBw;
            double totalHops = 0;
            for (SDNHost currHost : currClique) {
                // Distance from candidate host to all the other hosts
                totalHops += computeMinNetworkHops(new LinkSelectionPolicyBandwidthAllocation(), candidate, currHost, 0);
            }

            freeResources[i] = this.convertWeightedMetric(mipsFreePercent, bwFreePercent) + 0.00001 * totalHops;
        }
        */

        for (int i = 0; i < numHosts; i++) {
            SDNHost candidate = candidateHosts.get(i);
            freeResources[i] = desirabilityCoeff(currClique, candidate, allocMap);
        }

        int sizeOfCurrClique = currClique.size();

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            //double freeResMax = Double.POSITIVE_INFINITY;
            double freeResMax = Double.NEGATIVE_INFINITY;
            int idx = -1;

            /*
            // technically we need to recomute this value each time PMconn expands
            for (int i = 0; i < numHosts; i++) {
                if (freeResources[i] < freeResMax) {
                    freeResMax = freeResources[i];
                    idx = i;
                }
            }
            */

            for (int i = 0; i < numHosts; i++) {
                if (freeResources[i] > freeResMax) {
                    freeResMax = freeResources[i];
                    idx = i;
                }
            }

            Host host = candidateHosts.get(idx);

            ListIterator<SDNVm> itr = taskVms.listIterator();
            while (itr.hasNext()) {
                SDNVm sdnVm = itr.next();
                result = canAllocateVmToHost(sdnVm,(SDNHost) host, allocMap);
                if (result) {
                    // Add vm to host mapping
                    allocMap.put((SDNHost)host, sdnVm);
                    itr.remove();
                    if (currClique.indexOf((SDNHost)host) == -1) {
                        currClique.add((SDNHost) host);
                    }
                }
            }

            // freeResources[idx] = Double.POSITIVE_INFINITY;
            freeResources[idx] = Double.NEGATIVE_INFINITY;
            if (taskVms.size() == 0)
                break;

            if (currClique.size() > sizeOfCurrClique) {
                // recompute desirability coeff for all candidate hosts not yet considered
                for (int i = 0; i < numHosts; i++) {
                    if (freeResources[i] != Double.POSITIVE_INFINITY) {
                        freeResources[i] = desirabilityCoeff(currClique, candidateHosts.get(i), allocMap);
                    }
                }
                sizeOfCurrClique = currClique.size();
            }
        }
        return allocMap;
    }

    /*
    public Multimap<SDNHost, SDNVm> heuristic(ArrayList<SDNVm> vmList, Task task, List<SDNHost> currClique, ArrayList<SDNHost> candidateHosts) {

        ArrayList<SDNVm> taskVms = new ArrayList<>();
        vmList.forEach(vm -> {
            taskVms.add(vm);
        });

        Multimap<SDNHost, SDNVm> allocMap = HashMultimap.create();
        int numHosts = candidateHosts.size();
        boolean result = false;

        double[] desirabilityIndex = computeDesirabilityOfHosts(task, currClique, candidateHosts, allocMap);
        List<SDNHost> consideredHosts = new ArrayList<>();

        for(int tries = 0; result == false && tries < numHosts; tries++) {// we still trying until we find a host or until we try all of them
            double desirability = -1;
            int idx = -1;

            // we want the host with less free pes
            for (int i = 0; i < numHosts; i++) {
                if (desirabilityIndex[i] > desirability) {
                    desirability = desirabilityIndex[i];
                    idx = i;
                }
            }
            Host host = candidateHosts.get(idx);

            ListIterator<SDNVm> itr = taskVms.listIterator();
            while (itr.hasNext()) {
                SDNVm sdnVm = itr.next();
                result = canAllocateVmToHost(sdnVm,(SDNHost) host, allocMap);
                if (result) {
                    // Add vm to host mapping
                    allocMap.put((SDNHost)host, sdnVm);
                    itr.remove();
                    if (currClique.indexOf((SDNHost)host) == -1)
                        currClique.add((SDNHost)host);
                }
            }

            if (taskVms.size() == 0)
                break;

            // Recompute desirability for hosts since curr clique may have changed
            desirabilityIndex = computeDesirabilityOfHosts(task, currClique, candidateHosts, allocMap);

            consideredHosts.add((SDNHost)host);
            //Avoid already considered hosts from being re-considerd
            for (SDNHost conHost : consideredHosts) {
                int id = candidateHosts.indexOf(conHost);
                desirabilityIndex [id] = -1;
            }
        }
        return allocMap;
    }
    */

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
                Task task = getTaskIdOfTheInstanceInVm((SDNVm)vm);
                task.getInstanceHostMap().put((SDNVm)vm, (SDNHost)currHost);
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
        ArrayList<SDNHost> eligibleHosts = new ArrayList<>();
        hosts.forEach(host -> {
            if (overUtilizedHosts.indexOf(host) == -1 && underUtilizedHosts.indexOf(host) == -1)
                eligibleHosts.add(host);
        });

        List<Map<String, Object>> migrationMap = new ArrayList<>();

        vmMigrationList.forEach(vm -> {
                    ArrayList<SDNHost> connectedHostList = new ArrayList<>();
                    Task vmTask = getTaskIdOfTheInstanceInVm(vm);

                    // Adding predecessor servers to connected host list
                    ArrayList<Task> predecessorTasks = vmTask.getPredecessorTasks();
                    for (int i = 0; i < predecessorTasks.size(); i++) {
                        Task predecessor = predecessorTasks.get(i);
                        if (predecessor.getMessageVol() == 0)
                            continue;
                        predecessor.getInstanceHostMap().forEach((instanceVm, host) -> {
                            if (connectedHostList.indexOf(host) == -1)
                                connectedHostList.add(host);
                        });
                    }

                    // Adding servers in which instances of the same task are executing to predecessor host list
                    List<SDNVm> connectedVms = vmTask.getScheduledInstances();
                    connectedVms.forEach(conVm -> {
                        if (connectedHostList.indexOf((SDNHost) conVm.getHost()) == -1 && eligibleHosts.indexOf(conVm.getHost()) != -1)
                            connectedHostList.add((SDNHost) conVm.getHost());
                    });

                    ArrayList<SDNVm> migrationVm = new ArrayList<>();
                    migrationVm.add(vm);

                    Multimap<SDNHost, SDNVm> allocMap = heuristic(migrationVm, vmTask, connectedHostList, eligibleHosts);

                    Set keySet = allocMap.keySet();
                    Iterator keyIterator = keySet.iterator();
                    while (keyIterator.hasNext()) {
                        SDNHost host = (SDNHost) keyIterator.next();
                        ArrayList<SDNVm> vms = new ArrayList<SDNVm>(allocMap.get(host));
                        Iterator itr = vms.iterator();
                        boolean success = false;
                        while (itr.hasNext()) {
                            Vm vm_ = (Vm) itr.next();
                            Map<String, Object> vmToHostMap = new HashMap<>();
                            vmToHostMap.put("host", host);
                            vmToHostMap.put("vm", vm_);
                            migrationMap.add(vmToHostMap);
                        }
                    }

                });

  /*
        // New VM to host assignments (Use ACO to find these mappings)
        List<Map<String, Object>> migrationMap = new ArrayList<>();

        vmMigrationList.forEach(vm -> {
            // Recall that each job has multiple tasks and each task has multiple instances. For each instance in a task, a VM was assigned.
            // So getTaskIdOfTheInstanceInVm returns the id of the task, one of whose instances are held by the vm.
            Task vmTask = getTaskIdOfTheInstanceInVm(vm);
            List<SDNVm> connectedVms = vmTask.getScheduledInstances();
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
                NewAntColonyOptimization antColony = new NewAntColonyOptimization(hosts, eligibleHosts, vmToBeMigrated, connectedHosts, this);
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
*/

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
            if (task != null)
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
                totalAllocPes = vmsMappedToHost.stream().mapToInt(Vm::getNumberOfPes).sum() + host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum(); // // because free pes do not properly get computed
            }


            if (host.getStorage() - totalAllocStorage >= mockVm.getSize() && host.getBandwidth() - totalAllocBw >= mockVm.getCurrentRequestedBw()
                    && host.getTotalMips() - totalAllocMips >= mockVm.getTotalMips() && host.getPeList().size() - totalAllocPes >= mockVm.getNumberOfPes()
                    && host.getPeList().get(0).getMips() > mockVm.getMips() /* // each virtual PE of a VM must require not more than the capacity of a physical PE */
                    && host.getRamProvisioner().getRam() - totalAllocRam  >= mockVm.getCurrentRequestedRam()) {
                // Found a host with adequate capacity
                if (vmsMappedToHost == null)
                    vmsMappedToHost = vmList;
                else
                    vmsMappedToHost.addAll(vmList);
                currAllocMap.put(host, vmsMappedToHost);
                result = true;
            }
        }
        return result;
    }

    public boolean allocateHostsForVmEnrealAll(Task task, SDNVm mockVm, List<SDNVm> vmList, List<SDNHost> hosts, Map<SDNHost, List<SDNVm>> currAllocMap) {

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
                totalAllocStorage = vmsMappedToHost.stream().mapToLong(Vm::getSize).sum() + host.getVmList().stream().mapToLong(Vm::getSize).sum();
                totalAllocBw = vmsMappedToHost.stream().mapToLong(Vm::getCurrentRequestedBw).sum() + host.getVmList().stream().mapToLong(Vm::getCurrentRequestedBw).sum();
                totalAllocMips = (long) (vmsMappedToHost.stream().mapToLong(SDNVm::getTotalMips).sum() + host.getVmList().stream().mapToDouble(Vm::getMips).sum());
                totalAllocRam = vmsMappedToHost.stream().mapToInt(Vm::getCurrentRequestedRam).sum() + host.getVmList().stream().mapToInt(Vm::getCurrentRequestedRam).sum();
                totalAllocPes = vmsMappedToHost.stream().mapToInt(Vm::getNumberOfPes).sum() + host.getVmList().stream().mapToInt(Vm::getNumberOfPes).sum();
            }

// Here we only consider the available capacity of PMs since the tasks that are already executing in these PMs will not be considerd for migration....
            if (host.getStorage() - totalAllocStorage >= mockVm.getSize() && host.getBandwidth() - totalAllocBw >= mockVm.getCurrentRequestedBw()
                    && host.getTotalMips() - totalAllocMips >= mockVm.getTotalMips() && host.getPeList().size() - totalAllocPes >= mockVm.getNumberOfPes()
                    && host.getPeList().get(0).getMips() > mockVm.getMips() /* // each virtual PE of a VM must require not more than the capacity of a physical PE */
                    && host.getRam() - totalAllocRam  >= mockVm.getCurrentRequestedRam()) {
                // Found a host with adequate capacity
                if (vmsMappedToHost == null)
                    vmsMappedToHost = vmList;
                else
                    vmsMappedToHost.addAll(vmList);
                currAllocMap.put(host, vmsMappedToHost);
                result = true;
            }

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
    public ArrayList<SDNVm> allocateHostsForTask_COMP(Task task) {
        ArrayList<SDNVm> taskVms = new ArrayList<>();

        task.getPendingInstances().forEach(vm -> {
            // Since normal copying of of arraylists pass by reference
            taskVms.add(vm);
            getTaskVmIdMap().put(task, vm.getId());
        });

        ArrayList<SDNVm> scheduledVms = new ArrayList<>();
        getTaskVmMap().put(task, taskVms);

        ArrayList<SDNHost> connectedHostList = new ArrayList<>();
        ArrayList<Task> predecessorTasks = task.getPredecessorTasks();
        for (int i = 0; i < predecessorTasks.size(); i++) {
            Task predecessor = predecessorTasks.get(i);
            if (predecessor.getMessageVol() == 0)
                continue;
            predecessor.getInstanceHostMap().forEach((instanceVm, host) -> {
                if (connectedHostList.indexOf(host) == -1)
                    connectedHostList.add(host);
            });
        }

        // Allocation of vms to first fit host --- AMANDAAA Use these methods for comparisons and can implement EnREAL as well here itself because all it does is order the PMs in increasing order of Power something and find the first fit one...
        boolean success = true;
        for (SDNVm vm : taskVms) {
            //boolean result = allocateHostForVm(vm);
            //success = allocateHostForVmMipsMostFullFirstEnergyAware(vm);
            //success = allocateHostForVmEnergyAwareGreedy(vm);
            //success = allocateHostForVmMinDED(vm);
            success = herosDAGNet(vm, task, connectedHostList);
            //success = allocateHostForVmFF(vm, task);
            if (success) {
                scheduledVms.add(vm);
            } else {
                System.out.println("unsuccessful allocation");
            }
        }


        task.getScheduledInstances().addAll(scheduledVms);
        task.getPendingInstances().removeAll(scheduledVms);
        return scheduledVms;
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
            getTaskVmIdMap().put(task, vm.getId());
        });

        ArrayList<SDNVm> scheduledVms = new ArrayList<>();
        getTaskVmMap().put(task, taskVms);
        //jobTaskVMMap.put(task.getJob_id(), taskVmMap);

        Multimap<SDNHost, SDNVm> allocMap = null;
        List<SDNHost> hostList = getHostList();
        try {
            ArrayList<SDNHost> connectedHostList = new ArrayList<>();
            ArrayList<Task> predecessorTasks = task.getPredecessorTasks();
            for (int i = 0; i < predecessorTasks.size(); i++) {
                Task predecessor = predecessorTasks.get(i);
                if (predecessor.getMessageVol() == 0)
                    continue;
                predecessor.getInstanceHostMap().forEach((instanceVm, host) -> {
                    if (connectedHostList.indexOf(host) == -1)
                        connectedHostList.add(host);
                });
            }

            //Find an active host
            //if (connectedHostList.size() == 0) {
              //  for (int m = 0; m < hostList.size(); m++) {
                //    if (hostList.get(m).isActive()) {
                  //      connectedHostList.add(hostList.get(m));
                    //    break;
                    //}
                //}
            //}

            //Remove blacklisted hosts for this task
            ArrayList<SDNHost> eligibleHostList = new ArrayList<>();
            for (SDNHost host : hostList) {
                if (task.getBlacklist().indexOf(host) == -1) {
                    eligibleHostList.add(host);
                } else{
                    //Log.printLine(CloudSim.clock() + ": " + "Excluded blacklisted Host: " + host.getId() + " from eligible hostlist of task " + task.getName() + " in job " + task.getJob_id());

                }
            }
            ArrayList<SDNHost> ddd = new ArrayList<>();
            if (connectedHostList.size() > 0)
                ddd.add(connectedHostList.get(0));



            // Single colony ACO ------
            /*
            if (connectedHostList.size() == 0)
                allocMap = recursiveNetworkPartitionNew(taskVms, task, connectedHostList, eligibleHostList); //heuristic(taskVms, task, connectedHostList, eligibleHostList);
            else {
                NewAntColonyOptimization antColony = new NewAntColonyOptimization(hostList, eligibleHostList, taskVms, connectedHostList, this);
                allocMap =  antColony.startAntOptimizationFromPartialState();
            }
*/

            // Network Unaware ACO
/*
            if (connectedHostList.size() == 0)
                allocMap = recursiveNetworkPartitionNew(taskVms, task, connectedHostList, eligibleHostList); //heuristic(taskVms, task, connectedHostList, eligibleHostList);
            else {
                NewAntColonyOptimization antColony = new NewAntColonyOptimization(hostList, eligibleHostList, taskVms, null, this);
                allocMap = antColony.startAntOptimization();
            }
*/

            allocMap = recursiveNetworkPartitionNew(taskVms, task, connectedHostList, eligibleHostList); //heuristic(taskVms, task, connectedHostList, eligibleHostList);
            //allocMap = recursiveNetworkPartition(taskVms, task, connectedHostList, eligibleHostList); //heuristic(taskVms, task, connectedHostList, eligibleHostList);


            //allocMap = heuristic(taskVms, task, connectedHostList, eligibleHostList);

            // Multi colony ACO ------
/*
            if (connectedHostList.size() == 0)
                allocMap = recursiveNetworkPartitionNew(taskVms, task, connectedHostList, eligibleHostList); //heuristic(taskVms, task, connectedHostList, eligibleHostList);
            else {
                ArrayList<Multimap<SDNHost, SDNVm>> allocMapList = new ArrayList<>();
                for (SDNHost host : connectedHostList) {
                    ArrayList<SDNHost> connectedHost = new ArrayList<>();
                    connectedHost.add(host);
                    NewAntColonyOptimization antColony = new NewAntColonyOptimization(hostList, eligibleHostList, taskVms, connectedHost, this);
                    allocMapList.add(antColony.startAntOptimizationFromPartialState());
                }
                // select the best AllocMap
                allocMap = allocMapList.get(0);
                double bestCost = Double.POSITIVE_INFINITY;
                for (Multimap<SDNHost, SDNVm> map : allocMapList) {
                    // take free resource percentage divided by the total no of machines as the cost of soln
                    double cost = costOfSoln(map);
                    if (cost < bestCost) {
                        allocMap = map;
                        bestCost = cost;
                    }
                }
            }
*/

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
                    if (success) {
                        scheduledVms.add((SDNVm) vm);
                        task.getInstanceHostMap().put((SDNVm) vm, (SDNHost) host);
                        if (podList.indexOf(host.getPodId()) == -1) {
                            podList.add(host.getPodId());
                        }
                    }
                    else {
                        task.getBlacklist().add(host);
                        //Log.printLine(CloudSim.clock() + ": " + "Added Host: " + host.getId() + " to blacklisted hostlist of task " + task.getName() + " in job " + task.getJob_id());
                    }
                }
            }
        } catch (Exception e) {
            if (allocMap != null) {
                for (SDNHost host : allocMap.keySet()) {
                    task.getBlacklist().add(host);
                    //Log.printLine(CloudSim.clock() + ": " + "Added Host: " + host.getId() + " to blacklisted hostlist of task " + task.getName() + " in job " + task.getJob_id());
                }
            }
            return null;
        }

        task.getScheduledInstances().addAll(scheduledVms);
        task.getPendingInstances().removeAll(scheduledVms);
        return scheduledVms;
    }

    public ArrayList<SDNHost> getHostsInArea(ArrayList<String> locationIds, ArrayList<SDNHost> eligibleHosts) {
        ArrayList<SDNHost> hostsInCloseProximity = new ArrayList<>();
        for (SDNHost host: eligibleHosts) {
            if (locationIds.indexOf(host.getRackId()) != -1 || locationIds.indexOf(host.getPodId()) != -1) {
                hostsInCloseProximity.add(host);
            }
        }
        return hostsInCloseProximity;
    }

    public Multimap<SDNHost, SDNVm> recursiveNetworkPartition(ArrayList<SDNVm> vmList, Task task, List<SDNHost> currClique, ArrayList<SDNHost> eligibleHosts) {
        Multimap<SDNHost, SDNVm> allocMap = HashMultimap.create();

        if (currClique.size() == 0)
            return heuristic(vmList, task, currClique, eligibleHosts);

        ArrayList<String> locationIds = new ArrayList<>();
        // Add relevant racks
        for (SDNHost host: currClique) {
            locationIds.add(host.getRackId());
        }

        // Add relevant pods
        for (SDNHost host: currClique) {
            locationIds.add(host.getPodId());
        }

        int index = 1;
        while (allocMap.size() != vmList.size()) {
            ArrayList<String> locs = new ArrayList<>(locationIds.subList(0, index));
            ArrayList<SDNHost> candidateHosts = getHostsInArea(locs, eligibleHosts);
            allocMap = heuristic(vmList, task, currClique, candidateHosts);
            index++;
            // This is temporary, fix this.... even racks and then more closeby racks should be tried before going for pods
            if (index > locationIds.size()) {
                allocMap = heuristic(vmList, task, currClique, eligibleHosts);
                break;
            }
        }
        return allocMap;
    }

    public double costOfSoln(Multimap<SDNHost, SDNVm> map) {
        double mipsFreePercent = 0.0;
        double bwFreePercent = 0.0;
        double totalFreePercent = 0.0;
        Set keySet = map.keySet();
        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext()) {
            SDNHost host = (SDNHost) keyIterator.next();
            ArrayList<SDNVm> vms = new ArrayList<SDNVm>(map.get(host));
            mipsFreePercent += (host.getTotalMips() - host.getVmList().stream().mapToDouble(Vm::getMips).sum() - vms.stream().mapToDouble(Vm::getMips).sum()) / getHostTotalMips();
            bwFreePercent += (host.getBandwidth() - host.getVmList().stream().mapToLong(Vm::getCurrentRequestedBw).sum() - vms.stream().mapToDouble(Vm::getMips).sum()) / getHostTotalBw();

        }
        totalFreePercent = convertWeightedMetric(mipsFreePercent, bwFreePercent);
        return totalFreePercent;
    }

    public Multimap<SDNHost, SDNVm> recursiveNetworkPartitionNew(ArrayList<SDNVm> vmList, Task task, List<SDNHost> currClique, ArrayList<SDNHost> eligibleHosts) {
        Multimap<SDNHost, SDNVm> globalAllocMap = HashMultimap.create();

        if (currClique.size() == 0)
            return heuristic(vmList, task, currClique, eligibleHosts);
/*
        if (currClique.size() == 0) {
            // select the servers in utilized pods first....from podlist
            ArrayList<SDNHost> eligibleHostsInUtilizedPods = new ArrayList<>();
            for (SDNHost host : eligibleHosts) {
                if (podList.indexOf(host.getPodId()) != -1) {
                 eligibleHostsInUtilizedPods.add(host);
                }
            }

            globalAllocMap = heuristic(vmList, task, currClique, eligibleHostsInUtilizedPods);
            if (globalAllocMap.size() == vmList.size())
                return globalAllocMap;
            // try all hosts...
            return heuristic(vmList, task, currClique, eligibleHosts);
        }
*/

        ArrayList<String> locationIds = new ArrayList<>();
        // Add relevant racks
        for (SDNHost host: currClique) {
            locationIds.add(host.getRackId());
        }

        // Add relevant pods
        for (SDNHost host: currClique) {
            locationIds.add(host.getPodId());
        }

        // First try active hosts
/*
        int index = 1;
        while (globalAllocMap.size() != vmList.size()) {
            Multimap<SDNHost, SDNVm> localAllocMap = HashMultimap.create();
            ArrayList<String> locs = new ArrayList<>(locationIds.subList(index-1, index));
            ArrayList<SDNHost> candidateHosts = getHostsInArea(locs, eligibleHosts);
            ArrayList<SDNHost> activeCandidateHosts = getActiveHosts(host -> host.isActive() != false, candidateHosts);

            localAllocMap = heuristic(vmList, task, currClique, activeCandidateHosts);
            globalAllocMap.putAll(localAllocMap);

            // Remove vms to which servers have been allocated in localAllocMap and add the corresponding servers to curr clique
            Set<SDNHost> keys = localAllocMap.keySet();
            for (SDNHost host : keys) {
                if (currClique.indexOf(host) == -1)
                    currClique.add(host);
                Collection<SDNVm> vmSet = localAllocMap.get(host);
                for(SDNVm vm : vmSet){
                    vmList.remove(vm);
                }
            }

            index++;
            if (index > locationIds.size()) {
                activeCandidateHosts = getActiveHosts(host -> host.isActive() != false, eligibleHosts);
                localAllocMap = heuristic(vmList, task, currClique, activeCandidateHosts);
                globalAllocMap.putAll(localAllocMap);
                break;
            }
        }
        */

        // set of servers in which predecessors reside or within one hop...
        // Sort servers by the no of hops to curr clique
        // In each iteration give the set of servers further distant .....
        // as input to the assignment function only give the set of servers that were not previously assigned...
        // Give the updated currClique based on allocations that have been made in last iteration....
        // we can use the same method below, but instead of giving locationIds.subList(0, index) need to give locationIds.subList(prev_index, curr_index)

        int index = 1;
        while (globalAllocMap.size() != vmList.size()) {
            Multimap<SDNHost, SDNVm> localAllocMap = HashMultimap.create();
            ArrayList<String> locs = new ArrayList<>(locationIds.subList(index-1, index));
            ArrayList<SDNHost> candidateHosts = getHostsInArea(locs, eligibleHosts);
            localAllocMap = heuristic(vmList, task, currClique, candidateHosts);
            globalAllocMap.putAll(localAllocMap);

            // Remove vms to which servers have been allocated in localAllocMap and add the corresponding servers to curr clique
            Set<SDNHost> keys = localAllocMap.keySet();
            for (SDNHost host : keys) {
                if (currClique.indexOf(host) == -1)
                    currClique.add(host);
                Collection<SDNVm> vmSet = localAllocMap.get(host);
                for(SDNVm vm : vmSet){
                    vmList.remove(vm);
                }
            }

            index++;
            // This is temporary, fix this.... even racks and then more closeby racks should be tried before going for pods
            if (index > locationIds.size()) {
                localAllocMap = heuristic(vmList, task, currClique, eligibleHosts);
                globalAllocMap.putAll(localAllocMap);
                break;
            }
        }
        return globalAllocMap;
    }

    public ArrayList<SDNHost> getActiveHosts(Predicate<SDNHost> coursePredicate, ArrayList<SDNHost> hosts) {
        ArrayList<SDNHost> toReturn = new ArrayList<>();
        for (SDNHost c : hosts.stream().filter(coursePredicate).toArray(SDNHost[]::new)) {
            toReturn.add(c);
        }
        return toReturn;
    }

    public Task getTaskIdOfTheInstanceInVm(SDNVm vm) {
        Set<Task> keys = taskVmIdMap.keySet();
        for (Task task : keys) {
            Collection<Integer> vmIdSet = taskVmIdMap.get(task);
            for(Integer vmId : vmIdSet){
                if (vmId == vm.getId())
                    return task;
            }
        }

        /*
        Iterator<Map.Entry<Task, List<SDNVm>>> iter = taskVmMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Task, List<SDNVm>> entry = iter.next();
            List<SDNVm> vmList = entry.getValue();
            for (SDNVm vmInTask : vmList) {
                if (vmInTask.equals(vm))
                    return entry.getKey();
            }
        }
        */
        return null;
    }
}

