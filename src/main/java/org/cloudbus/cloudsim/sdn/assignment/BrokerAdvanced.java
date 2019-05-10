package org.cloudbus.cloudsim.sdn.assignment;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import java.util.*;

public class BrokerAdvanced extends DatacenterBroker {

    // VM Category List
    private List<Vm> vmCategoryList = new ArrayList<>();

    /**
     * Contains a Map of vm-ids as keys with cloudlets that should be assigned to them in the linkedlist
     */
    private Map <Integer, LinkedList<Cloudlet>> vmCloudletQueue = new HashMap<>();

    /**
     * A list of cloudlets with the resource requirements. Contains elements of the form: [ cloudletId, {RAM, Storage} ]
     * ToDo double check if it's not possible to get these from created cloudlet instances without maintaining an additional list
     */
    private Map<Integer, Vector<Double>> cloudletSpecList = new HashMap<>();

    public void setVmCategoryList(List<Vm> vmCategoryList) {
        this.vmCategoryList = vmCategoryList;
    }

    public List<Vm> getVmCategoryList() {
        return vmCategoryList;
    }

    public Map<Integer, LinkedList<Cloudlet>> getVmCloudletQueue() {
        return vmCloudletQueue;
    }

    public void setVmCloudletQueue(Map<Integer, LinkedList<Cloudlet>> vmCloudletQueue) {
        this.vmCloudletQueue = vmCloudletQueue;
    }

    public Map<Integer, Vector<Double>> getCloudletSpecList() {
        return cloudletSpecList;
    }

    public void setCloudletSpecList(Map<Integer, Vector<Double>> cloudletSpecList) {
        this.cloudletSpecList = cloudletSpecList;
    }


    /**
     * Created a new AdvancedBroker object.
     *
     * @param name name to be associated with this entity
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public BrokerAdvanced(String name) throws Exception {
        super(name);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            // A finished cloudlet returned
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            // if cloudlets are still pending on a vm
            case CloudSimTags.CLOUDLET_PENDING_SUBMIT:
                submitCloudlets();
                break;
            // if the simulation finishes
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    @Override
    protected void processResourceCharacteristics(SimEvent ev)
    {
        DatacenterCharacteristics characteristics = (DatacenterCharacteristics) ev.getData();
        getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

        if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size())
        {
            createVMs(getDatacenterIdsList().get(0));
        }
    }

    /**
     * Distributes the VMs across the data centers using the round-robin approach. A VM is allocated to a data center only if there isn't
     * a VM in the data center with the same id.
     */
    protected void createVMs(int datacenterId)
    {
        int numberOfVmsAllocated = 0;

        for (Vm vm : getVmList())
        {
            String datacenterName = CloudSim.getEntityName(datacenterId);

            if (!getVmsToDatacentersMap().containsKey(vm.getId()))
            {
                Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId() + " in " + datacenterName);
                sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
                numberOfVmsAllocated++;
            }
        }

        getDatacenterRequestedIdsList().add(datacenterId);
        setVmsRequested(numberOfVmsAllocated);
        setVmsAcks(0);
    }

    /**
     * Submit cloudlets to the created VMs.
     *
     * @pre $none
     * @post $none
     * @see #submitCloudletList(List)
     */
    @Override
    protected void submitCloudlets() {
        int vmIndex = 0;
        List<Cloudlet> successfullySubmitted = new ArrayList<Cloudlet>();
        for (Cloudlet cloudlet : getCloudletList()) {
            Vm vm = null;
            // if user didn't bind this cloudlet and it has not been executed yet
            if (cloudlet.getVmId() == -1) {
                //vm = getVmsCreatedList().get(vmIndex);
                Vector<Double> compatibleVMSpecs = findCompatibleVMCategory(cloudlet);
                Set<Integer> compatibleVMIds = getCompatibleVMIds(compatibleVMSpecs);
                Iterator<Integer> iterator = compatibleVMIds.iterator();
                while (iterator.hasNext()) {
                    Integer vmId = iterator.next();
                    // check if the VM is free, if so assign the cloudlet to VM
                    // A cloudlet can be assigned to VM if there are free cores (pes) in the VM.
                    // The condition getVmsCreatedList().get(vmId).getCloudletScheduler().getCloudletExecList().size() gives the no of cloudlets assigned to this VM for execution.
                    // ToDo When debugging check if you need to manually update the scheduler info
                    if (getVmsCreatedList().get(vmId).getNumberOfPes() > getVmsCreatedList().get(vmId).getCloudletScheduler().getCloudletExecList().size()) {
                        vm = getVmsCreatedList().get(vmId);
                        getVmsCreatedList().get(vmId).getCloudletScheduler().getCloudletExecList().add(new ResCloudlet(cloudlet));
                        if (!Log.isDisabled()) {
                            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": +++++ Sending cloudlet ",
                                    cloudlet.getCloudletId(), " to VM #", vm.getId());
                        }
                        cloudlet.setVmId(vm.getId());
                        sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                        cloudletsSubmitted++;
                        vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
                        getCloudletSubmittedList().add(cloudlet);
                        successfullySubmitted.add(cloudlet);
                        break;
                    } else {
                        Vm tempVm = getVmsCreatedList().get(vmId);
                        // If tasks are there in VM queue evaluate if this task can be inserted into the queue so that it's deadline won't be missed.
                        long collectiveExecutionOfPendingTasks = 0;
                        LinkedList<Cloudlet> tempCloudletList = new LinkedList<>();
                        if (!(getVmCloudletQueue().get(vmId) == null)) {
                            tempCloudletList = getVmCloudletQueue().get(vmId);
                            Iterator<Cloudlet> cloudletItr = tempCloudletList.iterator();
                            while (cloudletItr.hasNext()) {
                                collectiveExecutionOfPendingTasks = +Math.floorDiv(cloudletItr.next().getCloudletLength(), (long) tempVm.getMips());
                            }
                            double expectedTimeOfCompletion = collectiveExecutionOfPendingTasks + Math.floorDiv(cloudlet.getCloudletLength(), (long) tempVm.getMips());

                            if (getCloudletSpecList().get(cloudlet.getCloudletId()).get(2) < expectedTimeOfCompletion) {
                                continue;
                            }
                        }
                        // ToDo improve to place the task by sorting the deadlines of queued tasks and reordering the queue accordingly.
                        //  Maybe by reordering the tasks it would be possible to place a task in the queue of this VM instead of moving it to a different one.
                        vm = tempVm;
                        tempCloudletList.add(cloudlet);
                        getVmCloudletQueue().put(vmId, tempCloudletList);
                        cloudlet.setVmId(vmId);
                        if (!Log.isDisabled()) {
                            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Adding cloudlet ",
                                    cloudlet.getCloudletId(), " to the waiting queue of VM #", vm.getId());
                        }
                        break;
                    }
                }
                if (vm == null) {
                    // It wasn't possible to queue or assign this cloudlet to any of the existing VMs. Hence create a new VM and assign this. Will have to generate two events.
                    // One event for VM creation, and another for cloudlet submission.
                    // Wasn't able to find any such VM creation action in the middle of execution in existing codes.
                    if (!Log.isDisabled()) {
                        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Unable to find a compatible VM for cloudlet ",
                                cloudlet.getCloudletId());
                    }
                }
            } else { // submit to the specific vm
                vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
                if (vm == null) { // vm was not created
                    if (!Log.isDisabled()) {
                        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Postponing execution of cloudlet ",
                                cloudlet.getCloudletId(), ": bount VM not available");
                    }
                    continue;
                }

                if (!Log.isDisabled()) {
                    Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": *********************** Sending cloudlet ",
                            cloudlet.getCloudletId(), " to VM #", vm.getId());
                }

                // Remove this cloudlet from the queue of corresponding Vm
                LinkedList<Cloudlet> tempCloudletList = getVmCloudletQueue().get(vm.getId());
                if (tempCloudletList.contains(cloudlet)) {
                    tempCloudletList.remove(cloudlet);
                    getVmCloudletQueue().put(vm.getId(), tempCloudletList);
                }

                sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                cloudletsSubmitted++;
                vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
                getCloudletSubmittedList().add(cloudlet);
                successfullySubmitted.add(cloudlet);
            }
        }
        // remove submitted cloudlets from waiting list
        getCloudletList().removeAll(successfullySubmitted);

        // Generate events for cloudlets that were queued on VMs (i.e. ones that were not successfully submitted).
        // Note that the successfully submitted cloudlets are removed from cloudletList so getCloudletList() returns pending cloudlets only.
        for (Cloudlet cloudlet : getCloudletList()) {
            // If a cloudlet is added to the queue of a particular VM, then send an event to itself with the details of vmId and cloudletId,
            // and generate a NEW type of event (CLOUDLET_PENDING_SUBMIT) for this.
            sendNow(this.getId(), CloudSimTags.CLOUDLET_PENDING_SUBMIT, cloudlet);
        }

        Iterator<Vm> iterator = getVmsCreatedList().iterator();
        while (iterator.hasNext()) {
            // NOTE This step is required since CloudletScheduler has it's own way of including and excluding cloudlets to the execution list of VMs.
            // So the ones that we added to this list above in this function need to be removed to prevent the duplication of cloudlets in the execution lists of VMs.
            Vm vm = iterator.next();
            List<ResCloudlet> resCloudlets = vm.getCloudletScheduler().getCloudletExecList();
            vm.getCloudletScheduler().getCloudletExecList().removeAll(resCloudlets);
        }
    }

    private double computeEuclideanDistance(Vector sequence1, Vector sequence2) {
        double sum = 0.0;
        for (int index = 0; index < sequence1.size(); index++) {
            sum = sum + Math.pow((Double)sequence1.get(index) - (Double)sequence2.get(index), 2.0);
        }

        return Math.pow(sum, 0.5);
    }

    private Vector<Double> findCompatibleVMCategory(Cloudlet cloudlet) {
        Vector<Double> matchingVmSpecs = new Vector<>();
        Vector<Double> cloudletSpec = getCloudletSpecList().get(cloudlet.getCloudletId());
        Iterator<Vm> vmCategoryIter = getVmCategoryList().iterator();
        Double minEucledianDist = 0.0;
        while (vmCategoryIter.hasNext()) {
            Vm vm = vmCategoryIter.next();
            if (vm.getRam() < cloudletSpec.get(0) || vm.getSize() < cloudletSpec.get(1)) {
                // Skip VMs that may result in giving lower euclidean distance but actually consists of inadequate resources
                continue;
            }
            Vector<Double> vmCategorySpecs = new Vector<>();
            vmCategorySpecs.add( new Double(vm.getRam()));
            vmCategorySpecs.add( new Double(vm.getSize()));
            double euclDist = computeEuclideanDistance(vmCategorySpecs, cloudletSpec);
            if (minEucledianDist == 0.0) {
                minEucledianDist = euclDist;
                matchingVmSpecs = vmCategorySpecs;
            }
            if (euclDist < minEucledianDist) {
                minEucledianDist = euclDist;
                matchingVmSpecs = vmCategorySpecs;
            }
        }
        return  matchingVmSpecs;
    }


    // Returns a list of VMs with given specifications
    public Set<Integer> getCompatibleVMIds(Vector<Double> specs) {
        Set<Integer> keys = new HashSet<Integer>();
        Iterator<Vm> vmIterator = getVmsCreatedList().iterator();
        while (vmIterator.hasNext()) {
            Vm vm = vmIterator.next();
            if ((double)vm.getRam() == specs.get(0) && (double)vm.getSize() == specs.get(1)) {
                keys.add(vm.getId());
            }
        }
        return keys;
    }
}
