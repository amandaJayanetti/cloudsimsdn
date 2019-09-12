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
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.sdn.CloudSimTagsSDN;
import org.cloudbus.cloudsim.sdn.CloudletSchedulerSpaceSharedMonitor;
import org.cloudbus.cloudsim.sdn.Configuration;
import org.cloudbus.cloudsim.sdn.nos.NetworkOperatingSystem;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunctionChainPolicy;
import org.cloudbus.cloudsim.sdn.virtualcomponents.FlowConfig;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer.Task;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple network operating system class for the example.
 * In this example, network operating system (aka SDN controller) finds shortest path
 * when deploying the application onto the cloud.
 *
 * @author Jungmin Son
 * @since CloudSimSDN 1.0
 */
public class NetworkOperatingSystemCustom extends NetworkOperatingSystem {

    private static int LAST_SCHEDULED_TIME = 86400; // 0; //86400; //90000;
    private static Multimap<Integer, SDNHost> serverClusters;
    private static Map<SDNVm, SDNHost> occupancyServerVmMap;
    private static ArrayList<SDNVm> vmListForOccupancy = new ArrayList<>();
    private static ArrayList<Job> sortedJobs = new ArrayList<>();
    private static int vmId = 0;

    private void setServerClusters ()
    {
        datacenter.getHostList().forEach(host -> {
            if (host.getPeList().get(0).getMips() == 2000) {
                serverClusters.put(1, (SDNHost)host);
            } else if (host.getPeList().get(0).getMips() == 1000) {
                serverClusters.put(2, (SDNHost)host);
            } else {
                serverClusters.put(3, (SDNHost)host);
            }
        });
    }

    public NetworkOperatingSystemCustom(String name) {
        super(name);
    }

    public NetworkOperatingSystemCustom() {
        super("NOS");
        serverClusters = HashMultimap.create();
    }

    @Override
    protected boolean deployApplication(List<Vm> vms, Collection<FlowConfig> links, List<ServiceFunctionChainPolicy> sfcPolicy) {
        Log.printLine(CloudSim.clock() + ": " + getName() + ": Starting to deploying scientific workflows..");

        //setServerClusters();

        // Sort jobs in ascending order of the start time
        ArrayList<Job> jobs = this.getJobList();
        Collections.sort(jobs, new Comparator<Job>() {
            public int compare(Job o1, Job o2) {
                return (int) (o1.getStartTime() - o2.getStartTime());
            }
        });

        //jobs = new ArrayList<>(jobs.subList(0, 1000));
        deployTasks(jobs);
        sortedJobs = jobs;
       //createDatacenterOccupancy();

        /*
        jobs.sort((task1, task2) -> {
            return (int) (task1.getStartTime() - task2.getStartTime());
        });
*/

/*
        // Sort VMs in ascending order of the start time
        Collections.sort(vms, new Comparator<Vm>() {
            public int compare(Vm o1, Vm o2) {
                return (int) (o2.getMips() * o2.getNumberOfPes() - o1.getMips() * o1.getNumberOfPes());
            }
        });

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

    protected void createDatacenterOccupancy() {
        CloudletScheduler clSch = new CloudletSchedulerSpaceSharedMonitor(Configuration.TIME_OUT);
        for (int i = 0; i < 100; i++) {
            SDNVm vm = new SDNVm(vmId++, 4,500,3,100,100,1000,"VMM",  clSch, 0, 0);
            vmListForOccupancy.add(vm);
        }
        sendNow(datacenter.getId(), CloudSimTagsSDN.SDN_VM_CREATE, vmListForOccupancy);
    }

    @Override
    protected boolean deployTasksInitial() {
        // Select jobs that have start times falling within the current time partition (For now we'll just select 2 jobs to schedule in each iteration)
        /*
            1. Select jobs that fall within the current time partition (Maintain a static current time variable (LAST_SCHEDULED_TIME)
             and append 100s to it to get the end_time. start_time would be the static variable's value
            2. Include the tasks of all the pending tasks of these jobs in the tasks to be scheduled
            3. When scheduling, in processTaskCreate function, if a task cannot be scheduled with the available resources, it'll be queued to schedule later,
             and a smaller task with fewer requirements will be chosen to schedule....
         */

        int END_TIME = LAST_SCHEDULED_TIME + 3;
        List <Task> tasks = new ArrayList<>();
        List <Job> schedulableJobs = new ArrayList<>();
        Iterator<Job> iter = sortedJobs.iterator();
        while (iter.hasNext()) {
            Job job = iter.next();
            // Select jobs that were submitted within the considered time interval
            //if (job.getStartTime() >= LAST_SCHEDULED_TIME && job.getStartTime() < END_TIME) {
            if (job.getStartTime() < END_TIME) {
                schedulableJobs.add(job);
                for (int i = 0; i < job.getPendingTasks().size(); i++) {
                    Task task = job.getPendingTasks().get(i);
                    String name = task.getName();
                    List<Task> predecessors = task.getPredecessorTasks();
                    boolean taskReady = true;
                    for (int k = 0; k < predecessors.size(); k++) {
                        if (!predecessors.get(k).isCompleted()) {
                            taskReady = false;
                            break;
                        }
                    }
                    if (taskReady && !task.isSubmitted()) {
                        tasks.add(task);
                        task.setSubmitted(true);
                    }
                }
            } else {
                // Amandaaa because jobs are sorted in ascending order of start time. no need to iterate rest of the jobs as their start times would be even later....
                break;
            }

            int submittedTasks = (int) job.getTasks().stream().filter(Task::isSubmitted).count();
            if (submittedTasks == job.getTasks().size()) {
                iter.remove();
            }
            // Because there could be jobs with task maps that are partially completed, due to errors when parsing the data set... so give up after like 100 retries...
            job.setRetries(job.getRetries() + 1);
           /*
            if (job.getRetries() > 1000) {
                job.setFailed(true);
                iter.remove();
            }
            */
        }
        LAST_SCHEDULED_TIME = END_TIME;
        if (tasks.size() > 0)
            send(datacenter.getId(), 0.0, CloudSimTagsSDN.TASK_CREATE_ACK, tasks);
        //scheduleTasks(schedulableJobs);

        if (sortedJobs.isEmpty() != true) {
            // This is to call the deployTasks function again for scheduling remaining jobs.... do this after a delay...
            send(this.getId(), 3, CloudSimTagsSDN.SCHEDULE_TASKS, sortedJobs);
        } else {
            // create an event to destroy the vms we used to create some level of data center occupancy...
        }
        return true;
    }

    @Override
    protected boolean deployTasks(List<Job> jobs) {
        // Select jobs that have start times falling within the current time partition (For now we'll just select 2 jobs to schedule in each iteration)
        /*
            1. Select jobs that fall within the current time partition (Maintain a static current time variable (LAST_SCHEDULED_TIME)
             and append 100s to it to get the end_time. start_time would be the static variable's value
            2. Include the tasks of all the pending tasks of these jobs in the tasks to be scheduled
            3. When scheduling, in processTaskCreate function, if a task cannot be scheduled with the available resources, it'll be queued to schedule later,
             and a smaller task with fewer requirements will be chosen to schedule....
         */

        int END_TIME = LAST_SCHEDULED_TIME + 3;
        List <Task> tasks = new ArrayList<>();
        List <Job> schedulableJobs = new ArrayList<>();
        Iterator<Job> iter = jobs.iterator();
        while (iter.hasNext()) {
            Job job = iter.next();
            // Select jobs that were submitted within the considered time interval
            //if (job.getStartTime() >= LAST_SCHEDULED_TIME && job.getStartTime() < END_TIME) {
            if (job.getStartTime() < END_TIME) {
                schedulableJobs.add(job);
                for (int i = 0; i < job.getPendingTasks().size(); i++) {
                    Task task = job.getPendingTasks().get(i);
                    String name = task.getName();
                    List<Task> predecessors = task.getPredecessorTasks();
                    boolean taskReady = true;
                    for (int k = 0; k < predecessors.size(); k++) {
                        if (!predecessors.get(k).isCompleted()) {
                            taskReady = false;
                            break;
                        }
                    }
                    if (taskReady && !task.isSubmitted()) {
                        tasks.add(task);
                        task.setSubmitted(true);
                    }
                }
            } else {
                // Amandaaa because jobs are sorted in ascending order of start time. no need to iterate rest of the jobs as their start times would be even later....
                break;
            }

            int submittedTasks = (int) job.getTasks().stream().filter(Task::isSubmitted).count();
            if (submittedTasks == job.getTasks().size()) {
                iter.remove();
            }
            // Because there could be jobs with task maps that are partially completed, due to errors when parsing the data set... so give up after like 100 retries...
            job.setRetries(job.getRetries() + 1);
           /*
            if (job.getRetries() > 1000) {
                job.setFailed(true);
                iter.remove();
            }
            */
        }
        LAST_SCHEDULED_TIME = END_TIME;
        if (tasks.size() > 0)
            send(datacenter.getId(), 0.0, CloudSimTagsSDN.TASK_CREATE_ACK, tasks);
        //scheduleTasks(schedulableJobs);

        if (jobs.isEmpty() != true) {
            // This is to call the deployTasks function again for scheduling remaining jobs.... do this after a delay...
            send(this.getId(), 3, CloudSimTagsSDN.SCHEDULE_TASKS, jobs);
            //send(this.getId(), 3, CloudSimTagsSDN.SCHEDULE_MIGRATION);
        } else {
            // create an event to destroy the vms we used to create some level of data center occupancy...
        }



    /*
        int MAX_VM_REQ = 100;
        List <Task> tasksToBeScheduled = new ArrayList<>();
        while (tasksToBeScheduled.size() < MAX_VM_REQ && jobs.size() > 0) {
            Task currTask = jobs.get(0).getPendingTasks().get(0);
            Job currJob = jobs.get(0);
            double earliestStartingTime = currTask.getStart_time();

            Iterator<Job> iter = jobs.iterator();
            while (iter.hasNext()) {
                Job job = iter.next();
                for (Task task: job.getPendingTasks()) {
                    if (task.getStart_time() < earliestStartingTime) {
                        earliestStartingTime = task.getStart_time();
                        currTask = task;
                        currJob = job;
                    }
                }
            }
            tasksToBeScheduled.add(currTask);
            currJob.removeFromPendingTaskList(currTask);
            currJob.addTaskToScheduledList(currTask);
            if (currJob.getPendingTasks().size() == 0) {
                jobs.remove(currJob);
            }
        }
        send(datacenter.getId(), 0.0, CloudSimTagsSDN.TASK_CREATE_ACK, tasksToBeScheduled);

        if (jobs.isEmpty() != true) {
            // This is to call the deployTasks function again for scheduling remaining jobs.... do this after a delay...
            send(this.getId(), 10, CloudSimTagsSDN.SCHEDULE_TASKS, jobs);
        }
     */
        return true;
    }

    private Long computeUpwardRank(Task task) {
        ArrayList<Long> successorCostList = new ArrayList<>();
        task.getSuccessorTasks().forEach(succTask -> {
            Long commCost = Long.valueOf(0); //The amount of comm should be given if applicable... so this value would be amount of comm divided by comm rate
            successorCostList.add(commCost + computeUpwardRank(succTask));
        });
        if (successorCostList.size() != 0 )
            return task.getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum() + Collections.max(successorCostList);
        else
            return task.getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum();
    }

    private Long computeDownwardRank(Task task) {
        ArrayList<Long> predCostList = new ArrayList<>();
        task.getPredecessorTasks().forEach(predTask -> {
            Long commCost = Long.valueOf(0); //The amount of comm should be given if applicable... so this value would be amount of comm divided by comm rate
            predCostList.add(commCost + computeDownwardRank(predTask) + predTask.getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum());
        });
        if (predCostList.size() != 0 )
            return Collections.max(predCostList);
        else
            return Long.valueOf(0);
    }

    private Double computeLatestFinishTime(Task task, Map<Task, Double> criticalFinishTimes, int avgServRate) {
        if (criticalFinishTimes.containsKey(task))
            return criticalFinishTimes.get(task);
        ArrayList<Double> latestFinishTime = new ArrayList<>();
        task.getSuccessorTasks().forEach(succTask -> {
            latestFinishTime.add(computeLatestFinishTime(succTask, criticalFinishTimes, avgServRate) - (double)succTask.getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum()/avgServRate + 0);
        });
        return null;
    }

    private void scheduleTasks(List<Job> jobs) {
        jobs.forEach(job -> {
            Map<Task, Long> upwardRank = new HashMap<>();
            Map<Task, Long> downwardRank = new HashMap<>();
            AtomicReference<Long> criticalPath = new AtomicReference<>();
            ArrayList<Task> criticalNodes = new ArrayList<>();
            List <Task> tasks = job.getPendingTasks();
            tasks.forEach(task -> {
                Long upRank = computeUpwardRank(task);
                Long downRank = computeDownwardRank(task);
                if (task.getPredecessorTasks().size() == 0) {
                    criticalPath.set(upRank);
                }
                upwardRank.put(task, upRank);
                downwardRank.put(task, downRank);
            });

            tasks.forEach(task -> {
                if (upwardRank.get(task) + downwardRank.get(task) == criticalPath.get()) {
                    criticalNodes.add(task);
                }
            });

            Map<Integer, Double> finishTimes = new HashMap<>();

            for(Integer key : serverClusters.keySet()) {
                ArrayList<SDNHost> hosts = new ArrayList<SDNHost>((Collection<SDNHost>)serverClusters.get(key));
                int ui = hosts.get(0).getPeList().get(0).getMips(); //service rate of a host
                Double backgroundWorkload = 0.0; // Estimate this!!!!!!!!!!
                Double scheduleLen = 0.0;
                if ((hosts.size() -1) * ui > backgroundWorkload) {
                    for (int i = 0; i < criticalNodes.size() - 1; i++) {
                        Long commCost = Long.valueOf(0); //communication cost between task i and i+1
                        scheduleLen += (double)criticalNodes.get(i).getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum()/ui + commCost;
                    }
                    scheduleLen += (double)criticalNodes.get(tasks.size()-1).getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum()/ui;
                } else {
                    Long W0 = Long.valueOf(0); // Mean waiting time... Estimate this!!!
                    Double len = 0.0;
                    for (int i = 0; i < criticalNodes.size() - 1; i++) {
                        Long commCost = Long.valueOf(0); //communication cost between task i and i+1
                        len += (double)criticalNodes.get(i).getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum()/ui + commCost;
                    }
                    scheduleLen = W0 + ((backgroundWorkload - ((hosts.size() -1) * ui))/(hosts.size()*ui) + 1)*len + (double)criticalNodes.get(tasks.size()-1).getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum()/ui;
                }
                finishTimes.put(key, scheduleLen);
            }


            // We have the schedule length of the job in each cluster. So we can define a metric for each cluster....
            // based on power consumption and schedule length ratios and utilization as well
            // prefer clusters with low power consumption, low schedule lengths and high utilizations .....
            // Assign the job to cluster with the highest metric val
            // Can incorporate with a user defined deadlines, where algorithm determines the most energy efficient cluster which can meet the deadline....

            // In a heterogeneous datacenter with clusters of homogeneous servers,
            // scattering the tasks of a job among multiple clusters could be detrimental with respect to data locality and communication considerations.....

                double maxFinishTime = Collections.max(finishTimes.values());
                double minFinishTime = Collections.min(finishTimes.values());
                double alpha = 0.2;
                double beta = 0.8;
                double accScheduleLen = alpha * minFinishTime + beta * maxFinishTime;

                // Calculating longest finish times of critical tasks assuming a cluster with AVERAGE service rate and AVERAGE no of servers
                Map <Task, Double> finishTimeMap = new HashMap<>();
                AtomicReference<Double> accuLen = new AtomicReference<>(0.0);
                criticalNodes.forEach(node -> {
                    Long commCost = Long.valueOf(0); //communication cost between task i and i+1
                   // accuLen.updateAndGet(v -> v + (double) node.getPendingInstances().stream().mapToLong(SDNVm::getTotalMips).sum() / ui + commCost);
                });

        });

        // For each job that has been submitted during the considered time period
        //      Calculate the rank of each task of the job (HEFT)
        //      Find the tasks on the critical path
        //      For each cluster
        //          Using the reference algorithm find the schedule length (SL) of the job in the cluster Map<<ClusterId,jobId>,finishTime>>
        //      MaxSL <- Max(Finish times in different clusters)
        //      MinSL <- Min(Finish times in different clusters)
        //
        //      Longest acceptable schedule length (SL_ACC) = alpha * MaxSL + beta * MinSL (alpha and beta can be experimentally determined,
        // or a cost function can be defined and thereby allow users to set the alpha and beta values, and accordingly charge them)
        //      Latest finish time of sink task = curr time + SL_ACC
        //      Find the latest finish time of all the tasks using eqn in reference paper
        //      For each task
        //          For each cluster
        //              M-cluster = (w1 * total cluster utilization ratio) * (w2 * free bandwidth ratio) * (w3 * 1/(distance to servers from parent tasks))
        //          Assign task to the most energy efficient cluster (one with the maxPPW) that has the highest M-cluster val.
    }

    @Override
    public void processVmCreateAck(SimEvent ev) {
        super.processVmCreateAck(ev);

        // print the created VM info
        SDNVm vm = (SDNVm) ev.getData();
        Log.printLine(CloudSim.clock() + ": " + getName() + ": VM Created: " + vm + " in " + vm.getHost());
        deployFlow(this.flowMapVmId2Flow.values());

        // AMANDAAAA submit cloudlets here??? Just send a CLOUDLET_SUBMIT to datacenter with Cloudlet info as data..... see org/cloudbus/cloudsim/sdn/physicalcomponents/SDNDatacenter.java:556
        // Cloudlet properties
        // In our model Cloudlet maps directly to VM. I.e so the length of a cloudlet is the same as mips * pes of the corresponding VM.


        // If there are different host types --- check the host category and accordingly determine how long a task will take to complete on the host and assign length accordingly\
        long length = vm.getTotalMips(); //mips is actually based on endtime-starttime of task
        long fileSize = 300;
        long outputSize = 300;
        int pesNumber = 1; // number of cpus
        UtilizationModel utilizationModel = new UtilizationModelFull();
        Cloudlet cloudlet = new Cloudlet(vm.getId(), length, vm.getNumberOfPes(), fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
        cloudlet.setVmId(vm.getId());
        cloudlet.setUserId(vm.getUserId());
        sendNow(ev.getSource(), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
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

    public Task getTask(SDNVm sdnvm) {
        AtomicReference<Task> vmTask = null;
        ArrayList<Job> jobs = this.getJobList();
        jobs.forEach(job -> {
            List<Task> tasks = job.getTasks();
            tasks.forEach(task -> {
                List <SDNVm> vms = task.getInstances();
                vms.forEach(vm -> {
                    if (vm.getId() == sdnvm.getId())
                        vmTask.set(task);
                        }
                );
            });
        });
        return vmTask.get();
    }

}
