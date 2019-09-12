package org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer;

import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Task {
    private String name;

    public Map<String, Long> getInputFiles() {
        return inputFiles;
    }

    public void setInputFiles(Map<String, Long> inputFiles) {
        this.inputFiles = inputFiles;
    }

    private Map<String, Long> inputFiles;

    public Map<String, Long> getOutputFiles() {
        return outputFiles;
    }

    public void setOutputFiles(Map<String, Long> outputFiles) {
        this.outputFiles = outputFiles;
    }

    private Map<String, Long> outputFiles;

    public boolean isSubmitted() {
        return submitted;
    }

    public void setSubmitted(boolean submitted) {
        this.submitted = submitted;
    }

    private boolean submitted;

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    private boolean completed;

    public String getJob_id() {
        return job_id;
    }

    private String job_id;
    private long no_instances;

    public long getStart_time() {
        return start_time;
    }

    private long start_time;
    private long end_time;
    private int ram;
    private int pes;
    private long bw;
    private long mips;

    public Map<SDNVm, SDNHost> getInstanceHostMap() {
        return instanceHostMap;
    }

    public void setInstanceHostMap(Map<SDNVm, SDNHost> instanceHostMap) {
        this.instanceHostMap = instanceHostMap;
    }

    private Map<SDNVm, SDNHost> instanceHostMap;

    public long getMessageVol() {
        return messageVol;
    }

    public void setMessageVol(long messageVol) {
        this.messageVol = messageVol;
    }

    private long messageVol;

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    private int clusterId;

    public long getSize() {
        return size;
    }

    public String getType() {
        return type;
    }

    private String type;
    private long size;
    private ArrayList<String> predecessors;

    public ArrayList<SDNHost> getBlacklist() {
        return blacklist;
    }

    public void setBlacklist(ArrayList<SDNHost> blacklist) {
        this.blacklist = blacklist;
    }

    private ArrayList<SDNHost> blacklist;

    public ArrayList<Task> getPredecessorTasks() {
        return predecessorTasks;
    }

    public void setPredecessorTasks(ArrayList<Task> predecessorTasks) {
        this.predecessorTasks = predecessorTasks;
    }

    public void addPredecessorTask(Task predecessorTask) {
        this.predecessorTasks.add(predecessorTask);
    }

    public void addSuccessorTask(Task SuccessorTask) {
        this.successorTasks.add(SuccessorTask);
    }

    private ArrayList<Task> predecessorTasks;

    public ArrayList<String> getSuccessors() {
        return successors;
    }

    public void setSuccessors(ArrayList<String> successors) {
        this.successors = successors;
    }

    private ArrayList<String> successors;

    public ArrayList<Task> getSuccessorTasks() {
        return successorTasks;
    }

    public void setSuccessorTasks(ArrayList<Task> successorTasks) {
        this.successorTasks = successorTasks;
    }

    private ArrayList<Task> successorTasks;
    private List<SDNVm> instances = new ArrayList<SDNVm>();

    public List<SDNVm> getPendingInstances() {
        return pendingInstances;
    }

    public void setPendingInstances(List<SDNVm> pendingInstances) {
        this.pendingInstances = pendingInstances;
    }

    private List<SDNVm> pendingInstances = new ArrayList<SDNVm>();

    public List<SDNVm> getCompletedInstances() {
        return completedInstances;
    }

    public void setCompletedInstances(List<SDNVm> completedInstances) {
        this.completedInstances = completedInstances;
    }

    private List<SDNVm> completedInstances = new ArrayList<SDNVm>();

    public List<SDNVm> getScheduledInstances() {
        return scheduledInstances;
    }

    public void setScheduledInstances(List<SDNVm> scheduledInstances) {
        this.scheduledInstances = scheduledInstances;
    }

    private List<SDNVm> scheduledInstances = new ArrayList<SDNVm>();
    public List<SDNVm> getInstances() {
        return instances;
    }
    public void setInstances(List<SDNVm> instances) {
        this.instances = instances;
    }
    public void addInstance(SDNVm instance) {
        this.instances.add(instance);
    }
    public void addCompletedInstance(SDNVm instance) {
        this.completedInstances.add(instance);
    }

    public Task(String name, String job_id, long no_instances, long start_time, long end_time, double ram, int pes, long bw, long mips, ArrayList<String> predecessors,
                long messageVol) {
        this.name = name;
        this.job_id = job_id;
        this.no_instances = no_instances;
        this.start_time = start_time;
        this.end_time = end_time;
        this.ram = (int)(ram * 100);
        this.pes = pes;
        this.bw = bw;
        this.mips = mips;
        this.predecessors = predecessors;
        this.size = 1000;
        this.type = "vm";
        this.predecessorTasks = new ArrayList<>();
        this.successorTasks = new ArrayList<>();
        this.messageVol = messageVol;
        this.instanceHostMap = new HashMap<>();
        this.completed = false;
        this.submitted = false;
        this.blacklist = new ArrayList<>();
        this.inputFiles = new HashMap<>();
        this.outputFiles = new HashMap<>();
    }

    public void addPredecessor (String job) {
        predecessors.add(job);
    }

    public String getName() {
        return name;
    }

    public long getNo_instances() {
        return no_instances;
    }

    public long getEnd_time() {
        return end_time;
    }

    public int getRam() {
        return ram;
    }

    public int getPes() {
        return pes;
    }

    public long getBw() {
        return bw;
    }

    public long getMips() {
        return mips;
    }

    public ArrayList<String> getPredecessors() {
        return predecessors;
    }
}
