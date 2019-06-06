package org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager;

import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

import java.util.ArrayList;
import java.util.List;

public class Task {
    public double getStartTime() {
        return startTime;
    }

    public void setStartTime(double startTime) {
        this.startTime = startTime;
    }

    private double startTime;

    public double getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(double finishTime) {
        this.finishTime = finishTime;
    }

    private double finishTime;

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    private long jobId;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    private long taskId;

    public Task(long taskId, long jobId, double startTime, double finishTime, List<SDNVm> instances) {
        setTaskId(taskId);
        setJobId(jobId);
        setFinishTime(finishTime);
        setStartTime(startTime);
        setInstances(instances);
    }

    public List<SDNVm> getInstances() {
        return instances;
    }

    public void setInstances(List<SDNVm> instances) {
        this.instances = instances;
    }

    private List<SDNVm> instances = new ArrayList<SDNVm>();
}
