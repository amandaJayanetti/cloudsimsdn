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

    public long getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(long workflowId) {
        this.workflowId = workflowId;
    }

    private long workflowId;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    private long taskId;

    public Task(long taskId, long workflowId, double startTime, double finishTime) {
        setTaskId(taskId);
        setWorkflowId(workflowId);
        setFinishTime(finishTime);
        setStartTime(startTime);
    }

    public List<SDNVm> getJobs() {
        return jobs;
    }

    public void setJobs(List<SDNVm> jobs) {
        this.jobs = jobs;
    }

    private List<SDNVm> jobs = new ArrayList<SDNVm>();
}
