package org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager;

import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer.Task;

import java.util.ArrayList;
import java.util.List;

public class Job {
    public double getStartTime() {
        return startTime;
    }

    public void setStartTime(double startTime) {
        this.startTime = startTime;
    }

    private double startTime;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    private String jobId;

    private long taskId;

    public Job(String jobId, double startTime, List<Task> tasks) {
        setJobId(jobId);
        setStartTime(startTime);
        setTasks(tasks);
        setPendingTasks(tasks);
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }
    private List<Task> tasks = new ArrayList<>();
    private List<Task> scheduledTasks = new ArrayList<>();

    public void setPendingTasks(List<Task> pendingTasks) {
        this.pendingTasks = pendingTasks;
    }

    public List<Task> getPendingTasks() {
        return pendingTasks;
    }

    private List<Task> pendingTasks = new ArrayList<>();

    public void addTaskToScheduledList(Task task) {
        this.scheduledTasks.add(task);
    }

    public void removeFromPendingTaskList(Task task) {
        this.pendingTasks.remove(task);
    }
}
