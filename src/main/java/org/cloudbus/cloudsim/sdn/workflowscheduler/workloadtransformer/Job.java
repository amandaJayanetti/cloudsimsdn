package org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer;

import java.util.ArrayList;

public class Job {


    public String getId() {
        return id;
    }

    public String id;
    public int submission_time;
    public ArrayList <Task> tasks;

    public Job(ArrayList<Task> tasks, String id, int submission_time) {
        this.tasks = tasks;
        this.id = id;
        this.submission_time = submission_time;
    }

    public void addTask(Task task) {
        tasks.add(task);
    }
}
