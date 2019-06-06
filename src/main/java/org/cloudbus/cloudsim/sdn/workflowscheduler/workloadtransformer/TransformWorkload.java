package org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer;

public class TransformWorkload {
    /* Read the csv line by line.
    Create a task from each new line.

    1. Read 1st line
    2. Create a task from the content in 1st line
    3. Create a new job and add the task to task_list of the job
    4. Set starttime of job = starttime of the task in 1st line
    5. Read the next line
    6. Create a task from the content in 2nd line
    6. If the job_name of this task is the same as the previous job to which this task belonged, then:
        a. add this task also to the task_list of the previous job
        b. if this tasks starttime < starttime of the job already set: update the job starttime to this tasks starttime
    7. If the job_name of this task is different to the previous job, then:
        a. create a new job
        b. add this task to task_list of new job
        c. set the starttime of the new job to that of this task
    8. Read the next line.................................


Also keep the job_ids in a separate array so later on we can sort it and get the job arriving order
     */
}
