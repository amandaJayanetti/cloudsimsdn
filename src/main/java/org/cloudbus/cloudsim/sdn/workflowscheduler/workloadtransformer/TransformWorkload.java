package org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

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

    public static void main(String[] args) throws IOException {
        //CSVReader reader = new CSVReader(new FileReader("src/main/java/org/cloudbus/cloudsim/sdn/workflowscheduler/workloadtransformer/alibaba.csv"));
        CSVReader reader = new CSVReader(new FileReader("src/main/java/org/cloudbus/cloudsim/sdn/workflowscheduler/workloadtransformer/batch_task_2nd_day_1_hour.csv"));
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("alibaba_data.json");
        FileWriter fileWriter = new FileWriter(file, true);
        fileWriter.write("{ \"jobs\": ");
        SequenceWriter seqWriter = mapper.writerWithDefaultPrettyPrinter().writeValuesAsArray(fileWriter);
        String[] nextLine;
        Job currJob = null;
        int line = 0;
        while ((nextLine = reader.readNext()) != null) {
            try {
                String[] arr = nextLine[0].split("_");
                if (!arr[0].equals("task") && !arr[0].equals("MergeTask")) {
                    ArrayList<String> predecessors = new ArrayList<>();
                    for (int i = 1; i < arr.length; i++) {
                        predecessors.add(arr[i]);
                    }

                    long start = Long.parseLong(nextLine[5]);
                    if (start < 86400 || start > 172800) {
                        System.out.println("WHAAAAAAT");
                        continue;
                    }

                    // volume of data (message size) that flows from a task to it's successors is uniformly chosen between Min_V and Max_V
                    // So if the message volume is zero, that means this task's successors do not have a data dependency....
                    // In the simulation message size doesn't matter much at all because no matter what the size is energy consumption is extremely low for switches,
                    // all that matters is that there's a flow...

                    Random rand = new Random();
                    int volume = rand.nextInt(5);

                    Task task = new Task(arr[0], nextLine[2], Long.parseLong(nextLine[1]), Long.parseLong(nextLine[5]), Long.parseLong(nextLine[6]),
                            Double.parseDouble(nextLine[8]), Integer.parseInt(nextLine[7]), 1000, ((Integer.parseInt(nextLine[6]) - Integer.parseInt(nextLine[5])) * 1000)/Integer.parseInt(nextLine[7]), predecessors,
                            volume);
                    /* (Million Instructions of an instance of the task) /(Million Instructions Per Second Per PE of the machine used by the task) = Time taken for task execution
                        Million Instructions of an instance of the task = Time taken for task execution * (Million Instructions Per Second Per PE of the machine / No of PEs used by the task)

                    In alibaba machine meta data, every machine in the cluster seems to have 96 cores
                    CPU Cores == PE
                    in tasks, 100 cpu == 1 core == 1 PE
                               50 cpu == 0.5 core == 0.5 PE

                    BUT since we can't use fractional values as PEs in our model we have used the no of cpus directly as the PEs. To compensate for this we have used MIPS of machines as 1000 * 100 = 100000
                     */
                    if (currJob != null) {
                        if (!currJob.getId().equals(nextLine[2])) {
                            seqWriter.write(currJob);
                            currJob = new Job(new ArrayList<Task>(), nextLine[2], Integer.parseInt(nextLine[5]));
                            currJob.addTask(task);
                        } else {
                            currJob.addTask(task);
                        }
                    } else {
                        currJob = new Job(new ArrayList<Task>(), nextLine[2], Integer.parseInt(nextLine[5]));
                        currJob.addTask(task);
                    }
                }
            } catch (Exception e) {
                System.out.println("An Exception occurred. " + e.getMessage());
            }
        }
        //seqWriter.close();
        fileWriter.write("]}");
        fileWriter.close();
    }
}
