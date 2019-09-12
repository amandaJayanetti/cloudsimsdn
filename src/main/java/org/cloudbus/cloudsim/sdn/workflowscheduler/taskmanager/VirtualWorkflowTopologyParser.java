/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2017, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.dist.dist.PoissonDistr;
import org.cloudbus.cloudsim.sdn.CloudletSchedulerSpaceSharedMonitor;
import org.cloudbus.cloudsim.sdn.Configuration;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunction;
import org.cloudbus.cloudsim.sdn.sfc.ServiceFunctionChainPolicy;
import org.cloudbus.cloudsim.sdn.virtualcomponents.FlowConfig;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer.Task;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import static java.lang.Math.log;
import static java.lang.Math.round;

/**
 * This class parses Virtual Topology (VMs, Network flows between VMs, and SFCs).
 * It loads Virtual Topology JSON file and creates relevant objects in the simulation.
 *
 * @author Jungmin Son
 * @since CloudSimSDN 1.0
 */
public class VirtualWorkflowTopologyParser {

    private static int flowNumbers = 0;

    // task_id and vm_name pairs
    private Multimap<String, String> taskVmMap = HashMultimap.create();

    public ArrayList<Job> getJobList() {
        return jobList;
    }

    // task_id list
    private ArrayList<Job> jobList = new ArrayList<>();
    private Multimap<String, SDNVm> vmList;
    private List<ServiceFunction> sfList = new LinkedList<ServiceFunction>(); // SFs are added in both VM list and SF list
    private List<FlowConfig> arcList = new LinkedList<FlowConfig>();
    private List<ServiceFunctionChainPolicy> policyList = new LinkedList<ServiceFunctionChainPolicy>();

    private String vmsFileName;
    private int userId;

    private String defaultDatacenter;

    public VirtualWorkflowTopologyParser(String datacenterName, String topologyFileName, int userId) {
        vmList = HashMultimap.create();
        this.vmsFileName = topologyFileName;
        this.userId = userId;
        this.defaultDatacenter = datacenterName;

        // AMANDAAAA
        //parse();
        //parseXml();

        try {
            parseJobs(topologyFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void parse() {

        try {
            JSONObject doc = (JSONObject) JSONValue.parse(new FileReader(vmsFileName));
            Hashtable<String, Integer> vmNameIdTable = parseTasks(doc);
            Hashtable<String, Integer> flowNameIdTable = parseLinks(doc, vmNameIdTable);
            parseSFCPolicies(doc, vmNameIdTable, flowNameIdTable);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void parseXml() {
        try {
            File inputFile = new File(vmsFileName);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();

            Hashtable<String, Integer> vmNameIdTable = parseTasks(doc);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Hashtable<String, Integer> parseTasks(Document doc) {
        Hashtable<String, Integer> vmNameIdTable = new Hashtable<String, Integer>();

        Job job;
        String jobId = "Montage--";
        ArrayList<Task> tasks = new ArrayList<>();
        NodeList parentNodeList = doc.getElementsByTagName("job");
        NodeList childNodeList = doc.getElementsByTagName("child");

        // Job Iteration
        for (int i = 0; i < parentNodeList.getLength(); i++) {
            Node parentNode = parentNodeList.item(i);
            Task task = null;

            if (parentNode.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) parentNode;
                String name = element.getAttribute("id");
                int cores = 1;
                if (element.getAttribute("cores") != "")
                    cores = Integer.parseInt(element.getAttribute("cores"));
                double mips_ = Double.parseDouble(element.getAttribute("runtime"));
                long mips = (long)mips_; // 130; // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11
                ArrayList<String> predecessors = new ArrayList<>();
                if (mips > 2000)
                    mips = 1990;

                task = new Task(name, jobId, 1, 0, 0, 100, cores, 1000, mips, predecessors, 1);

                for (int n = 0; n < 1; n++) {
                    String nodeName2 = name;

                    CloudletScheduler clSch = new CloudletSchedulerSpaceSharedMonitor(Configuration.TIME_OUT);
                    //CloudletScheduler clSch = new CloudletSchedulerTimeSharedMonitor(mips);
                    int vmId = SDNVm.getUniqueVmId();
                    // Create VM objects
                    SDNVm vm = new SDNVm(vmId, userId, mips, cores, 100, 1000, 1000, "VMM", clSch, 0, 0);
                    //SDNVm vm = new SDNVm(vmId, userId, 2000, 2, 512, 100000000, 1000, "VMM", clSch, starttime, endtime);
                    vm.setName(nodeName2);
                    vmList.put(this.defaultDatacenter, vm);
                    task.addInstance(vm);
                    task.getPendingInstances().add(vm);

                    vmNameIdTable.put(nodeName2, vmId);
                }

                NodeList fileList = element.getElementsByTagName("uses");
                Map<String, Long> inFiles = new HashMap<>();
                Map<String, Long> outFiles = new HashMap<>();

                for (int k = 0; k < fileList.getLength(); k++) {
                    Node childNode = fileList.item(k);
                    Element fileEle = (Element) childNode;
                    String fileName = fileEle.getAttribute("file");
                    String link = fileEle.getAttribute("link");
                    Long size = Long.parseLong(fileEle.getAttribute("size"));
                    if (link.equals("input"))
                        inFiles.put(fileName, size);
                    else if (link.equals("output"))
                        outFiles.put(fileName, size);
                    task.setInputFiles(inFiles);
                    task.setOutputFiles(outFiles);
                }

            }
            if (task != null)
                tasks.add(task);
        }


        for (int i = 0; i < childNodeList.getLength(); i++) {
            Node childNode = childNodeList.item(i);
            if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) childNode;
                String name = element.getAttribute("ref");

                Task childTask = null;
                for (Task task : tasks) {
                    if (task.getName().equals(name)) {
                        childTask = task;
                    }
                }

                NodeList predecessorList = childNode.getChildNodes();
                for (int k = 0; k < predecessorList.getLength(); k++) {
                    if (predecessorList.item(k).getNodeType() == Node.ELEMENT_NODE) {
                        Element parentEle = (Element) predecessorList.item(k);
                        String parentName = parentEle.getAttribute("ref");
                        for (Task task : tasks) {
                            if (task.getName().equals(parentName)) {
                                childTask.getPredecessorTasks().add(task);
                                task.getSuccessorTasks().add(childTask);
                            }
                        }
                    }
                }
            }
        }

        job = new Job(jobId, 0, tasks);
        jobList.add(job);
        return vmNameIdTable;
    }

    private Hashtable<String, Integer> parseJobs(String folderName) throws IOException, SAXException, ParserConfigurationException {
        Hashtable<String, Integer> vmNameIdTable = new Hashtable<String, Integer>();

        PoissonDistr poisson = null;
        final long seed=System.currentTimeMillis();
        poisson = new PoissonDistr(0.4, seed);
        //customersInAllSimulations += runSimulation.apply(poisson);

        ArrayList<Integer> startTimes = PoissonDistr.getEventTimes(poisson, 3600);

        int idIterator = 0;
        String jobId = "job_" + idIterator;
        File path = new File(folderName);
        File [] files = path.listFiles();
        int allIns = 0;
        for (int p = 0; p < files.length; p++) {
            File inputFile = files[p];
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();
            for (int q = 0; q < 1; q++) {
            Job job;
            idIterator++;
            ArrayList<Task> tasks = new ArrayList<>();
            NodeList parentNodeList = doc.getElementsByTagName("job");
            NodeList childNodeList = doc.getElementsByTagName("child");

            // Job Iteration
            for (int i = 0; i < parentNodeList.getLength(); i++) {
                    Node parentNode = parentNodeList.item(i);
                    Task task = null;

                    if (parentNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element element = (Element) parentNode;
                        String name = element.getAttribute("id");
                        int cores = 1;
                        if (element.getAttribute("cores") != "")
                            cores = Integer.parseInt(element.getAttribute("cores"));
                        double mips_ = Double.parseDouble(element.getAttribute("runtime"));
                        long mips = (long) mips_; // 130; // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11
                        ArrayList<String> predecessors = new ArrayList<>();
                        if (mips > 2000)
                            mips = 1990;

                        task = new Task(name, jobId, 1, 0, 0, 100, cores, 1000, mips, predecessors, 1);

                        for (int n = 0; n < 1; n++) {
                            allIns++;
                            String nodeName2 = name;

                            CloudletScheduler clSch = new CloudletSchedulerSpaceSharedMonitor(Configuration.TIME_OUT);
                            //CloudletScheduler clSch = new CloudletSchedulerTimeSharedMonitor(mips);
                            int vmId = SDNVm.getUniqueVmId();
                            // Create VM objects
                            SDNVm vm = new SDNVm(vmId, userId, mips, cores, 100, 1000, 1000, "VMM", clSch, 0, 0);
                            //SDNVm vm = new SDNVm(vmId, userId, 2000, 2, 512, 100000000, 1000, "VMM", clSch, starttime, endtime);
                            vm.setName(nodeName2);
                            vmList.put(this.defaultDatacenter, vm);
                            task.addInstance(vm);
                            task.getPendingInstances().add(vm);

                            vmNameIdTable.put(nodeName2, vmId);
                        }

                        NodeList fileList = element.getElementsByTagName("uses");
                        Map<String, Long> inFiles = new HashMap<>();
                        Map<String, Long> outFiles = new HashMap<>();

                        for (int k = 0; k < fileList.getLength(); k++) {
                            Node childNode = fileList.item(k);
                            Element fileEle = (Element) childNode;
                            String fileName = fileEle.getAttribute("file");
                            String link = fileEle.getAttribute("link");
                            Long size = Long.parseLong(fileEle.getAttribute("size"));
                            if (link.equals("input"))
                                inFiles.put(fileName, size);
                            else if (link.equals("output"))
                                outFiles.put(fileName, size);
                            task.setInputFiles(inFiles);
                            task.setOutputFiles(outFiles);
                        }

                    }
                    if (task != null)
                        tasks.add(task);
                }


                for (int i = 0; i < childNodeList.getLength(); i++) {
                    Node childNode = childNodeList.item(i);
                    if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element element = (Element) childNode;
                        String name = element.getAttribute("ref");

                        Task childTask = null;
                        for (Task task : tasks) {
                            if (task.getName().equals(name)) {
                                childTask = task;
                            }
                        }

                        NodeList predecessorList = childNode.getChildNodes();
                        for (int k = 0; k < predecessorList.getLength(); k++) {
                            if (predecessorList.item(k).getNodeType() == Node.ELEMENT_NODE) {
                                Element parentEle = (Element) predecessorList.item(k);
                                String parentName = parentEle.getAttribute("ref");
                                for (Task task : tasks) {
                                    if (task.getName().equals(parentName)) {
                                        childTask.getPredecessorTasks().add(task);
                                        task.getSuccessorTasks().add(childTask);
                                    }
                                }
                            }
                        }
                    }
                }

                job = new Job(jobId, startTimes.get(idIterator), tasks);
                startTimes.remove(idIterator);
                jobList.add(job);
            }
        }
        Collections.shuffle(jobList);
        return vmNameIdTable;
    }

    private Hashtable<String, Integer> parseTasks(JSONObject doc) {
        // vmNameIdTable will have all the VMs from all tasks
        Hashtable<String, Integer> vmNameIdTable = new Hashtable<String, Integer>();
        // Parse tasks
        JSONArray jobs = (JSONArray) doc.get("jobs");

        int noInsAll = 0;
        int no_jobs = 0;
        @SuppressWarnings("unchecked")
        Iterator<JSONObject> iter = jobs.iterator();
        while (iter.hasNext()) {
            // To control the experiment...
            no_jobs ++;
            //if (no_jobs > 500)
              //  break;

            JSONObject job = iter.next();

            JSONArray tasks = (JSONArray) job.get("tasks");
            List<Task> taskList = new ArrayList<>();

            @SuppressWarnings("unchecked")
            Iterator<JSONObject> nodeItr = tasks.iterator();
            while (nodeItr.hasNext()) {
                JSONObject task = nodeItr.next();

                String nodeType = (String) task.get("type");
                String nodeName = (String) task.get("name");
                int pes = new BigDecimal((Long) task.get("pes")).intValueExact()/100;
                if (pes == 0) {
                    pes = 1;
                 }
                long mips = (Long) task.get("mips");
                if (mips > 1999)
                    mips = 1999; // AMANDAAA Since the maximum per PE that servers currently used can accomodate is 1500
                else if (mips == 0) {
                    continue;
                }
                int ram = new BigDecimal((Long) task.get("ram")).intValueExact();
                long size = (Long) task.get("size");
                long bw = 0;
                Long no_instances = (Long) task.get("no_instances");

                List<SDNVm> taskVms = new ArrayList<>();

                // Add task to taskVmMap
                taskVmMap.put(nodeName + ":" + (String) task.get("job_id"), nodeName);

                if (task.get("bw") != null)
                    bw = (Long) task.get("bw");

                long starttime = 0;
                long endtime = 0;
                if (task.get("start_time") != null)
                    starttime = (Long) task.get("start_time");
                if (task.get("end_time") != null)
                    endtime = (Long) task.get("end_time");

                String dcName = this.defaultDatacenter;
                if (task.get("datacenter") != null)
                    dcName = (String) task.get("datacenter");

                // Optional datacenter specifies the alternative data center if 'data center' has no more resource.
                ArrayList<String> optionalDatacenter = null;
                if (task.get("subdatacenters") != null) {
                    optionalDatacenter = new ArrayList<>();
                    JSONArray subDCs = (JSONArray) task.get("subdatacenters");

                    for (int i = 0; i < subDCs.size(); i++) {
                        String subdc = subDCs.get(i).toString();
                        optionalDatacenter.add(subdc);
                    }
                }


                String hostName = "";
                if (task.get("host") != null)
                    hostName = (String) task.get("host");

                long instances = 1;
                if (task.get("no_instances") != null)
                    instances = (Long) task.get("no_instances");

                long msgVol = 0;
                if (task.get("messageVol") != null)
                    msgVol = (Long) task.get("messageVol");

                /*
                All instances within a task execute exactly the same binary with the same resource request, but with different input data
                Note: Instances execute the same binary --> each instance is like a thread???
                So all instances need to be scheduled on the same VM??
                It also says:  Instance is the smallest scheduling unit of batch workload.
                Maybe this means scheduling cores of the same VM to which the task is assigned!
                 */



                ArrayList<String> taskPredecessors = new ArrayList<>();
                JSONArray predecessors = (JSONArray) task.get("predecessors");
                for (int i = 0; i < predecessors.size(); i++) {
                    String predecessor = predecessors.get(i).toString();
                    taskPredecessors.add(predecessor);
                }

                //instances = 3;

                Random rand = new Random();
                int r = rand.nextInt(100);
                int m = r % 10;
                if (m < 7)
                    msgVol = 0; // 10 %
                else
                    msgVol = 1; // 90 %


                Task newTask = new Task(nodeName, (String) job.get("id"), instances, starttime, endtime, ram, pes, bw, mips, taskPredecessors, msgVol);

                for (int n = 0; n < instances; n++) {
                    noInsAll++;
                    String nodeName2 = (String) job.get("id") + ":" + nodeName;
                    if (instances > 1) {
                        // Nodename should be numbered.
                        nodeName2 = (String) job.get("id") + ":" + nodeName + ":" + n;
                    }

                    CloudletScheduler clSch = new CloudletSchedulerSpaceSharedMonitor(Configuration.TIME_OUT);
                    //CloudletScheduler clSch = new CloudletSchedulerTimeSharedMonitor(mips);
                    int vmId = SDNVm.getUniqueVmId();

                    if (nodeType.equalsIgnoreCase("vm")) {
                        // Create VM objects
                        SDNVm vm = new SDNVm(vmId, userId, mips, pes, ram, bw, size, "VMM", clSch, starttime, endtime);
                        //SDNVm vm = new SDNVm(vmId, userId, 2000, 2, 512, 100000000, 1000, "VMM", clSch, starttime, endtime);
                        vm.setName(nodeName2);
                        vm.setHostName(hostName);
                        vm.setOptionalDatacenters(optionalDatacenter);
                        vmList.put(dcName, vm);
                        newTask.addInstance(vm);
                        newTask.getPendingInstances().add(vm);
                    } else {
                        // Create ServiceFunction objects
                        ServiceFunction sf = new ServiceFunction(vmId, userId, mips, pes, ram, bw, size, "VMM", clSch, starttime, endtime);
                        long mipOperation = (Long) task.get("mipoper");

                        sf.setName(nodeName2);
                        sf.setHostName(hostName);
                        sf.setOptionalDatacenters(optionalDatacenter);
                        sf.setMIperOperation(mipOperation);

                        sf.setMiddleboxType(nodeType);
                        vmList.put(dcName, sf);
                        sfList.add(sf);
                    }

                    vmNameIdTable.put(nodeName2, vmId);
                }
                taskList.add(newTask);
            }
            //currJob.setInstances(taskVmList);
            Job currJob = new Job((String) job.get("id"),
                    (Long) job.get("submission_time"), taskList);

            currJob.getPendingTasks().forEach(task -> {
                ArrayList<String> pred = task.getPredecessors();

                pred.forEach(predecessor -> {
                    for (int i = 0; i < currJob.getPendingTasks().size(); i++) {
                        Task currTask = currJob.getPendingTasks().get(i);
                        String taskName = currTask.getName().split("_")[0];
                        if (predecessor.equals(taskName.substring(1,taskName.length()))) {
                            task.addPredecessorTask(currTask);
                            currTask.addSuccessorTask(task);
                        }

                    }
                });
            });

            // Add task to task list
            jobList.add(currJob);
        }

        return vmNameIdTable;
    }

    private Hashtable<String, Integer> parseVMs(JSONObject doc) {
        Hashtable<String, Integer> vmNameIdTable = new Hashtable<String, Integer>();

        // Parse VM nodes
        JSONArray nodes = (JSONArray) doc.get("nodes");

        @SuppressWarnings("unchecked")
        Iterator<JSONObject> iter = nodes.iterator();
        while (iter.hasNext()) {
            JSONObject node = iter.next();

            String nodeType = (String) node.get("type");
            String nodeName = (String) node.get("name");
            int pes = new BigDecimal((Long) node.get("pes")).intValueExact();
            long mips = (Long) node.get("mips");
            int ram = new BigDecimal((Long) node.get("ram")).intValueExact();
            long size = (Long) node.get("size");
            long bw = 0;

            if (node.get("bw") != null)
                bw = (Long) node.get("bw");

            long starttime = 0;
            long endtime = 0;
            if (node.get("starttime") != null)
                starttime = (Long) node.get("starttime");
            if (node.get("endtime") != null)
                endtime = (Long) node.get("endtime");

            String dcName = this.defaultDatacenter;
            if (node.get("datacenter") != null)
                dcName = (String) node.get("datacenter");

            // Optional datacenter specifies the alternative data center if 'data center' has no more resource.
            ArrayList<String> optionalDatacenter = null;
            if (node.get("subdatacenters") != null) {
                optionalDatacenter = new ArrayList<>();
                JSONArray subDCs = (JSONArray) node.get("subdatacenters");

                for (int i = 0; i < subDCs.size(); i++) {
                    String subdc = subDCs.get(i).toString();
                    optionalDatacenter.add(subdc);
                }
            }

            String hostName = "";
            if (node.get("host") != null)
                hostName = (String) node.get("host");

            long nums = 1;
            if (node.get("nums") != null)
                nums = (Long) node.get("nums");

            for (int n = 0; n < nums; n++) {
                String nodeName2 = nodeName;
                if (nums > 1) {
                    // Nodename should be numbered.
                    nodeName2 = nodeName + n;
                }

                CloudletScheduler clSch = new CloudletSchedulerSpaceSharedMonitor(Configuration.TIME_OUT);
                //CloudletScheduler clSch = new CloudletSchedulerTimeSharedMonitor(mips);
                int vmId = SDNVm.getUniqueVmId();

                if (nodeType.equalsIgnoreCase("vm")) {
                    // Create VM objects
                    SDNVm vm = new SDNVm(vmId, userId, mips, pes, ram, bw, size, "VMM", clSch, starttime, endtime);
                    vm.setName(nodeName2);
                    vm.setHostName(hostName);
                    vm.setOptionalDatacenters(optionalDatacenter);
                    vmList.put(dcName, vm);
                } else {
                    // Create ServiceFunction objects
                    ServiceFunction sf = new ServiceFunction(vmId, userId, mips, pes, ram, bw, size, "VMM", clSch, starttime, endtime);
                    long mipOperation = (Long) node.get("mipoper");

                    sf.setName(nodeName2);
                    sf.setHostName(hostName);
                    sf.setOptionalDatacenters(optionalDatacenter);
                    sf.setMIperOperation(mipOperation);

                    sf.setMiddleboxType(nodeType);
                    vmList.put(dcName, sf);
                    sfList.add(sf);
                }

                vmNameIdTable.put(nodeName2, vmId);
            }
        }

        return vmNameIdTable;
    }

    private Hashtable<String, Integer> parseLinks(JSONObject doc, Hashtable<String, Integer> vmNameIdTable) {
        Hashtable<String, Integer> flowNameIdTable = new Hashtable<String, Integer>();

        // AMANDAAAA commenting out links
		/*
		// Parse VM-VM links
		JSONArray links = (JSONArray) doc.get("links");
		
		@SuppressWarnings("unchecked")
		Iterator<JSONObject> linksIter = links.iterator(); 
		while(linksIter.hasNext()){
			JSONObject link = linksIter.next();
			String name = (String) link.get("name");
			String src = (String) link.get("source");  
			String dst = (String) link.get("destination");
			
			Object reqLat = link.get("latency");
			Object reqBw = link.get("bandwidth");
			
			double lat = 0.0;
			long bw = 0;
			
			if(reqLat != null)
				lat = (Double) reqLat;
			if(reqBw != null)
				bw = (Long) reqBw;
			
			int srcId = vmNameIdTable.get(src);
			int dstId = vmNameIdTable.get(dst);
			
			int flowId = -1;
			
			if(name == null || "default".equalsIgnoreCase(name)) {
				// default flow.
				flowId = -1;
			}
			else {
				flowId = flowNumbers++;
			}
			
			FlowConfig arc = new FlowConfig(srcId, dstId, flowId, bw, lat);
			if(flowId != -1) {
				arc.setName(name);
			}
			
			arcList.add(arc);
			flowNameIdTable.put(name, flowId);
		}
		*/
        return flowNameIdTable;
    }

    private void parseSFCPolicies(JSONObject doc, Hashtable<String, Integer> vmNameIdTable, Hashtable<String, Integer> flowNameIdTable) {
        // Parse SFC policies
        JSONArray policies = (JSONArray) doc.get("policies");

        if (policies == null)
            return;

        @SuppressWarnings("unchecked")
        Iterator<JSONObject> policyIter = policies.iterator();
        while (policyIter.hasNext()) {
            JSONObject policy = policyIter.next();
            String name = (String) policy.get("name");
            String src = (String) policy.get("source");
            String dst = (String) policy.get("destination");
            String flowname = (String) policy.get("flowname");
            Double expectedTime = (Double) policy.get("expected_time");
            if (expectedTime == null) {
                expectedTime = Double.POSITIVE_INFINITY;
            }

            int srcId = vmNameIdTable.get(src);
            int dstId = vmNameIdTable.get(dst);
            int flowId = flowNameIdTable.get(flowname);

            JSONArray sfc = (JSONArray) policy.get("sfc");
            ArrayList<Integer> sfcList = new ArrayList<Integer>();
            for (int i = 0; i < sfc.size(); i++) {
                String sfName = sfc.get(i).toString();
                int sfVmId = vmNameIdTable.get(sfName);
                sfcList.add(sfVmId);
            }

            ServiceFunctionChainPolicy pol = new ServiceFunctionChainPolicy(srcId, dstId, flowId, sfcList, expectedTime);
            if (name != null)
                pol.setName(name);

            policyList.add(pol);
        }
    }

    public Collection<SDNVm> getVmList(String dcName) {
        return vmList.get(dcName);
    }

    public List<FlowConfig> getArcList() {
        return arcList;
    }

    public List<ServiceFunction> getSFList() {
        return sfList;
    }

    public List<ServiceFunctionChainPolicy> getSFCPolicyList() {
        return policyList;
    }
}
