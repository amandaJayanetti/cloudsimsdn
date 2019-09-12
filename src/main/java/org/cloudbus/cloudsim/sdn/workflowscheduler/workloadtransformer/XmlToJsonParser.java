package org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import org.cloudbus.cloudsim.Log;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.*;
import java.io.*;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

public class XmlToJsonParser {
    static long mipsMultiplicationFactor = 10;

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
         //   File inputFile = new File("/home/student.unimelb.edu.au/amjayanetti/Documents/Simulators/cloudsimsdn/src/main/java/org/cloudbus/cloudsim/sdn/workflowscheduler/workloadtransformer/Montage_1000.xml");

            ArrayList<Job> jobList = new ArrayList<>();
            String file = "/home/student.unimelb.edu.au/amjayanetti/Documents/CLOUDS/LiteratureReview/1_/Workloads/MixedWorkloads/aggrgate.xml";

            File path = new File("/home/student.unimelb.edu.au/amjayanetti/Documents/CLOUDS/LiteratureReview/1_/Workloads/MixedWorkloads");

            File [] files = path.listFiles();

            for (int p = 0; p < files.length; p++) {
                File inputFile = files[p];
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(inputFile);
                doc.getDocumentElement().normalize();

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
                        double mips_ = Double.parseDouble(element.getAttribute("runtime")); // * mipsMultiplicationFactor;
                        long mips = (long) mips_ * mipsMultiplicationFactor;
                        ArrayList<String> predecessors = new ArrayList<>();

                        task = new Task(name, jobId, 1, 0, 0, 100, cores, 1000, mips, predecessors, 1);

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

                job = new Job(tasks, jobId, 0);
                jobList.add(job);
            }
            System.out.printf("");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
