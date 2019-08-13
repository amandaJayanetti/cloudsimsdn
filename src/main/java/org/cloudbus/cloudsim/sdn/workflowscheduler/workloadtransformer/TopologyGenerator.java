package org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TopologyGenerator {

    public static void main(String[] args) throws IOException {
        JSONObject resources = new JSONObject();

        JSONArray nodes = new JSONArray();
        JSONArray links = new JSONArray();

        int host_no = 0;
        for (int i = 0; i < 1000; i++) {
            JSONObject node = new JSONObject();
            node.put("ram", 10000); // Max memory in the normalized range [0,100] since i multiplied the mem value by 100 range in workload transformer is [100,10000]
            node.put("pes", 4);
            node.put("name", "h_" + i);
            node.put("bw", 1000000000);
            node.put("mips", 1000);
            node.put("type", "host");
            node.put("storage", 10000000);
            node.put("cluster", 1);
            nodes.add(node);
            host_no = i + 1;

            JSONObject link = new JSONObject();
            link.put("latency", 0.1);
            link.put("source", "e" + 1);
            link.put("destination", "h_" + i);
            links.add(link);
        }

        JSONObject edge1 = new JSONObject();
        edge1.put("iops", 1000000000);
        edge1.put("name", "e" + 1);
        edge1.put("bw", 1000000000);
        edge1.put("type", "edge");
        nodes.add(edge1);

        for (int i = host_no; i < 8; i++) {
            JSONObject node = new JSONObject();
            node.put("ram", 10000); // Max memory in the normalized range [0,100] since i multiplied the mem value by 100 range in workload transformer is [100,10000]
            node.put("pes", 8);
            node.put("name", "h_" + i);
            node.put("bw", 1000000000);
            node.put("mips", 2000);
            node.put("type", "host");
            node.put("storage", 10000000);
            node.put("cluster", 2);
            nodes.add(node);
            host_no = i + 1;

            JSONObject link = new JSONObject();
            link.put("latency", 0.1);
            link.put("source",  "e" + 2);
            link.put("destination", "h_" + i);
            links.add(link);
        }

        JSONObject edge2 = new JSONObject();
        edge2.put("iops", 1000000000);
        edge2.put("name", "e" + 2);
        edge2.put("bw", 1000000000);
        edge2.put("type", "edge");
        nodes.add(edge2);

        for (int i = host_no; i < 20; i++) {
            JSONObject node = new JSONObject();
            node.put("ram", 10000); // Max memory in the normalized range [0,100] since i multiplied the mem value by 100 range in workload transformer is [100,10000]
            node.put("pes", 4);
            node.put("name", "h_" + i);
            node.put("bw", 1000000000);
            node.put("mips", 150);
            node.put("type", "host");
            node.put("storage", 10000000);
            node.put("cluster", 3);
            nodes.add(node);
            host_no = i + 1;

            JSONObject link = new JSONObject();
            link.put("latency", 0.1);
            link.put("source",  "e" + 3);
            link.put("destination", "h_" + i);
            links.add(link);
        }

        JSONObject edge3 = new JSONObject();
        edge3.put("iops", 1000000000);
        edge3.put("name", "e" + 3);
        edge3.put("bw", 1000000000);
        edge3.put("type", "edge");
        nodes.add(edge3);

        for (int i = host_no; i < 25; i++) {
            JSONObject node = new JSONObject();
            node.put("ram", 10000); // Max memory in the normalized range [0,100] since i multiplied the mem value by 100 range in workload transformer is [100,10000]
            node.put("pes", 4);
            node.put("name", "h_" + i);
            node.put("bw", 1000000000);
            node.put("mips", 1000);
            node.put("type", "host");
            node.put("storage", 10000000);
            node.put("cluster", 4);
            nodes.add(node);
            host_no = i + 1;

            JSONObject link = new JSONObject();
            link.put("latency", 0.1);
            link.put("source",  "e" + 4);
            link.put("destination", "h_" + i);
            links.add(link);
        }

        JSONObject edge4 = new JSONObject();
        edge4.put("iops", 1000000000);
        edge4.put("name", "e" + 4);
        edge4.put("bw", 1000000000);
        edge4.put("type", "edge");
        nodes.add(edge4);

        JSONObject core1 = new JSONObject();
        core1.put("iops", 1000000000);
        core1.put("name", "c1");
        core1.put("bw", 1000000000);
        core1.put("type", "core");
        nodes.add(core1);

        JSONObject core2 = new JSONObject();
        core2.put("iops", 1000000000);
        core2.put("name", "c2");
        core2.put("bw", 1000000000);
        core2.put("type", "core");
        nodes.add(core2);

        JSONObject aggr1 = new JSONObject();
        aggr1.put("iops", 1000000000);
        aggr1.put("name", "a1");
        aggr1.put("bw", 1000000000);
        aggr1.put("type", "aggregate");
        nodes.add(aggr1);

        JSONObject aggr2 = new JSONObject();
        aggr2.put("iops", 1000000000);
        aggr2.put("name", "a2");
        aggr2.put("bw", 1000000000);
        aggr2.put("type", "aggregate");
        nodes.add(aggr2);

        JSONObject aggr3 = new JSONObject();
        aggr3.put("iops", 1000000000);
        aggr3.put("name", "a3");
        aggr3.put("bw", 1000000000);
        aggr3.put("type", "aggregate");
        nodes.add(aggr3);

        JSONObject aggr4 = new JSONObject();
        aggr4.put("iops", 1000000000);
        aggr4.put("name", "a4");
        aggr4.put("bw", 1000000000);
        aggr4.put("type", "aggregate");
        nodes.add(aggr4);


        JSONObject aggrEdgeLink1 = new JSONObject();
        aggrEdgeLink1.put("latency", 0.1);
        aggrEdgeLink1.put("source", "a1");
        aggrEdgeLink1.put("destination", "e1");
        links.add(aggrEdgeLink1);

        JSONObject aggrEdgeLink2 = new JSONObject();
        aggrEdgeLink2.put("latency", 0.1);
        aggrEdgeLink2.put("source", "a1");
        aggrEdgeLink2.put("destination", "e2");
        links.add(aggrEdgeLink2);


        JSONObject aggrEdgeLink3 = new JSONObject();
        aggrEdgeLink3.put("latency", 0.1);
        aggrEdgeLink3.put("source", "a2");
        aggrEdgeLink3.put("destination", "e1");
        links.add(aggrEdgeLink3);

        JSONObject aggrEdgeLink4 = new JSONObject();
        aggrEdgeLink4.put("latency", 0.1);
        aggrEdgeLink4.put("source", "a2");
        aggrEdgeLink4.put("destination", "e2");
        links.add(aggrEdgeLink4);

        JSONObject aggrEdgeLink5 = new JSONObject();
        aggrEdgeLink5.put("latency", 0.1);
        aggrEdgeLink5.put("source", "a3");
        aggrEdgeLink5.put("destination", "e3");
        links.add(aggrEdgeLink5);

        JSONObject aggrEdgeLink6 = new JSONObject();
        aggrEdgeLink6.put("latency", 0.1);
        aggrEdgeLink6.put("source", "a3");
        aggrEdgeLink6.put("destination", "e4");
        links.add(aggrEdgeLink6);


        JSONObject aggrEdgeLink7 = new JSONObject();
        aggrEdgeLink7.put("latency", 0.1);
        aggrEdgeLink7.put("source", "a4");
        aggrEdgeLink7.put("destination", "e3");
        links.add(aggrEdgeLink7);

        JSONObject aggrEdgeLink8 = new JSONObject();
        aggrEdgeLink8.put("latency", 0.1);
        aggrEdgeLink8.put("source", "a4");
        aggrEdgeLink8.put("destination", "e4");
        links.add(aggrEdgeLink8);

        JSONObject aggrCoreLink1 = new JSONObject();
        aggrCoreLink1.put("latency", 0.1);
        aggrCoreLink1.put("source", "c1");
        aggrCoreLink1.put("destination", "a1");
        links.add(aggrCoreLink1);

        JSONObject aggrCoreLink2 = new JSONObject();
        aggrCoreLink2.put("latency", 0.1);
        aggrCoreLink2.put("source", "c1");
        aggrCoreLink2.put("destination", "a3");
        links.add(aggrCoreLink2);

        JSONObject aggrCoreLink3 = new JSONObject();
        aggrCoreLink3.put("latency", 0.1);
        aggrCoreLink3.put("source", "c2");
        aggrCoreLink3.put("destination", "a2");
        links.add(aggrCoreLink3);

        JSONObject aggrCoreLink4 = new JSONObject();
        aggrCoreLink4.put("latency", 0.1);
        aggrCoreLink4.put("source", "c2");
        aggrCoreLink4.put("destination", "a4");
        links.add(aggrCoreLink4);

        resources.put("nodes", nodes);
        resources.put("links", links);
        Files.write(Paths.get("resources.json"), resources.toJSONString().getBytes());
    }

}

