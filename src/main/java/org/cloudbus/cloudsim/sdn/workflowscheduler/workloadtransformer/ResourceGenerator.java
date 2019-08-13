package org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ResourceGenerator {

    public static void main(String[] args) throws IOException {
        JSONObject resources = new JSONObject();

        JSONArray nodes = new JSONArray();
        JSONArray links = new JSONArray();

        int host_no = 0;
        for (int i = 0; i < 5; i++) {
            JSONObject node = new JSONObject();
            node.put("ram", 10000); // Max memory in the normalized range [0,100] since i multiplied the mem value by 100 range in workload transformer is [100,10000]
            node.put("pes", 4);
            node.put("name", "h_" + i);
            node.put("bw", 1000000000);
            node.put("mips", 1000);
            node.put("type", "host");
            node.put("storage", 10000000);
            nodes.add(node);
            host_no = i + 1;
        }

        for (int i = host_no; i < 8; i++) {
            JSONObject node = new JSONObject();
            node.put("ram", 10000); // Max memory in the normalized range [0,100] since i multiplied the mem value by 100 range in workload transformer is [100,10000]
            node.put("pes", 8);
            node.put("name", "h_" + i);
            node.put("bw", 1000000000);
            node.put("mips", 2000);
            node.put("type", "host");
            node.put("storage", 10000000);
            nodes.add(node);
            host_no = i + 1;
        }

        for (int i = host_no; i < 20; i++) {
            JSONObject node = new JSONObject();
            node.put("ram", 10000); // Max memory in the normalized range [0,100] since i multiplied the mem value by 100 range in workload transformer is [100,10000]
            node.put("pes", 4);
            node.put("name", "h_" + i);
            node.put("bw", 1000000000);
            node.put("mips", 150);
            node.put("type", "host");
            node.put("storage", 10000000);
            nodes.add(node);
        }

        for (int i = 0; i < 100; i++) {
            JSONObject edge = new JSONObject();
            edge.put("iops", 1000000000);
            edge.put("name", "e" + i);
            edge.put("bw", 1000000000);
            edge.put("type", "edge");
            nodes.add(edge);
        }

        JSONObject core = new JSONObject();
        core.put("iops", 1000000000);
        core.put("name", "c");
        core.put("bw", 1000000000);
        core.put("type", "core");
        nodes.add(core);

        int j = 0;
        while (j < 20) {
            JSONObject coreLink = new JSONObject();
            coreLink.put("latency", 0.1);
            coreLink.put("source", "c");
            coreLink.put("destination", "e" + j/10);
            links.add(coreLink);

            for (int k = j; k < j + 10; k++) {
                JSONObject link = new JSONObject();
                link.put("latency", 0.1);
                link.put("source", "e" + j/10);
                link.put("destination", "h_" + k);
                links.add(link);
            }
            j += 10;
        }

        resources.put("nodes", nodes);
        resources.put("links", links);
        Files.write(Paths.get("resources.json"), resources.toJSONString().getBytes());
    }

}
