package org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager;

import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

import java.util.List;
import java.util.Map;

public class JobTaskVMMapper {
    // job-id, List{task-id, List<SDNVms>)} E.g  {j_3, {R6_3, [vm0, vm1, ...vm371]}, j_3, {J4_2_3, [vm450, vm451...]}, ....}
    private Map<String, Map<String, List<SDNVm>>> ramProvisioner;

}
