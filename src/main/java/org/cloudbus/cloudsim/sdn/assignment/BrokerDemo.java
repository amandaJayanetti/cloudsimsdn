package org.cloudbus.cloudsim.sdn.assignment;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.assignment.BrokerAdvanced;
import org.cloudbus.cloudsim.assignment.Pair;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class BrokerDemo {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;
	/** The vmlist. */
	private static List<Vm> vmlist;
	/**
	 * A list of cloudlets with the resource requirements. Contains elements of the form: [ cloudletId, {RAM, Storage} ]
	 */
	private static Map<Integer, Vector<Double>> cloudletSpecList = new HashMap<>();

	public static void main(String[] args) throws IOException {
		// First step: Initialize the CloudSim package. It should be called before creating any entities.
		int num_user = 1; // number of cloud users
		Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
		boolean trace_flag = false; // trace events
		CloudSim.init(num_user, calendar, trace_flag);
		
		// Third step: Create Broker
		BrokerAdvanced broker = createBroker();
		int brokerId = broker.getId();
		cloudletList = new ArrayList<Cloudlet>();
		initCloudlets("/home/student.unimelb.edu.au/amjayanetti/Documents/Simulators/cloudsim/modules/cloudsim/src/main/java/org/cloudbus/cloudsim/assignment/resources/workload.txt", broker);
		// submit cloudlet list to the broker
		broker.submitCloudletList(cloudletList);
		broker.setCloudletSpecList(cloudletSpecList);

		vmlist = new ArrayList<Vm>();
		initVms("/home/student.unimelb.edu.au/amjayanetti/Documents/CLOUDS/Assignments/Python/vmtypes.txt", broker);

/*
		initVms("/home/student.unimelb.edu.au/amjayanetti/Documents/CLOUDS/Assignments/Python/vmtypes.txt", broker);
		List<Vm> vmCategoriesList = new ArrayList<>();
		// VM Categories
		Vm vm_1 = new Vm(0, brokerId, 1000, 1, 3500, 1000, 1000, "Xen", new CloudletSchedulerTimeShared());
		Vm vm_11 = new Vm(1, brokerId, 1000, 2, 3500, 1000, 1000, "Xen", new CloudletSchedulerTimeShared());
		Vm vm_12 = new Vm(2, brokerId, 1000, 3, 3500, 1000, 1000, "Xen", new CloudletSchedulerTimeShared());
		Vm vm_13 = new Vm(3, brokerId, 1000, 8, 3500, 1000, 1000, "Xen", new CloudletSchedulerTimeShared());
		Vm vm_2 = new Vm(4, brokerId, 1000, 1, 2500, 1000, 900, "Xen", new CloudletSchedulerTimeShared());
		Vm vm_21 = new Vm(5, brokerId, 1000, 2, 2500, 1000, 900, "Xen", new CloudletSchedulerTimeShared());
		Vm vm_22 = new Vm(5, brokerId, 1000, 8, 2500, 1000, 900, "Xen", new CloudletSchedulerTimeShared());
		Vm vm_3 = new Vm(6, brokerId, 1000, 2, 1500, 1000, 800, "Xen", new CloudletSchedulerTimeShared());
		Vm vm_4 = new Vm(7, brokerId, 1000, 1, 1000, 1000, 600, "Xen", new CloudletSchedulerTimeShared());

		vmCategoriesList.add(vm_1);
		vmCategoriesList.add(vm_11);
		//vmCategoriesList.add(vm_12);
		//vmCategoriesList.add(vm_13);
		vmCategoriesList.add(vm_2);
		vmCategoriesList.add(vm_21);
		//vmCategoriesList.add(vm_22);
		vmCategoriesList.add(vm_3);
		vmCategoriesList.add(vm_4);

		broker.setVmCategoryList(vmCategoriesList);

		// Initialize VM List
		vmlist = new ArrayList<Vm>();
		int incrementalVmId = 0;

		// Create 5 VMs of each VM category
		for (int i = 0; i < 1; i++) {
			Vm vm = new Vm(incrementalVmId++, brokerId, vm_1.getMips(), vm_1.getNumberOfPes(), vm_1.getRam(), vm_1.getBw(), vm_1.getSize(), "Xen", new CloudletSchedulerAdvanced());
			// add the VM to the vmList
			vmlist.add(vm);
		}

		for (int i = 0; i < 1; i++) {
			Vm vm = new Vm(incrementalVmId++, brokerId, vm_11.getMips(), vm_11.getNumberOfPes(), vm_11.getRam(), vm_11.getBw(), vm_11.getSize(), "Xen", new CloudletSchedulerAdvanced());
			// add the VM to the vmList
			vmlist.add(vm);
		}

		for (int i = 0; i < 1; i++) {
			Vm vm = new Vm(incrementalVmId++, brokerId, vm_2.getMips(), vm_2.getNumberOfPes(), vm_2.getRam(), vm_2.getBw(), vm_2.getSize(), "Xen", new CloudletSchedulerAdvanced());
			// add the VM to the vmList
			vmlist.add(vm);
		}

		for (int i = 0; i < 1; i++) {
			Vm vm = new Vm(incrementalVmId++, brokerId, vm_21.getMips(), vm_21.getNumberOfPes(), vm_21.getRam(), vm_21.getBw(), vm_21.getSize(), "Xen", new CloudletSchedulerAdvanced());
			// add the VM to the vmList
			vmlist.add(vm);
		}

		for (int i = 0; i < 1; i++) {
			Vm vm = new Vm(incrementalVmId++, brokerId, vm_4.getMips(), vm_4.getNumberOfPes(), vm_4.getRam(), vm_4.getBw(), vm_4.getSize(), "Xen", new CloudletSchedulerAdvanced());
			// add the VM to the vmList
			vmlist.add(vm);
		}

		for (int i = 0; i < 1; i++) {
			Vm vm = new Vm(incrementalVmId++, brokerId, vm_3.getMips(), vm_3.getNumberOfPes(), vm_3.getRam(), vm_3.getBw(), vm_3.getSize(), "Xen", new CloudletSchedulerAdvanced());
			// add the VM to the vmList
			vmlist.add(vm);
		}

		// CONTINUE adding VMS....

*/
		// submit vm list to the broker
		broker.submitVmList(vmlist);

		Datacenter datacenter0 = createDatacenter("Datacenter_0");
		// Sixth step: Starts the simulation
		CloudSim.startSimulation();

		CloudSim.stopSimulation();
	}

	/**
	 * Creates the datacenter.
	 *
	 * @param name the name
	 *
	 * @return the datacenter
	 */
	private static Datacenter createDatacenter(String name) {

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		// our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		List<Pe> peList = new ArrayList<Pe>();

		int noPes = 8;
		int mips = 10000;

		for (int i = 0; i <= noPes; i++) {
			// 3. Create PEs and add these into a list.
			peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		}

		int hostCount = 500;

		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
		int hostId = 0;
		int ram = 66560; // host memory (MB)
		long storage = 10485760; // host storage (MB)
		int bw = 10000;

		for (int i = 0; i <= hostCount; i++) {
			System.out.println("Adding new host..");
			hostId = i;
			hostList.add(
					new Host(
							hostId,
							new RamProvisionerSimple(ram),
							new BwProvisionerSimple(bw),
							storage,
							peList,
							new VmSchedulerTimeShared(peList)
					)
			);
		}
		// 5. Create a DatacenterCharacteristics object that stores the
		// properties of a data center: architecture, OS, list of
		// Machines, allocation policy: time- or space-shared, time zone
		// and its price (G$/Pe time unit).
		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this
		// resource
		double costPerBw = 0.0; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
		// devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem,
				costPerStorage, costPerBw);

		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	// We strongly encourage users to develop their own broker policies, to
	// submit vms and cloudlets according
	// to the specific rules of the simulated scenario
	/**
	 * Creates the broker.
	 *
	 * @return the datacenter broker
	 */
	private static BrokerAdvanced createBroker() {
		BrokerAdvanced broker = null;
		try {
			broker = new BrokerAdvanced("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}
	
	public static void initCloudlets(String filepath, DatacenterBroker broker) throws IOException {
		//Files.lines(Paths.get(filepath)).forEach(System.out::println);
		
		try {
			FileReader fr = new FileReader(filepath);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			int cloudletId = 0;
			while (line != null) {
				String[] tempArray = line.split(" ");
				String submissionTime = tempArray[0];
				String mis = tempArray[1];
				String minMemory = tempArray[2];
				String minStorage = tempArray[3];
				String deadline = tempArray[4];
				int cost = Integer.parseInt(mis)/1000;  // Machines must be modeled after Amazon EC2 virtual
				// machines (Sydney price). You can assume that 1ECU=1000 MIPS.

				// Group tasks according to non-overlapping sets and assign one VM category for each set based on the task with highest resource req

				// Cloudlet properties
				long length = 400000;
				long fileSize = 300;
				long outputSize = 300;
				int pesNumber = 1; // number of cpus
				UtilizationModel utilizationModel = new UtilizationModelFull();

				Cloudlet cloudlet = new Cloudlet(cloudletId, length, pesNumber, fileSize,
	                                        outputSize, utilizationModel, utilizationModel, 
	                                        utilizationModel);
				cloudlet.setSubmissionTime(Double.parseDouble(submissionTime));
				cloudlet.setCloudletLength(Long.parseLong(mis));
				// Not sure about this setExecParam() function. Debug and verify!
				cloudlet.setExecParam(Double.parseDouble(deadline), Double.parseDouble(deadline));
				cloudlet.setUserId(broker.getId());
				// add the cloudlet to the list
				cloudletList.add(cloudlet);

				Vector<Double> cloudletSpecs = new Vector<>();
				cloudletSpecs.add(Double.parseDouble(minMemory));
				cloudletSpecs.add(Double.parseDouble(minStorage));
				// See if there's a way to add deadline in the cloudlet itself
				cloudletSpecs.add(Double.parseDouble(deadline));
				cloudletSpecList.put(cloudletId, cloudletSpecs);


				line = br.readLine();
				cloudletId ++;
			}	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void initVms(String filepath, BrokerAdvanced broker) throws IOException {
		int vmId = 0;
		int vmCategoryId = 0;
		List<Vm> vmCategoriesList = new ArrayList<>();
		// VM upper bound category
		vmCategoriesList.add(new Vm(vmCategoryId++, broker.getId(), 1000, 1, 3500, 1000, 1000, "Xen", new CloudletSchedulerTimeShared()));
		for (int i = 0; i < 10; i++) {
			Vm vm = new Vm(vmId++, broker.getId(), 1000, 1, 3500, 1000, 1000, "Xen", new CloudletSchedulerTimeShared());
			vmlist.add(vm);
		}
		// VM Categories
		try {
			FileReader fr = new FileReader(filepath);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			while (line != null) {
				String[] tempArray = line.split(" ");
				String ram = tempArray[0];
				String storage = tempArray[1];
				Vm vmCategory = new Vm(vmCategoryId++, broker.getId(), 1000, 1, Integer.parseInt(ram), 1000, Long.parseLong(storage), "Xen", new CloudletSchedulerTimeShared());
				vmCategoriesList.add(vmCategory);

				for (int i = 0; i < 10; i++) {
					Vm vm = new Vm(vmId++, broker.getId(), 1000, 1, Integer.parseInt(ram), 1000, Long.parseLong(storage), "Xen", new CloudletSchedulerTimeShared());
					vmlist.add(vm);
				}
				line = br.readLine();
			}
			broker.setVmCategoryList(vmCategoriesList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private boolean nonOverlapping(org.cloudbus.cloudsim.assignment.Pair interval, SortedSet<org.cloudbus.cloudsim.assignment.Pair> selectedIntervals) {
		if(selectedIntervals.isEmpty())
			return true;
		if(selectedIntervals.contains(interval)){
			return true;
		}
		org.cloudbus.cloudsim.assignment.Pair[] sortedSelections = selectedIntervals.toArray(new org.cloudbus.cloudsim.assignment.Pair[0]);
		int pos = Arrays.binarySearch(sortedSelections, interval, new Comparator<org.cloudbus.cloudsim.assignment.Pair>() {

			@Override
			public int compare(org.cloudbus.cloudsim.assignment.Pair o1, Pair o2) {
				return o1.getStart() - o2.getEnd();
			}
		});
		pos = (-pos) -1;
		if(pos == sortedSelections.length){
			if(sortedSelections[pos - 1].getEnd() < interval.getStart()){
				return true;
			}

		}
		else if(sortedSelections[pos].getEnd() > interval.getStart()){
			if(pos + 1 < sortedSelections.length){
				if(sortedSelections[pos + 1].getEnd() < interval.getStart()){
					return false;
				}
			}
			if(pos - 1 >= 0){
				if(sortedSelections[pos - 1].getEnd() < interval.getStart()){
					return false;
				}
			}
			return true;
		}
		return false;
	}
}

