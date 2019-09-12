package org.cloudbus.cloudsim.sdn.workflowscheduler.aco.baseline;

import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.sdn.physicalcomponents.Link;
import org.cloudbus.cloudsim.sdn.physicalcomponents.Node;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicy;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicyBandwidthAllocation;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workflowscheduler.taskmanager.VmAllocationPolicyToTasks;
import org.cloudbus.cloudsim.sdn.workflowscheduler.workloadtransformer.Task;

import java.util.*;
import java.util.stream.IntStream;

public class AntColonyOptimization {
    private double c = 1.0;
    private double alpha = 1;
    private double beta = 5;
    private double gamma = 5;
    private double evaporation = 0.5;
    private double antFactor = 0.8;
    private double randomFactor = 0.01;

    private int maxIterations = 100;

    private int numberOfHosts;
    private Task task;
    private int numberOfAnts;
    private double graph[][];
    private double trails[][];
    private List<Ant> ants = new ArrayList<>();
    private Random random = new Random();
    Map<SDNHost, Double> probabilitiesList = new HashMap<>();
    private List<SDNHost> hostList;
    private List<SDNVm> vmList;
    private List<SDNHost> currClique;
    private List<SDNHost> eligibleHosts;

    private ArrayList<Integer> bestTourOrderOverall;
    private double bestTourLengthOverall;
    private Multimap<SDNHost, SDNVm> bestAllocationPlan;
    private VmAllocationPolicyToTasks policy;

    public AntColonyOptimization(List<SDNHost> hostList, List<SDNVm> vmList, VmAllocationPolicyToTasks policy) {
        this.hostList = hostList;
        graph = policy.getHostMatrix();
        numberOfHosts = graph.length;
        numberOfAnts = (int) (numberOfHosts * antFactor);
        this.policy = policy;

        trails = new double[numberOfHosts][numberOfHosts];
        this.vmList = vmList;

        IntStream.range(0, numberOfAnts)
                .forEach(i -> {
                    // Since we need to pass a separate copy of the jobs to each ant. Arraylist pass by reference if copied in the ordinary way.
                    ArrayList<SDNVm> taskVms = new ArrayList<>();
                    vmList.forEach(job -> {
                        taskVms.add(job);
                    });
                    ants.add(new Ant(numberOfHosts, taskVms, hostList));
                });
    }

    public AntColonyOptimization(List<SDNHost> hostList, List<SDNHost> eligibleHosts, List<SDNVm> vmList, List<SDNHost> currentClique, VmAllocationPolicyToTasks policy) {
        this.hostList = hostList;
        // Need to keep copies of current clique and eligible hosts since we need these details in function initializeAntsWithPartialState for initializing ants in each new iteration
        this.currClique = currentClique;
        this.eligibleHosts = eligibleHosts;

        this.policy = policy;
        graph = policy.getHostMatrix();
        numberOfHosts = graph.length;
        numberOfAnts = (int) (numberOfHosts * antFactor);

        trails = new double[numberOfHosts][numberOfHosts];
        this.vmList = vmList;

        for (int i = 0; i < ants.size(); i++) {
            // Since we need to pass a separate copy of the jobs to each ant. Arraylist pass by reference if copied in the ordinary way.
            ArrayList<SDNVm> taskVms = new ArrayList<>();
            vmList.forEach(job -> {
                taskVms.add(job);
            });
            ArrayList<SDNHost> currSelectedHosts = new ArrayList<>();
            currentClique.forEach(host -> {
                currSelectedHosts.add(host);
            });
            ants.add(new Ant(numberOfHosts, taskVms, hostList, eligibleHosts, currSelectedHosts));
        }

        this.task = policy.getTaskIdOfTheInstanceInVm(vmList.get(0));
    }

    protected double computeMinNetworkHops(LinkSelectionPolicy linkSelector, Node src, Node dest, double noHops) {
        if (src.equals(dest))
            return noHops;
        List<Link> nextLinkCandidates = src.getRoute(dest);
        Link nextLink = linkSelector.selectLink(nextLinkCandidates, 0, src, dest, src);
        Node nextHop = nextLink.getOtherNode(src);
        return computeMinNetworkHops(linkSelector, nextHop, dest, ++noHops);
    }

    /**
     * Perform aco optimization
     */
    public Multimap<SDNHost, SDNVm> startAntOptimization() {
        clearTrails();
        IntStream.range(0, maxIterations)
                .forEach(i -> {
                    initializeAnts();
                    //setupAnts();
                    moveAnts();
                    updateBestTrail();
                    updateTrails();
                });
        System.out.println("Best tour length: " + (bestTourLengthOverall));
        System.out.println("Best tour order: ");
        for (Integer hostIndex : bestTourOrderOverall) {
            System.out.print(hostList.get(hostIndex).getId() + " ");
        }
        System.out.println();
        return bestAllocationPlan;
    }

    /**
     * Perform aco optimization - NOTE the setupAnts() is not there
     */
    public Multimap<SDNHost, SDNVm> startAntOptimizationFromPartialState() {
        clearTrails();
        IntStream.range(0, maxIterations)
                .forEach(i -> {
                    initializeAntsWithPartialState();
                    moveAnts();
                    updateBestTrail();
                    updateTrails();
                });
        System.out.println("Best tour length: " + (bestTourLengthOverall));
        System.out.println("Best tour order: ");
        for (Integer hostIndex : bestTourOrderOverall) {
            System.out.print(hostList.get(hostIndex).getId() + " ");
        }
        System.out.println();
        return bestAllocationPlan;
    }

    /**
     * Prepare ants for the simulation
     */
    private void setupAnts() {
        ants.forEach(ant -> {
            // Select the first host randomly i.e for a particular ant if random.nextInt(numberOfHosts) is 5. The first random host it considers is hostList.get(5).
            // can call this function visitHost
            ant.visitHost(hostList.get(random.nextInt(numberOfHosts)));
        });
    }

    /**
     * Initialize ants for the simulation
     */
    private void initializeAnts() {
        ants.clear();
        IntStream.range(0, numberOfAnts)
                .forEach(i -> {
                    // Since we need to pass a separate copy of the jobs to each ant. Arraylist pass by reference if copied in the ordinary way.
                    ArrayList<SDNVm> taskVms = new ArrayList<>();
                    vmList.forEach(job -> {
                        taskVms.add(job);
                    });
                    ants.add(new Ant(numberOfHosts, taskVms, hostList));
                });
    }

    /**
     * Initialize ants for the simulation
     */
    private void initializeAntsWithPartialState() {
        ants.clear();
        IntStream.range(0, numberOfAnts)
                .forEach(i -> {
                    // Since we need to pass a separate copy of the jobs to each ant. Arraylist pass by reference if copied in the ordinary way.
                    ArrayList<SDNVm> taskVms = new ArrayList<>();
                    vmList.forEach(job -> {
                        taskVms.add(job);
                    });
                    ArrayList<SDNHost> currSelectedHosts = new ArrayList<>();
                    currClique.forEach(host -> {
                        currSelectedHosts.add(host);
                    });
                    ants.add(new Ant(numberOfHosts, taskVms, hostList, eligibleHosts, currSelectedHosts));
                });
    }

    /**
     * At each iteration, move ants
     */
    private void moveAnts() {
        ants.forEach(ant -> {
            while (!ant.unallocatedVmList.isEmpty()) {
                ant.visitHost(selectNextCity(ant));
            }
        });
    }

    /**
     * Select next host
     */
    private SDNHost selectNextCity(Ant ant) {
        // Allow a small chance for random host selection (If necessary we can remove this random possibility by setting randomFactor to 0.0)
        int randIndex = random.nextInt(numberOfHosts);

        // To leave a random possibility for ants to explore new hosts rather than exploiting accumulated knowledge to select a host
        if (random.nextDouble() < randomFactor) {
            int finalRandIndex = randIndex;
            OptionalInt cityIndex = IntStream.range(0, numberOfHosts)
                    .filter(i -> i == finalRandIndex)
                    .findFirst();
            if (cityIndex.isPresent()) {
                //return hostList.get(cityIndex.getAsInt());
            }
        }

        // Calculate the probabilities of this NewAnt selecting any one of the hosts in hostList as the next host from current host.
        calculateProbabilities(ant);

        // Find the host with the highest probability
        Map.Entry<SDNHost, Double> tuple = probabilitiesList.entrySet().iterator().next();
        SDNHost maxHost = tuple.getKey();
        double maxProbability = 0.0;

        for (Map.Entry<SDNHost, Double> entry : probabilitiesList.entrySet()) {
            if (entry.getValue() > maxProbability) {
                maxProbability = entry.getValue();
                maxHost = entry.getKey();
            }
        }


        // AMANDAAAA ---- IMPORTANT we also need to favor more utilised servers rather than unused ones... so from out of the servers with
        // highest probability multiply the probability by utilisation percentage

        return maxHost;
    }

    /**
     * Calculate the probabilities for selecting next host
     */
    public void calculateProbabilities(Ant ant) {
        probabilitiesList.clear();
        List<SDNHost> currClique = ant.getSelectedHostList();
        List<SDNHost> candidateHosts = new ArrayList<>();
        ant.getEligibleHostList().forEach(host -> {
            // Since normal copying of of arraylists pass by reference
            candidateHosts.add(host);
        });
        //candidateHosts.removeAll(currClique);

        if (candidateHosts.size() == 0) {
            throw new RuntimeException("Could not find a host to assign Vm.");
            // How about resetting host list to all and re-running aco to find the best Host to queue this task in???? (See my work in the assignment...)
            // Maybe we could make the whole startAntOptimization throw an exception, so from there a different aco based queuing scheduling method can be devised????
        }
        List<Double> numerators = new ArrayList<>();
        double denominator = 0.0;

        double denominatorRes = 0.0;
        for (SDNHost candidateHost : candidateHosts) {
            double mipsFreePercent = candidateHost.getAvailableMips() / policy.getHostTotalMips();
            double bwFreePercent = candidateHost.getAvailableBandwidth() / policy.getHostTotalBw();
            candidateHost.setFreeResourcePercentage(policy.convertWeightedMetric(mipsFreePercent, bwFreePercent));
            denominatorRes += 1/(candidateHost.getFreeResourcePercentage()+0.00001);
        }


        // If the size of curr clique is zero should use a diff way to assign probabilities such that a server with highest utilization/lowest power consumption gets selected
        if (currClique.size() == 0) {
            for (int i = 0; i < candidateHosts.size(); i++) {
                SDNHost host = candidateHosts.get(i);
                double probability = (1/host.getFreeResourcePercentage())/ denominatorRes;
                probabilitiesList.put(candidateHosts.get(i), probability);
            }
            return;
        }


        for (SDNHost candidateHost : candidateHosts) {
            double phermoneFactor = 0.0;
            for (SDNHost currHost : currClique) {
                // Pheromone trail (from candidate host to all the hosts in the current clique)
                phermoneFactor += Math.pow(1/(trails[hostList.indexOf(candidateHost)][hostList.indexOf(currHost)] + 0.000001), 0); // to address phermone zero case
            }
            // Multiplying pheromone factor by the heuristic factor which is the inverse of estimated rise in power for a unit of mips to be processed.
            double heuristicFactor = 0.0;
            double totalHops = 0.0;
            double resUtil = 0.0;
            for (SDNHost currHost : currClique) {
                // Distance from candidate host to all the other hosts
                totalHops += (1/(computeMinNetworkHops(new LinkSelectionPolicyBandwidthAllocation(), candidateHost, currHost, 0) + 0.000001));
            }

            resUtil = 1/candidateHost.getFreeResourcePercentage();
            heuristicFactor = 1000 * totalHops + resUtil; //totalHops * resUtil; // Could use addition or multiplication here...
            heuristicFactor = Math.pow(heuristicFactor, 2);

            double numerator = heuristicFactor * phermoneFactor;


            // ***************   //
            // phermoneFactor *= Math.pow((1.0 / candidateHost.getEstimatedRiseInPowerConsumptionForNextPeriod(1000, 50)), gamma);

            // Multiplying pheromone factor by the heuristic factor which is the inverse of free mips (i.e. capacity). So in this approach hosts that have less capacity will be
            // favored for successive allocations as well. This was overall host utilisation would improve...
            //phermoneFactor *= Math.pow((1.0 / candidateHost.getVmScheduler().getAvailableMips()), gamma);
            denominator += numerator;
            if (task.getBlacklist().indexOf(candidateHost) != -1)
                numerators.add(0.0);
            else
                numerators.add(numerator);
        }

        for (int i = 0; i < candidateHosts.size(); i++) {
            double probability = numerators.get(i) / denominator;
            probabilitiesList.put(candidateHosts.get(i), probability);
        }

    }

    /**
     * Update trails that ants used
     */
    private void updateTrails() {
        for (int i = 0; i < numberOfHosts; i++) {
            for (int j = 0; j < numberOfHosts; j++) {
                trails[i][j] *= evaporation;
            }
        }

        // Find the best trail of current iteration
        Ant bestAntInCurrIteration = ants.get(0);
        double bestTourLenInCurrIteration = getSizeOfSolution(ants.get(0).trail);
        for (Ant a : ants) {
            System.out.println("-------------------- solution of ant: " + getSizeOfSolution(a.trail));
            for (Integer hostIndex : a.trail) {
                System.out.print(hostList.get(hostIndex).getId() + " ");
            }
            System.out.println();
            if (getSizeOfSolution(a.trail) > bestTourLenInCurrIteration) {
                bestTourLenInCurrIteration = getSizeOfSolution(a.trail);
                bestAntInCurrIteration = a;
            }
        }

        System.out.println("******************************************************* Best Tour Length of Curr Iteration: " + bestTourLenInCurrIteration);
        double pheromone = 1 / (1 + bestTourLengthOverall - bestTourLenInCurrIteration);

        // Deposit pheromone on each edge that is included in the best solution of the current iteration
        // Update trails: Add this ant's contribution to every edge that connects the hosts selected by this ant. For example if ant selects the hosts 1,4,5 then add contribution to the edges (1,4) (1,5) (4,5)
        for (int i = 0; i < bestAntInCurrIteration.trail.size(); i++) {
            for (int j = i + 1; j < bestAntInCurrIteration.trail.size(); j++) {
                trails[hostList.indexOf(bestAntInCurrIteration.getSelectedHostList().get(i))][hostList.indexOf(bestAntInCurrIteration.getSelectedHostList().get(j))] += pheromone;
            }
        }
    }

    /**
     * Update the best solution
     */
    private void updateBestTrail() {
        if (bestTourOrderOverall == null) {
            bestTourOrderOverall = ants.get(0).trail;
            bestTourLengthOverall = getSizeOfSolution(ants.get(0).trail);
            bestAllocationPlan = ants.get(0).allocMap;
        }

        for (Ant a : ants) {
            if (getSizeOfSolution(a.trail) > bestTourLengthOverall) {
                bestTourOrderOverall = a.trail;
                bestTourLengthOverall = getSizeOfSolution(a.trail);
                bestAllocationPlan = a.allocMap;
            }
        }

    }

    //how to compute the cost of a solution??

    /**
     * Clear trails after simulation
     */
    private double getSizeOfSolution(ArrayList<Integer> trail) {
        double size = 1.0;
        double Q = 500.0;


        for (int i = 0; i < trail.size(); i++) {
            for (int j = i + 1; j < trail.size(); j++) {
                SDNHost src = hostList.get(trail.get(i));
                SDNHost dest = hostList.get(trail.get(j));
                size += computeMinNetworkHops(new LinkSelectionPolicyBandwidthAllocation(), src, dest, 0);
                //size += (src.getTotalMips() * src.getNumberOfPes() + dest.getTotalMips() * dest.getNumberOfPes()) / computeMinNetworkHops(new LinkSelectionPolicyBandwidthAllocation(), src, dest, 0);
            }
        }

        return Q/size;
    }

    /**
     * Clear trails after simulation
     */
    private void clearTrails() {
        IntStream.range(0, numberOfHosts)
                .forEach(i -> {
                    IntStream.range(0, numberOfHosts)
                            .forEach(j -> trails[i][j] = c);
                });
    }

}
