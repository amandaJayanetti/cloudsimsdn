package org.cloudbus.cloudsim.sdn.workflowscheduler.aco;

import com.google.common.collect.Multimap;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.sdn.physicalcomponents.Link;
import org.cloudbus.cloudsim.sdn.physicalcomponents.Node;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicy;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicyBandwidthAllocation;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.IntStream;

public class AntColonyOptimization {
    private double c = 1.0;
    private double alpha = 1;
    private double beta = 5;
    private double evaporation = 0.5;
    private double Q = 500;
    private double antFactor = 0.8;
    private double randomFactor = 0.01;

    private int maxIterations = 1000;

    private int numberOfCities;
    private int numberOfAnts;
    private double graph[][];
    private double trails[][];
    private List<Ant> ants = new ArrayList<>();
    private Random random = new Random();
    private double probabilities[];
    private List<SDNHost> hostList;
    private List<SDNVm> vmList;

    private int currentIndex;

    private ArrayList<Integer> bestTourOrder;
    private double bestTourLength;
    private Multimap<SDNHost, SDNVm> bestAllocationPlan;

    public AntColonyOptimization(List<SDNHost> hostList, List<SDNVm> vmList, VmAllocationPolicy policy) {
        this.hostList = hostList;
        this.vmList = vmList;
        graph = generateHostMatrix(hostList);
        numberOfCities = graph.length;
        numberOfAnts = (int) (numberOfCities * antFactor);

        trails = new double[numberOfCities][numberOfCities];
        probabilities = new double[numberOfCities];
        IntStream.range(0, numberOfAnts)
                .forEach(i -> ants.add(new Ant(numberOfCities, vmList, hostList, policy)));
    }

    /**
     * Initialize host matrix based on the number of hops between hosts
     */
    public double[][] generateHostMatrix(List<SDNHost> hostList) {
        int numHosts = hostList.size();
        // hostMatrix stores minimum network hops from every host to every other host
        double[][] hostMatrix = new double[numHosts][numHosts];
        LinkSelectionPolicy linkSelector = new LinkSelectionPolicyBandwidthAllocation();
        /*
        hostList.forEach(srcHost -> hostList.forEach(destHost -> {
            computeMinNetworkHops(srcHost, destHost);
        }));
        */

        IntStream.range(0, numHosts).forEach(i -> IntStream.range(0, numHosts).forEach(j -> {
            hostMatrix[i][j] = computeMinNetworkHops(linkSelector, hostList.get(i), hostList.get(j), 0);
        }));

        return hostMatrix;
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
     * Generate initial solution
     */
    public double[][] generateRandomMatrix(int n) {
        double[][] randomMatrix = new double[n][n];
        IntStream.range(0, n)
                .forEach(i -> IntStream.range(0, n)
                        .forEach(j -> randomMatrix[i][j] = Math.abs(random.nextInt(100) + 1)));
        return randomMatrix;
    }

    /**
     * Perform aco optimization
     */
    public Multimap<SDNHost, SDNVm> startAntOptimization() {
        setupAnts();
        clearTrails();
        IntStream.range(0, maxIterations)
                .forEach(i -> {
                    moveAnts();
                    updateTrails();
                    updateBest();
                });
        System.out.println("Best tour length: " + (bestTourLength));
        System.out.println("Best tour order: " + bestTourOrder);
        return bestAllocationPlan;
    }

    /**
     * Prepare ants for the simulation
     */
    private void setupAnts() {
        ants.forEach(ant -> {
            ant.clear();
            // select the first host randomly i.e for a particular ant if random.nextInt(numberOfCities) is 5. The first random host it considers is hostList.get(5).
            // can call this function visitHost
            ant.visitCity(random.nextInt(numberOfCities));
        });
    }

    /**
     * At each iteration, move ants
     */
    private void moveAnts() {
        ants.forEach(ant -> {
            while (ant.unallocatedVmList.size() != ant.allocatedVmList.size()) {
                ant.visitCity(selectNextCity(ant));
            }
        });
    }

    /**
     * Select next city for each aco
     */
    private int selectNextCity(Ant ant) {
        // Allow a small chance for random host selection (If necessary we can remove this random possibility by setting randomFactor to 0.0)
        int t = random.nextInt(numberOfCities);
        if (random.nextDouble() < randomFactor) {
            OptionalInt cityIndex = IntStream.range(0, numberOfCities)
                    .filter(i -> i == t)
                    .findFirst();
            if (cityIndex.isPresent()) {
                return cityIndex.getAsInt();
            }
        }

        // Calculate the probabilities of this Ant selecting any one of the hosts in hostList as the next host from current host.
        calculateProbabilities(ant);

        // After we calculate probabilities, we can decide to which city to go to by using.. ???
        double r = random.nextDouble();
        double total = 0;
        for (int i = 0; i < numberOfCities; i++) {
            total += probabilities[i];
            if (total >= r) {
                return i;
            }
        }

        throw new RuntimeException("There are no other cities");
    }

    /**
     * Calculate the next city picks probabilities
     */
    public void calculateProbabilities(Ant ant) {
        List<SDNHost> currSelectedHostList = new ArrayList<>();

        IntStream.range(0, ant.trail.size()).forEach(i -> {
            currSelectedHostList.add(this.hostList.get(ant.trail.get(i + 1)));
        });

        final double[] pheromone = {0.0};
        // In aco based on clique strategy, next host selection should take into account all hosts in the current host selection as well.
        // Also, we don't have to eliminate already chosen hosts since we can select the same host again and again
        IntStream.range(0, numberOfCities).forEach(i -> IntStream.range(0, currSelectedHostList.size()).forEach(l -> {
            // pheromone[0] is the sum of Phermones of all the hosts
            // ants prefer to follow stronger and shorter trails: the values in trails[l][i] array indicate the degree to which other ants have already used that trail (strength of the trail), hence
            // we take the power of that. The value in graph[l][i] indicates the no of network hops from the current node selection to the city under consideration. So we take the inverse of that.
            if (graph[l][i] != 0) {
                pheromone[0] += Math.pow(trails[l][i], alpha) * Math.pow(1.0 / graph[l][i], beta);
            }
        }));


        double[] numerators = new double[numberOfCities];
        IntStream.range(0, numberOfCities).forEach(k -> IntStream.range(0, currSelectedHostList.size()).forEach(j -> {
            // numerators[k] is the Phermone of the considered host hostList.get(k) computed by considering network hops to it from all the hosts that have already been chosen.
            numerators[k] += Math.pow(trails[j][k], alpha) * Math.pow(1.0 / graph[j][k], beta);
        }));

        for (int j = 0; j < numberOfCities; j++) {
            // probabilities array holds the probabilities of this ant moving to each host in the graph. P(moving to a host) = Phermone of the considered host/Sum of Phermones of all the hosts
            probabilities[j] = numerators[j] / pheromone[0];
        }
    }

    /**
     * Update trails that ants used
     */
    private void updateTrails() {
        for (int i = 0; i < numberOfCities; i++) {
            for (int j = 0; j < numberOfCities; j++) {
                trails[i][j] *= evaporation;
            }
        }
        for (Ant a : ants) {
            double contribution = Q;
            if (a.trailLength(graph) != 0)
                contribution = Q / a.trailLength(graph);
            else {
                //if trail length is zero that means the solution has chosen only one host. In that case contribution is higher
                trails[a.trail.get(0)][a.trail.get(0)] = contribution;
            }

            for (int i = 1; i < a.trail.size(); i++) {
                trails[a.trail.get(i - 1)][a.trail.get(i)] += contribution; // Since contribution is inversely proportional to the trail length, if an aco had travelled a longer length it leaves a comparatively lower contribution on the trail.
            }
        }
    }

    /**
     * Update the best solution
     */
    private void updateBest() {
        if (bestTourOrder == null) {
            bestTourOrder = ants.get(0).trail;
            bestTourLength = ants.get(0).trailLength(graph);
            bestAllocationPlan = ants.get(0).allocMap;
        }
        for (Ant a : ants) {
            if (a.trailLength(graph) < bestTourLength) {
                bestTourLength = a.trailLength(graph);
                bestTourOrder = a.trail;
                bestAllocationPlan = a.allocMap;
            }
        }

    }

    /**
     * Clear trails after simulation
     */
    private void clearTrails() {
        IntStream.range(0, numberOfCities)
                .forEach(i -> {
                    IntStream.range(0, numberOfCities)
                            .forEach(j -> trails[i][j] = c);
                });
    }

}