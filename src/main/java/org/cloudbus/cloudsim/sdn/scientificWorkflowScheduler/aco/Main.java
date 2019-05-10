package org.cloudbus.cloudsim.sdn.scientificWorkflowScheduler.aco;

public class Main {
    public static void main(String[] args) {
        AntColonyOptimization antColony = new AntColonyOptimization(5);
        antColony.startAntOptimization();
    }
}
