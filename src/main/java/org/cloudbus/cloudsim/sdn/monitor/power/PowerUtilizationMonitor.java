/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2017, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.sdn.monitor.power;

import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;

// FYI
public class PowerUtilizationMonitor {
	private double previousTime = 0;

	private double totalEnergy = 0;
	private PowerUtilizationEnergyModel energyModel;

	public PowerUtilizationMonitor(PowerUtilizationEnergyModel model) {
		this.energyModel = model;
	}

	public double addPowerConsumption(double currentTime, double cpuUtilizationOfLastPeriod) {

		double duration = currentTime - previousTime;
		double energyConsumption = energyModel.calculateEnergyConsumption(duration, cpuUtilizationOfLastPeriod);

		totalEnergy += energyConsumption;
		previousTime = currentTime;

		return energyConsumption;
	}

	public double computePowerConsumption(double currentTime, double cpuUtilizationOfLastPeriod, SDNHost host) {

		double duration = currentTime - previousTime;
		double energyConsumption = energyModel.computeEnergyConsumption(duration, cpuUtilizationOfLastPeriod, host);

		totalEnergy += energyConsumption;
		previousTime = currentTime;

		return energyConsumption;
	}

    public double getPerformancePerWatt(double mips, SDNHost host) {
	    return energyModel.computePerformancePerWatt(mips, host);
    }

	public double estimatedPowerConsumptionForPeriod(double duration, double cpuUtilization, SDNHost host) {
		return energyModel.computeEnergyConsumption(duration, cpuUtilization, host);
	}

	public double estimatedPowerConsumptionForWorkload(double duration, double cpuUtilization, SDNHost host) {
		return energyModel.computeEnergyConsumptionOfWorkload(duration, cpuUtilization, host);
	}

	public void addPowerConsumptionDuration(double duration, double cpuUtilizationOfLastPeriod) {
		double energyConsumption = energyModel.calculateEnergyConsumption(duration, cpuUtilizationOfLastPeriod);

		totalEnergy += energyConsumption;
	}


	public double getTotalEnergyConsumed() {
		return totalEnergy;
	}
}
