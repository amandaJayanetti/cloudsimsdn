/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2017, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.sdn.monitor.power;

import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;

public class PowerUtilizationEnergyModelSwitchActivePort implements PowerUtilizationEnergyModel {
	
	/* based on CARPO: Correlation-Aware Power Optimization in Data Center Networks by Xiaodong Wang et al. */

	private final static double idleWatt = 66.7;
	private final static double workingWattProportional = 1.0;
	private final static double powerOffDuration = 0; //3600 if host is idle for longer than 3600 sec (1hr), it's turned off.

	private double calculatePower(double u) {
		double power = (double)idleWatt + (double)workingWattProportional * u;
		return power;
	}

	@Override
	public double calculateEnergyConsumption(double duration, double numPorts) {
		double power = calculatePower(numPorts);
		double energyConsumption = power * duration;

		// AMANDAAAA is this assumption required for the base case (i.e. we can use this as an improvement in our algorithm so for the base switches aren't turned off) ?????
		// Assume that the host is turned off when duration is long enough
		if(duration > powerOffDuration && numPorts == 0)
			energyConsumption = 0;
				
		return energyConsumption / 3600;
	}

	@Override
	public double computeEnergyConsumption(double duration, double utilization, SDNHost host) {
		return 0;
	}

	@Override
	public double computePerformancePerWatt(double mips, SDNHost host){
		return 0;
	}

	@Override
	public double getMaxPPW(SDNHost host) {
		return 0;
	}

	@Override
	public double computeEnergyConsumptionOfWorkload(double duration, double utilization, SDNHost host) {
		return 0;
	}
}
