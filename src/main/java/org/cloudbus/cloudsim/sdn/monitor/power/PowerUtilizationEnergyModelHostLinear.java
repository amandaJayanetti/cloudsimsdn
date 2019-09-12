/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2017, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.sdn.monitor.power;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;

public class PowerUtilizationEnergyModelHostLinear implements PowerUtilizationEnergyModel {
	
	private final static int idleWatt = 120;   // AMANDAAAAA change this back to 120
	private final static int workingWattProportional = 154;
	private final static double powerOffDuration = 3600; //3600 if host is idle for longer than 3600 sec (1hr), it's turned off.

	private double calculatePower(double u) {
		double power = (double)idleWatt + (double)workingWattProportional * u;
		return power;
	}

	private double calculateInstancePower(double u, SDNHost host) {
		String type = host.getType();
		double power = 0.0;
		switch(type) {
			case "Commodity":
				power = 100 + 200 * u;
				break;
			case "HPC":
				power = 150 + 500 * u;
				break;
			case "Micro":
				power = 3 + 6 * u;
				break;
			default:
				power = (double) idleWatt + (double) workingWattProportional * u;
		}
		return power;
	}

	@Override
	public double calculateEnergyConsumption(double duration, double utilization) {
		double power = calculatePower(utilization);
		double energyConsumption = power * duration;
		
		// Assume that the host is turned off when duration is long enough
		if(duration > powerOffDuration && utilization == 0)
			energyConsumption = 0;

		return energyConsumption / 3600;
	}

	@Override
	public double getMaxPPW(SDNHost host) {
		// Max PPW = max MIPS/max power --- at max utilization u is 1
		String type = host.getType();
		double u = 1;
		double power = 0.0;
		switch(type) {
			case "Commodity":
				power = 100 + 200 * u;
				break;
			case "HPC":
				power = 150 + 500 * u;
				break;
			case "Micro":
				power = 3 + 6 * u;
				break;
			default:
				power = (double) idleWatt + (double) workingWattProportional * u;
		}
		return host.getTotalMips()/power;
	}

	@Override
	public double computeEnergyConsumption(double duration, double utilization, SDNHost host) {
		double power = calculateInstancePower(utilization, host);
		double energyConsumption = power * duration;

		// Assume that the host is turned off when duration is long enough
		if(duration > powerOffDuration && utilization == 0)
			energyConsumption = 0;

		if (!host.isActive()) {
			energyConsumption = 0;
		}

		//if (CloudSim.clock() > 3100)
		//	energyConsumption = 0;

		return energyConsumption / 3600;
	}

	@Override
	public double computeEnergyConsumptionOfWorkload(double duration, double utilization, SDNHost host) {
		double power = calculateInstancePower(utilization, host);
		double energyConsumption = power * duration;

		// Assume that the host is turned off when duration is long enough
		if(duration > powerOffDuration && utilization == 0)
			energyConsumption = 0;

		return energyConsumption / 3600;
	}

	@Override
	public double computePerformancePerWatt(double mips, SDNHost host) {
		// Max PPW = max MIPS/max power --- at max utilization u is 1
		String type = host.getType();
		double u = mips/host.getTotalMips();
		double power = 0.0;
		switch(type) {
			case "Commodity":
				power = 100 + 200 * u;
				break;
			case "HPC":
				power = 150 + 500 * u;
				break;
			case "Micro":
				power = 3 + 6 * u;
				break;
			default:
				power = (double) idleWatt + (double) workingWattProportional * u;
		}
		return mips/power;
		/*
		String type = host.getType();
		double ppw = 0.0;

		// ppw = mips/(idleWatt + working watt * utilization)
		switch(type) {
			case "Commodity":
				ppw = mips/(100 + 72 * (mips/host.getPeList().get(0).getMips()/ Consts.MILLION));
				break;
			case "HPC":
				ppw = mips/(100 + 154 * (mips/host.getPeList().get(0).getMips()/ Consts.MILLION));
				break;
			case "Micro":
				ppw = mips/(10 + 5 * (mips/host.getPeList().get(0).getMips()/ Consts.MILLION));
				break;
			default:
				ppw = mips/(idleWatt + workingWattProportional * (mips/host.getPeList().get(0).getMips()/ Consts.MILLION));
		}
		return ppw;
		*/
	}
}
