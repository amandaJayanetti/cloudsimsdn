/*
 * Title:        CloudSimSDN
 * Description:  SDN extension for CloudSim
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2017, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.sdn.monitor.power;

import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;

public interface PowerUtilizationEnergyModel {
	public abstract double calculateEnergyConsumption(double duration, double utilization);
	public abstract double computeEnergyConsumption(double duration, double utilization, SDNHost host);
	public abstract double computeEnergyConsumptionOfWorkload(double duration, double utilization, SDNHost host);
	public abstract double computePerformancePerWatt(double mips, SDNHost host);
	public abstract double getMaxPPW(SDNHost host);
}
