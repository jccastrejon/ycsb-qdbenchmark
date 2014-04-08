package fr.imag.qdbenchmark.measurements;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.yahoo.ycsb.measurements.OneMeasurement;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * 
 * @author jccastrejon
 * 
 */
public class SetMeasurements extends OneMeasurement {
	Set<Double> _measurements;

	public SetMeasurements(String name, Properties props) {
		super(name);
		_measurements = new HashSet<Double>();
	}

	@Override
	public void reportReturnCode(int code) {
	}

	@Override
	public void measure(double measure) {
		_measurements.add(measure);
	}

	@Override
	public String getSummary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void exportMeasurements(MeasurementsExporter exporter)
			throws IOException {
		for (double measure : _measurements) {
			exporter.write(getName(), "", measure);
		}
	}
}
