package fr.imag.qdbenchmark;

import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.Sigar;

import com.yahoo.ycsb.measurements.Measurements;

import fr.imag.qdbenchmark.dsl.Attribute;
import fr.imag.qdbenchmark.dsl.Relationship_;
import fr.imag.qdbenchmark.dsl.Struct_;
import fr.imag.qdbenchmark.measurements.SetMeasurements;
import fr.imag.qdbenchmark.operations.AggregateOperations;
import fr.imag.qdbenchmark.operations.ConnectionOperations;
import fr.imag.qdbenchmark.operations.KeyOperations;

/**
 * 
 * @author jccastrejon
 * 
 */
public class DBWrapper extends com.yahoo.ycsb.DBWrapper implements
		KeyOperations, AggregateOperations, ConnectionOperations {

	private static Sigar sigar = new Sigar();

	public DBWrapper(com.yahoo.ycsb.DB db) {
		super(db);
		_measurements.data.put("MEMORY_USED", new SetMeasurements(
				"MEMORY_USED", _measurements._props));
		_measurements.data.put("CPU_TOTAL", new SetMeasurements("CPU_TOTAL",
				_measurements._props));
		_measurements.data.put("DISK_USAGE", new SetMeasurements("DISK_USAGE",
				_measurements._props));
	}

	@Override
	public int read(Relationship_ relationship) {
		long st = System.nanoTime();
		int res = ((ConnectionOperations) _db).read(relationship);
		long en = System.nanoTime();
		_measurements.measure("READ", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("READ", res);
		return res;
	}

	@Override
	public int insert(Relationship_ relationship) {
		long st = System.nanoTime();
		int res = ((ConnectionOperations) _db).insert(relationship);
		long en = System.nanoTime();
		_measurements.measure("INSERT", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("INSERT", res);
		return res;
	}

	@Override
	public int update(Relationship_ oldRelationship,
			Relationship_ newRelationship) {
		long st = System.nanoTime();
		int res = ((ConnectionOperations) _db).update(oldRelationship,
				newRelationship);
		long en = System.nanoTime();
		_measurements.measure("UPDATE", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("UPDATE", res);
		return res;
	}

	@Override
	public int delete(Relationship_ relationship) {
		long st = System.nanoTime();
		int res = ((ConnectionOperations) _db).delete(relationship);
		long en = System.nanoTime();
		_measurements.measure("DELETE", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("DELETE", res);
		return res;
	}

	@Override
	public int read(Struct_ pattern) {
		long st = System.nanoTime();
		int res = ((AggregateOperations) _db).read(pattern);
		long en = System.nanoTime();
		_measurements.measure("READ", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("READ", res);
		return res;
	}

	@Override
	public int insert(Struct_ value) {
		long st = System.nanoTime();
		int res = ((AggregateOperations) _db).insert(value);
		long en = System.nanoTime();
		_measurements.measure("INSERT", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("INSERT", res);
		return res;
	}

	@Override
	public int update(Struct_ pattern, Struct_ value) {
		long st = System.nanoTime();
		int res = ((AggregateOperations) _db).update(pattern, value);
		long en = System.nanoTime();
		_measurements.measure("UPDATE", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("UPDATE", res);
		return res;
	}

	@Override
	public int delete(Struct_ pattern) {
		long st = System.nanoTime();
		int res = ((AggregateOperations) _db).delete(pattern);
		long en = System.nanoTime();
		_measurements.measure("DELETE", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("DELETE", res);
		return res;
	}

	@Override
	public int read(Attribute key) {
		long st = System.nanoTime();
		int res = ((KeyOperations) _db).read(key);
		long en = System.nanoTime();
		_measurements.measure("READ", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("READ", res);
		return res;
	}

	@Override
	public int insert(Attribute key, Struct_ value) {
		long st = System.nanoTime();
		int res = ((KeyOperations) _db).insert(key, value);
		long en = System.nanoTime();
		_measurements.measure("INSERT", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("INSERT", res);
		return res;
	}

	@Override
	public int update(Attribute key, Struct_ value) {
		long st = System.nanoTime();
		int res = ((KeyOperations) _db).update(key, value);
		long en = System.nanoTime();
		_measurements.measure("UPDATE", (int) ((en - st) / 1000));
		measureResourceUtilisation(_measurements);
		_measurements.reportReturnCode("UPDATE", res);
		return res;
	}

	/**
	 * 
	 * @return
	 */
	private static void measureResourceUtilisation(Measurements measurements) {
		long diskUsage;

		try {
			measurements.measure("MEMORY_USED",
					sigar.getMem().getActualUsed() / 1024 / 1024);
			measurements.measure("CPU_TOTAL",
					sigar.getCpu().getTotal() / 1024 / 1024);

			diskUsage = 0;
			for (FileSystem fileSystem : sigar.getFileSystemList()) {
				diskUsage += sigar.getFileSystemUsage(fileSystem.getDirName())
						.getTotal();
			}
			measurements.measure("DISK_USAGE", diskUsage / 1024 / 1024);
		} catch (Exception e) {
		}
	}
}
