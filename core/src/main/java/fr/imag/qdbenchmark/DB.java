package fr.imag.qdbenchmark;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;

/**
 * 
 * @author jccastrejon
 * 
 */
public class DB extends com.yahoo.ycsb.DB {

	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		throw new RuntimeException("This method shouldn't be invoked in a QDBenchmark workload...");
	}

	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		throw new RuntimeException("This method shouldn't be invoked in a QDBenchmark workload...");
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
		throw new RuntimeException("This method shouldn't be invoked in a QDBenchmark workload...");
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		throw new RuntimeException("This method shouldn't be invoked in a QDBenchmark workload...");
	}

	@Override
	public int delete(String table, String key) {
		throw new RuntimeException("This method shouldn't be invoked in a QDBenchmark workload...");
	}
}
