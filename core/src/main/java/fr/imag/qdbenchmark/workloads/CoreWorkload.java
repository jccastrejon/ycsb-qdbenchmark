package fr.imag.qdbenchmark.workloads;

import java.util.Properties;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;

import fr.imag.qdbenchmark.operations.AggregateOperations;
import fr.imag.qdbenchmark.operations.ConnectionOperations;
import fr.imag.qdbenchmark.operations.KeyOperations;
import fr.imag.qdbenchmark.operations.OperationType.OperationGroup;

/**
 * 
 * @author jccastrejon
 * 
 */
public class CoreWorkload extends com.yahoo.ycsb.workloads.CoreWorkload {

	public static final String OPERATIONS_PROPERTY = "operations";

	public static OperationGroup operations;

	@Override
	public void init(final Properties properties) throws WorkloadException {
		super.init(properties);
		operations = OperationGroup.valueOf(properties.getProperty(
				OPERATIONS_PROPERTY, OperationGroup.KeyOperations.toString()));
	}

	@Override
	public boolean doInsert(DB db, Object threadstate) {
		this.doTransactionInsert(db);
		return true;
	}

	@Override
	public boolean doTransaction(DB db, Object threadstate) {
		String op = operationchooser.nextString();

		if (op.compareTo("READ") == 0) {
			doTransactionRead(db);
		} else if (op.compareTo("UPDATE") == 0) {
			doTransactionUpdate(db);
		} else if (op.compareTo("INSERT") == 0) {
			doTransactionInsert(db);
		}
		return true;
	}

	@Override
	public void doTransactionRead(DB db) {
		KeyOperations keyOperations;
		AggregateOperations aggregateOperations;
		ConnectionOperations connectionOperations;

		// Select operation to invoke
		if (OperationGroup.KeyOperations.equals(operations)) {
			keyOperations = (KeyOperations) db;
			keyOperations.read(null);
		} else if (OperationGroup.AggregateOperations.equals(operations)) {
			aggregateOperations = (AggregateOperations) db;
			aggregateOperations.read(null);
		} else if (OperationGroup.ConnectionOperations.equals(operations)) {
			connectionOperations = (ConnectionOperations) db;
			connectionOperations.read(null);
		}
	}

	@Override
	public void doTransactionUpdate(DB db) {
		KeyOperations keyOperations;
		AggregateOperations aggregateOperations;
		ConnectionOperations connectionOperations;

		// Select operation to invoke
		if (OperationGroup.KeyOperations.equals(operations)) {
			keyOperations = (KeyOperations) db;
			keyOperations.update(null, null);
		} else if (OperationGroup.AggregateOperations.equals(operations)) {
			aggregateOperations = (AggregateOperations) db;
			aggregateOperations.update(null, null);
		} else if (OperationGroup.ConnectionOperations.equals(operations)) {
			connectionOperations = (ConnectionOperations) db;
			connectionOperations.update(null, null);
		}
	}

	@Override
	public void doTransactionInsert(DB db) {
		KeyOperations keyOperations;
		AggregateOperations aggregateOperations;
		ConnectionOperations connectionOperations;

		// Select operation to invoke
		if (OperationGroup.KeyOperations.equals(operations)) {
			keyOperations = (KeyOperations) db;
			keyOperations.insert(null, null);
		} else if (OperationGroup.AggregateOperations.equals(operations)) {
			aggregateOperations = (AggregateOperations) db;
			aggregateOperations.insert(null);
		} else if (OperationGroup.ConnectionOperations.equals(operations)) {
			connectionOperations = (ConnectionOperations) db;
			connectionOperations.insert(null);
		}
	}
}
