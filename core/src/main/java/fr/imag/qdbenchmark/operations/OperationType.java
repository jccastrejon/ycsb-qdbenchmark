package fr.imag.qdbenchmark.operations;

/**
 * 
 * @author jccastrejon
 * 
 */
public class OperationType {
	public enum OperationGroup {
		AggregateOperations, ConnectionOperations, KeyOperations
	}

	public @interface InsertOperation {

	}

	public @interface ReadOperation {

	}

	public @interface UpdateOperation {

	}
}
