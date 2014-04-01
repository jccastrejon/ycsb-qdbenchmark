package fr.imag.qdbenchmark.operations;

import fr.imag.qdbenchmark.dsl.Relationship_;
import fr.imag.qdbenchmark.operations.OperationType.InsertOperation;
import fr.imag.qdbenchmark.operations.OperationType.ReadOperation;
import fr.imag.qdbenchmark.operations.OperationType.UpdateOperation;

/**
 * 
 * @author jccastrejon
 * 
 */
public interface ConnectionOperations {
	/**
	 * 
	 * @param relationship
	 * @return
	 */
	@ReadOperation
	public int read(Relationship_ relationship);

	/**
	 * 
	 * @param relationship
	 * @return
	 */
	@InsertOperation
	public int insert(Relationship_ relationship);

	/**
	 * 
	 * @param oldRelationship
	 * @param newRelationship
	 * @return
	 */
	@UpdateOperation
	public int update(Relationship_ oldRelationship, Relationship_ newRelationship);

	/**
	 * 
	 * @param relationship
	 * @return
	 */
	public int delete(Relationship_ relationship);
}
