package fr.imag.qdbenchmark.operations;

import fr.imag.qdbenchmark.dsl.Struct_;
import fr.imag.qdbenchmark.operations.OperationType.InsertOperation;
import fr.imag.qdbenchmark.operations.OperationType.ReadOperation;
import fr.imag.qdbenchmark.operations.OperationType.UpdateOperation;

/**
 * 
 * @author jccastrejon
 * 
 */
public interface AggregateOperations {

	/**
	 * 
	 * @param pattern
	 * @return
	 */
	@ReadOperation
	public int read(Struct_ pattern);

	/**
	 * 
	 * @param value
	 * @return
	 */
	@InsertOperation
	public int insert(Struct_ value);

	/**
	 * 
	 * @param pattern
	 * @param value
	 * @return
	 */
	@UpdateOperation
	public int update(Struct_ pattern, Struct_ value);

	/**
	 * 
	 * @param pattern
	 * @return
	 */
	public int delete(Struct_ pattern);
}
