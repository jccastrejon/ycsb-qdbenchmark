package fr.imag.qdbenchmark.db;

import fr.imag.qdbenchmark.operations.AggregateOperations;
import fr.imag.qdbenchmark.operations.ConnectionOperations;
import fr.imag.qdbenchmark.operations.KeyOperations;

/**
 * 
 * @author jccastrejon
 * 
 */
public interface RelationalDB extends KeyOperations, AggregateOperations,
		ConnectionOperations {
}
