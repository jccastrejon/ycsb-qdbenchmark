package fr.imag.qdbenchmark.db;

import fr.imag.qdbenchmark.DB;
import fr.imag.qdbenchmark.operations.AggregateOperations;
import fr.imag.qdbenchmark.operations.KeyOperations;

/**
 * 
 * @author jccastrejon
 * 
 */
public abstract class ColumnFamilyDB extends DB implements KeyOperations, AggregateOperations {
}
