package fr.imag.qdbenchmark.db;

import fr.imag.qdbenchmark.dsl.Attribute;
import fr.imag.qdbenchmark.dsl.Struct_;

/**
 * 
 * @author jccastrejon
 * 
 */
public class MongoDbClient extends DocumentDB {

	@Override
	public int read(Attribute key) {
		System.out.println("read:key");
		return 0;
	}

	@Override
	public int insert(Attribute key, Struct_ value) {
		System.out.println("insert:key");
		return 0;
	}

	@Override
	public int update(Attribute key, Struct_ value) {
		System.out.println("update:key");
		return 0;
	}

	@Override
	public int read(Struct_ pattern) {
		System.out.println("read:aggregate");
		return 0;
	}

	@Override
	public int insert(Struct_ value) {
		System.out.println("insert:aggregate");
		return 0;
	}

	@Override
	public int update(Struct_ pattern, Struct_ value) {
		System.out.println("update:aggregate");
		return 0;
	}

	@Override
	public int delete(Struct_ pattern) {
		System.out.println("delete:aggregate");
		return 0;
	}
}
