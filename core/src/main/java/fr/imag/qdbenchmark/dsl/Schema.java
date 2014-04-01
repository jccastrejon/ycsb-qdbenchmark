/**
 */
package fr.imag.qdbenchmark.dsl;

import java.util.List;

public interface Schema
{
  String getName();

  void setName(String value);

  List<Attribute> getAttributes();

  List<Entity> getEntities();

} // Schema
