/**
 */
package fr.imag.qdbenchmark.dsl;

import java.util.List;

public interface Entity
{
  String getName();

  void setName(String value);

  List<Attribute> getAttributes();

} // Entity
