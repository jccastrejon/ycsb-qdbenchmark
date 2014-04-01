/**
 */
package fr.imag.qdbenchmark.dsl;

import java.util.List;

public interface QualitySpecification
{
  String getCharacteristic();

  void setCharacteristic(String value);

  List<String> getSubCharacteristics();

} // QualitySpecification
