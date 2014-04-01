/**
 */
package fr.imag.qdbenchmark.dsl;

import java.util.List;

public interface Set_ extends Entity
{
  String getDataModel();

  void setDataModel(String value);

  List<Entity> getEntities();

  List<QualitySpecification> getQualitySpecifications();

} // Set_
