package edu.gmu.stc.hibernate;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.naming.PhysicalNamingStrategy;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class DocumentNameStrategy implements PhysicalNamingStrategy {
  private String tableName = "";

  public DocumentNameStrategy(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public Identifier toPhysicalCatalogName(Identifier name, JdbcEnvironment jdbcEnvironment) {
    return name;
  }

  @Override
  public Identifier toPhysicalSchemaName(Identifier name, JdbcEnvironment jdbcEnvironment) {
    return name;
  }

  @Override
  public Identifier toPhysicalTableName(Identifier name, JdbcEnvironment jdbcEnvironment) {
    Identifier identifier = new Identifier(this.tableName, false);
    return identifier;
  }

  @Override
  public Identifier toPhysicalSequenceName(Identifier name, JdbcEnvironment jdbcEnvironment) {
    return null;
  }

  @Override
  public Identifier toPhysicalColumnName(Identifier name, JdbcEnvironment jdbcEnvironment) {
    return null;
  }
}
