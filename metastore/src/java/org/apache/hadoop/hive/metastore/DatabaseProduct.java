package org.apache.hadoop.hive.metastore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum DatabaseProduct {
  DERBY, MYSQL, POSTGRES, ORACLE, SQLSERVER;

  private static Logger LOG = LoggerFactory.getLogger(DatabaseProduct.class);

  /**
   * Determine the database product type
   *
   * @return database product type
   */
  public static DatabaseProduct determineDatabaseProduct(String s) {
    switch (s) {
    case "Apache Derby":
      return DatabaseProduct.DERBY;
    case "Microsoft SQL Server":
      return DatabaseProduct.SQLSERVER;
    case "MySQL":
      return DatabaseProduct.MYSQL;
    case "Oracle":
      return DatabaseProduct.ORACLE;
    case "PostgreSQL":
      return DatabaseProduct.POSTGRES;
    default:
      String msg = "Unrecognized database product name <" + s + ">";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
  }
}
