package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.regex.Pattern;

/**
 * Utility function that constructs a notification filter to match a given db name and/or table name.
 * If dbName == null, fetches all warehouse events.
 * If dnName != null, but tableName == null, fetches all events for the db
 * If dbName != null && tableName != null, fetches all events for the specified table
 */
public class DatabaseAndTableFilter extends BasicFilter {
  private final String tableName;
  private final Pattern dbPattern;

  public DatabaseAndTableFilter(final String databaseNameOrPattern, final String tableName) {
    // we convert the databaseNameOrPattern to lower case because events will have these names in lower case.
    this.dbPattern = (databaseNameOrPattern == null || databaseNameOrPattern.equals("*"))
        ? null
        : Pattern.compile(databaseNameOrPattern, Pattern.CASE_INSENSITIVE);
    this.tableName = tableName;
  }

  @Override
  boolean shouldAccept(final NotificationEvent event) {
    if (dbPattern == null) {
      return true; // if our dbName is null, we're interested in all wh events
    }
    if (dbPattern.matcher(event.getDbName()).matches()) {
      if ((tableName == null)
          // if our dbName is equal, but tableName is blank, we're interested in this db-level event
          || (tableName.equalsIgnoreCase(event.getTableName()))
        // table level event that matches us
          ) {
        return true;
      }
    }
    return false;
  }
}
