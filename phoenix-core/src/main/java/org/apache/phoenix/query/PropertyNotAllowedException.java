package org.apache.phoenix.query;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Exception thrown when one or more properties that is not allowed by {@link PropertyPolicy}
 */
public class PropertyNotAllowedException extends SQLException {
  private static final long serialVersionUID = 1L;
  private final Properties offendingProperties;

  public PropertyNotAllowedException(Properties offendingProperties){
    this.offendingProperties=offendingProperties;
  }

  public Properties getOffendingProperties(){ return this.offendingProperties; }

}
