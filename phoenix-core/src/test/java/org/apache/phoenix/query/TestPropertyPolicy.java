/*
 * Copyright, 1999-2013, SALESFORCE.com
 * All Rights Reserved
 * Company Confidential
 */
package org.apache.phoenix.query;

import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Configuration factory that populates an {@link Configuration} with static and runtime
 * configuration settings.
 */
public class TestPropertyPolicy implements PropertyPolicy {
  final static Set<String> propertiesKeyDisAllowed = Collections.unmodifiableSet(
      new HashSet<>(asList("DisallowedProperty")));

  @Override public void evaluate(Properties properties) throws PropertyNotAllowedException {
    final Properties offendingProperties = new Properties();

    for(Object k:properties.keySet()){
      if (propertiesKeyDisAllowed.contains(k)) offendingProperties.put((String)k,properties.getProperty((String)k));
    }

    if (offendingProperties.size()>0) throw new PropertyNotAllowedException(offendingProperties);
  }
}