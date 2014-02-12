/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.map.reduce.util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Class to read configs.
 * 
 */

public class ConfigReader
 {

	private String propertyFile = null;
	private boolean loaded = false;
	private static final Object _synObj = new Object();
	private Map<String, String> properties = new HashMap<String, String>();
	private Exception loadException = null;

	/**
	 * Retrieves singleton config objects from a hashmap of stored objects,
	 * creates these objects if they aren't in the hashmap.
	 */

	public ConfigReader(String propertyFile) {
		this.propertyFile = propertyFile;
	}

	public void load() throws Exception {
		if (loaded) {
			if (loadException != null) {
				throw new Exception(loadException);
			}
			return;
		}
		synchronized (_synObj) {
			if (!loaded) {
				try {
					String tmpFile = propertyFile.trim();
					if (tmpFile.endsWith(".properties")) {
						tmpFile = tmpFile
								.substring(0, tmpFile.lastIndexOf("."));
					}
					ResourceBundle resource = ResourceBundle.getBundle(tmpFile);
					Enumeration<String> enm = resource.getKeys();

					while (enm.hasMoreElements()) {
						String key = enm.nextElement();
						String value = resource.getString(key);
						properties.put(key, value);
					}
				} catch (Exception e) {
					System.err
							.println("Exception while loading the config.properties file :: "
									+ e.getMessage());
					loadException = e;
					loaded = true;
					throw e;
				}
				loaded = true;
			}
		}
	}

	public void addConfig(String key, String value) {
		try {
			load();
		} catch (Exception e) {
			System.err.println("ERROR :: " + e.getMessage());
		}
		properties.put(key, value);
	}

	public boolean hasConfig(String key) {
		try {
			load();
		} catch (Exception e) {
			System.err.println("ERROR :: " + e.getMessage());
		}
		return properties.containsKey(key);
	}

	public String getConfig(String key) throws Exception {
		load();
		return properties.get(key);
	}

	public Map<String, String> getAllConfigMap() throws Exception {
		load();
		return properties;
	}

}
