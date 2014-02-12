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
package org.apache.phoenix.logging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.time.StopWatch;

/**
 * Performance data logging  
 * 
 */
public class PerformanceLog {
	private StopWatch stopWatch = null;	
	private static FileOutputStream fostream = null;

	public PerformanceLog(String startMessage) throws IOException {
		getStopWatch().start();
		instanceLog("START: " + startMessage);
	}
	
	public static void startLog() throws FileNotFoundException {
		getFileOutputStream();
	}
	
	public static void startLog(String fileName) throws FileNotFoundException {
		getFileOutputStream(fileName);
	}

	public static void stopLog() throws FileNotFoundException, IOException {
		getFileOutputStream().close();
		fostream = null;
	}

	public void stopStopWatch() throws IOException {
		getStopWatch().stop();
		instanceLog("STOP");		
	}
	
	public void stopStopWatch(String message) throws IOException {
		getStopWatch().stop();
		instanceLog("STOP: " + message);		
	}
	
	/**
	 * Log a message to persistent storage. Elapsed time since start is added.
	 * @param message
	 * @throws IOException
	 */
	public void instanceLog(String message) throws IOException {
		long elapsedMs = getStopWatch().getTime();
		String displayTime = elapsedMs < 1000 ? elapsedMs + " ms" : elapsedMs / 1000 + " sec";
		message = getDateTime() + " (" + displayTime + ") : " + message + "\n";
		System.out.println(message);
		getFileOutputStream().write(message.getBytes());
	}
	
	public static void log(String message) throws IOException {
		message = getDateTime() + ": " + message + "\n";
		System.out.println(message);
		getFileOutputStream().write(message.getBytes());
	}
	
	private static FileOutputStream getFileOutputStream() throws FileNotFoundException {
		return getFileOutputStream(null);
	}

	private static FileOutputStream getFileOutputStream(String fileName) throws FileNotFoundException {
		if (fostream == null) {
			String folderName = "results";
			File folder = new File(folderName);
			if (!folder.exists()) {
				folder.mkdir();
			}
			String generatedFileName = folderName
					+ "/"
					+ (fileName.endsWith("|") ? fileName.substring(0,
							fileName.length() - 1) : fileName) + ".txt";
			fostream = new FileOutputStream(generatedFileName);
		}

		return fostream;
	}
	
	private StopWatch getStopWatch() {
		if (stopWatch == null) {
			stopWatch = new StopWatch();
		}
		return stopWatch;
	}
	
	private final static String getDateTime() {
	    DateFormat df = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
	    return df.format(new Date());
	}
}
