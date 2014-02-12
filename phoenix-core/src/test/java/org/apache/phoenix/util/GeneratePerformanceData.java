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
package org.apache.phoenix.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

public class GeneratePerformanceData {
    private static final String FILENAME = "data.csv";

    public static void main(String[] args) throws FileNotFoundException, IOException {
        String[] host = {"NA","CS","EU"};
        String[] domain = {"Salesforce.com","Apple.com","Google.com"};
        String[] feature = {"Login","Report","Dashboard"};
        Calendar now = GregorianCalendar.getInstance();
        FileOutputStream fostream = new FileOutputStream(FILENAME);
        try {
            Random random = new Random();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (args.length < 1) {
                System.out.println("Row count must be specified as argument");
                return;
            }
            int rowCount = Integer.parseInt(args[0]);
            for (int i=0; i<rowCount; i++) {
                now.add(Calendar.SECOND, 1);
                fostream.write((host[random.nextInt(host.length)] + "," + 
                        domain[random.nextInt(domain.length)] + "," + 
                        feature[random.nextInt(feature.length)] + "," + 
                        sdf.format(now.getTime()) + "," + 
                        random.nextInt(500) + "," + 
                        random.nextInt(2000)+"," + 
                        random.nextInt(10000) + 
                        "\n").getBytes());
                if (i % 10000 == 0) {
                    System.out.print(".");
                }
            }
        } finally {
            fostream.close();
        }
    }
}
