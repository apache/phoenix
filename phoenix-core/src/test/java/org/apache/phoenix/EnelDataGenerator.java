/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

public class EnelDataGenerator {

    long pod = 0;
    long magnitude = 0;
    long recId = 0;
    long uuid = 0;

    public static void main(String[] args) throws IOException {
        EnelDataGenerator generator = new EnelDataGenerator();
        generator.generate(args);
    }

    private void generate(String[] args) throws IOException {
        String fileName = args[0];
        int recordCnt = Integer.parseInt(args[1]);
        try (BufferedWriter out = new BufferedWriter(new FileWriter(fileName));) {
            for (int c = 0; c < recordCnt; c++) {
                out.write(generateRecord());
            }
        }
    }

    private String generateRecord() throws IOException {
        StringBuffer b = new StringBuffer();
        b.append("\"").append("202304").append("\",") // MEAS_YM CHAR(6) NOT NULL,
                .append("\"").append(Long.toString(pod++)).append("\",") // POD VARCHAR(22) NOT NULL,
                .append("\"AA\",") // MEAS_D CHAR(2) NOT NULL,
                .append("\"").append(Long.toString(magnitude++)).append("\",") // MAGNITUDE  VARCHAR(12) NOT  NULL,
                .append("\"123456789123456789123456789123456789").append(Long.toString(recId++)).append("\",") // REC_ID VARCHAR(64) NOT NULL,
                .append("\"20230505132334\",") // CREATION_DT CHAR(14),
                .append("\"2023-05-05 11:13:23\",") // MEAS_TS TIMESTAMP,
                .append("\"2023-05-05 12:13:23\",") // CREATION_TS TIMESTAMP,
                .append(String.format("\"202304-78-%d\",", uuid++)) // UUID VARCHAR(120),
                .append("\"").append(UUID.randomUUID()).append("\",") // COMP_UUID VARCHAR(120),
                .append("\"2023-05-05 13:13:23\",") // READ_TS  TIMESTAMP,
                .append("\"MANUFACTURERERRRRRRR\",") // MANUFACT VARCHAR(50),
                .append("\"MODEL\",") // MODEL VARCHAR(20),
                .append("\"SERIAL_12345678912345678\",") // SERIAL_N VARCHAR(100),
                .append("\"RTYPE\",") // READ_TYPE VARCHAR(20),
                .append(
                    "\"SLOT_VAL12345678901234567890123456789012345678901234567890123456789012345678900123456789012345678901234567890123456789\",") // SLOT_VAL  VARCHAR(8192),
                .append("\"MDMSTATUS\",") // MDM_STATUS VARCHAR(10),
                .append(
                    "\"MDMPROD12345678901234567890123456789012345678901234567890123456789012345678900123456789012345678901234567890123456789\",")// MDM_PROD  VARCHAR(8192),
                .append(
                    "\"MDMCKSTATUS12345678901234567890123456789012345678901234567890123456789012345678900123456789012345678901234567890123456789\",")// MDM_CK_STATUS  VARCHAR(8192),
                .append("\"WORKID\",")// WORK_ID VARCHAR(100),
                .append(
                    "\"WORK12345678901234567890123456789012345678901234567890123456789012345678900123456789012345678901234567890123456789\",") // WORK  VARCHAR(8192),
                .append("\n");
        return b.toString();
    }

}
