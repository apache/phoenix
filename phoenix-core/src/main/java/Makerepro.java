import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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

public class Makerepro {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String mainTableName = "main";
        String secondaryTableName = "secondary";
        
        String mainGlobalIndexName = "main_global";
        String mainLocalIndexName = "mainl_local";

        String secondaryGlobalIndexName = "secondary_global";
        String secondaryLocalIndexName = "secondary_local";
        
        String createmain = "CREATE TABLE IF NOT EXISTS %s (ID INTEGER primary key, "+
            "unsig_id UNSIGNED_INT, "+
            "big_id BIGINT, "+
            "unsig_long_id UNSIGNED_LONG, "+
            "tiny_id TINYINT, "+
            "unsig_tiny_id UNSIGNED_TINYINT,"+
            "small_id SMALLINT, "+
            "unsig_small_id UNSIGNED_SMALLINT, "+
            "float_id FLOAT, "+
            "unsig_float_id UNSIGNED_FLOAT, "+
            "double_id DOUBLE, "+
            "unsig_double_id UNSIGNED_DOUBLE, "+
            "decimal_id DECIMAL, "+
            "boolean_id BOOLEAN, "+
            "time_id TIME, "+
            "date_id DATE, "+
            "timestamp_id TIMESTAMP, "+
            "unsig_time_id UNSIGNED_TIME, "+
            "unsig_date_id UNSIGNED_DATE, "+
            "unsig_timestamp_id UNSIGNED_TIMESTAMP, "+
            "varchar_id VARCHAR (30), "+
            "char_id CHAR (30), "+
            "binary_id BINARY (100), "+
            "varbinary_id VARBINARY (100), "+
            "array_id VARCHAR[])";
        
        String createSecondary = 
                "CREATE TABLE IF NOT EXISTS %s (SEC_ID INTEGER primary key," +
                "sec_unsig_id UNSIGNED_INT," +
                "sec_big_id BIGINT," +
                "sec_usnig_long_id UNSIGNED_LONG," +
                "sec_tiny_id TINYINT," +
                "sec_unsig_tiny_id UNSIGNED_TINYINT," +
                "sec_small_id SMALLINT," +
                "sec_unsig_small_id UNSIGNED_SMALLINT," +
                "sec_float_id FLOAT," +
                "sec_unsig_float_id UNSIGNED_FLOAT," +
                "sec_double_id DOUBLE," +
                "sec_unsig_double_id UNSIGNED_DOUBLE," +
                "sec_decimal_id DECIMAL," +
                "sec_boolean_id BOOLEAN," +
                "sec_time_id TIME," +
                "sec_date_id DATE," +
                "sec_timestamp_id TIMESTAMP," +
                "sec_unsig_time_id UNSIGNED_TIME," +
                "sec_unsig_date_id UNSIGNED_DATE," +
                "sec_unsig_timestamp_id UNSIGNED_TIMESTAMP," +
                "sec_varchar_id VARCHAR (30)," +
                "sec_char_id CHAR (30)," +
                "sec_binary_id BINARY (100)," +
                "sec_varbinary_id VARBINARY (100)," +
                "sec_array_id VARCHAR[])";
        
        String createGlobalIndexOnmain = "CREATE INDEX %s ON %s (unsig_id)";
        
        String createLocalIndexOnmain = "CREATE LOCAL INDEX %s ON %s (unsig_small_id)";
        
        String createGlobalIndexOnSecondary = "CREATE INDEX %s ON %s (sec_unsig_id)";
        
        String createLocalIndexOnSecondary = "CREATE LOCAL INDEX %s ON %s (sec_unsig_small_id)";
        
        String upsert = "UPSERT INTO %s values (%s)";
        
        String[] mainValues = {
                "0, 2, 3, 4, -5, 6, 7, 8, 9.3, 10.4, 11.5, 12.6, 13.7, True, CURRENT_TIME(),"+
                "CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(), CURRENT_TIME(),"+
                "'This is random textA', 'a', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='",
                
                "1, 2, 2, 40, -8, 6, 7, 8, 9.3, 10.4, 11.5, 12.6, 13.7, False,"+
                "CURRENT_TIME() - 1, CURRENT_DATE() + 1, CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(),"+
                "CURRENT_TIME(), 'This is random textB', 'b', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='",
                
                "2, 5, -1, 4000000, -9, 1, 6, 8, 9.3, 10.4, 11.5, 12.6, 13.7, True,"+
                "CURRENT_TIME() - 6, CURRENT_DATE() + 10051, CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(),"+
                "CURRENT_TIME(), 'This is random textG', 'g', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='",
                
                "3, null, null, null, null, null, null, null, null, null, null, null, null,"+
                "null, null, null, null, null, null, null, null, null, null, null"
        };
        
        String[] secondaryValues = {
                "0, 2, 3, 4, 5, 6, 7, 8, 9.3, 10.4, 11.5, 12.6, 13.7, True, CURRENT_TIME(),"+
                "CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(), CURRENT_TIME(),"+
                "'This is random text', 'a', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='",
                
                "2, null, null, null, null, null, null, null, null, null, null, null, null,"+
                "null, null, null, null, null, null, null, null, null, null, null",
                
                "3, 5, 3, 4, -5, 6, 7, 8, 9.3, 10.4, 11.5, 12.6, 13.7, True, CURRENT_TIME(),"+
                "CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(), CURRENT_TIME(),"+
                "'This is random textA', 'a', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='"
        };
        
        String query = 
                " SELECT A.ID, B.SEC_UNSIG_ID, C.TINY_ID, D.TIME_ID, E.SEC_TIME_ID, F.SEC_BOOLEAN_ID, G.VARBINARY_ID, H.SEC_UNSIG_ID, I.UNSIG_TIMESTAMP_ID, I.UNSIG_SMALL_ID "+
                " FROM " + mainTableName + " AS A "
              + " INNER JOIN " + secondaryTableName + " AS B ON A.ID = B.SEC_ID "+
                " LEFT OUTER JOIN " + mainTableName + " AS C ON B.SEC_BIG_ID=C.BIG_ID "+
                " LEFT OUTER JOIN " + mainTableName + " AS D ON C.TINY_ID=D.TINY_ID "+
                " INNER JOIN " + secondaryTableName + " AS E ON D.TIME_ID=E.SEC_TIME_ID "+
                " INNER JOIN " + secondaryTableName + " AS F ON D.BOOLEAN_ID = F.SEC_BOOLEAN_ID "+
                " RIGHT OUTER JOIN " + mainTableName + " AS G ON F.SEC_VARBINARY_ID=G.VARBINARY_ID "+
                " RIGHT OUTER JOIN " + secondaryTableName + " AS H ON H.SEC_UNSIG_ID=G.UNSIG_ID "+
                " INNER JOIN " + mainTableName + " AS I ON I.UNSIG_TIMESTAMP_ID=E.SEC_UNSIG_TIMESTAMP_ID "+
                " INNER JOIN " + mainTableName + " AS J ON J.UNSIG_SMALL_ID=F.SEC_UNSIG_SMALL_ID "+
                " WHERE A.ID IS NOT NULL " +
                "      AND I.UNSIG_TIMESTAMP_ID IS NOT NULL "+
                "      AND I.UNSIG_SMALL_ID IS NOT NULL "+
                " ORDER BY A.ID DESC";


            
            System.out.println(String.format(createmain, mainTableName) + ";");
            System.out.println(String.format(createSecondary, secondaryTableName) + ";");
            
            System.out.println(String.format(createGlobalIndexOnmain, mainGlobalIndexName, mainTableName) + ";");
            System.out.println(String.format(createLocalIndexOnmain, mainLocalIndexName, mainTableName) + ";");
            
            System.out.println(String.format(createGlobalIndexOnSecondary, secondaryGlobalIndexName, secondaryTableName) + ";");
            System.out.println(String.format(createLocalIndexOnSecondary, secondaryLocalIndexName, secondaryTableName) + ";");

            System.out.println(String.format(upsert, mainTableName, mainValues[0]) + ";");
            System.out.println(String.format(upsert, mainTableName, mainValues[1]) + ";");
            System.out.println(String.format(upsert, mainTableName, mainValues[2]) + ";");
            System.out.println(String.format(upsert, mainTableName, mainValues[3]) + ";");

            System.out.println(String.format(upsert, secondaryTableName, secondaryValues[0]) + ";");
            System.out.println(String.format(upsert, secondaryTableName, secondaryValues[1]) + ";");
            System.out.println(String.format(upsert, secondaryTableName, secondaryValues[2]) + ";");
            
            System.out.println(query+";");
  
    }

}
