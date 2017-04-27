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
package org.apache.phoenix.query;

import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.CLOSED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.LOCK_NOT_ACQUIRED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.RENEWED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.THRESHOLD_NOT_REACHED;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.sql.DriverManager;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.phoenix.iterate.RenewLeaseOnlyTableIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServicesImpl.RenewLeaseTask;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class ScannerLeaseRenewalTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testRenewLeaseTaskBehavior() throws Exception {
        // add connection to the queue
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        LinkedBlockingQueue<WeakReference<PhoenixConnection>> connectionsQueue = new LinkedBlockingQueue<>();
        connectionsQueue.add(new WeakReference<PhoenixConnection>(pconn));
        
        // create a scanner and add it to the queue
        int numLeaseRenewals = 4;
        int skipRenewLeaseCount = 2;
        int failToAcquireLockAt = 3;
        RenewLeaseOnlyTableIterator itr = new RenewLeaseOnlyTableIterator(numLeaseRenewals, skipRenewLeaseCount, failToAcquireLockAt, -1);
        LinkedBlockingQueue<WeakReference<TableResultIterator>> scannerQueue = pconn.getScanners();
        scannerQueue.add(new WeakReference<TableResultIterator>(itr));
        
        RenewLeaseTask task = new RenewLeaseTask(connectionsQueue);
        assertTrue(connectionsQueue.size() == 1);
        assertTrue(scannerQueue.size() == 1);
        
        task.run();
        assertTrue(connectionsQueue.size() == 1); 
        assertTrue(scannerQueue.size() == 1); // lease renewed
        assertEquals(RENEWED, itr.getLastRenewLeaseStatus());
        
        task.run();
        assertTrue(scannerQueue.size() == 1);
        assertTrue(connectionsQueue.size() == 1); // renew lease skipped but scanner still in the queue
        assertEquals(THRESHOLD_NOT_REACHED, itr.getLastRenewLeaseStatus());
        
        task.run();
        assertTrue(scannerQueue.size() == 1);
        assertTrue(connectionsQueue.size() == 1);
        assertEquals(LOCK_NOT_ACQUIRED, itr.getLastRenewLeaseStatus()); // lock couldn't be acquired
        
        task.run();
        assertTrue(scannerQueue.size() == 1);
        assertTrue(connectionsQueue.size() == 1);
        assertEquals(RENEWED, itr.getLastRenewLeaseStatus()); // lease renewed
        
        task.run();
        assertTrue(scannerQueue.size() == 0);
        assertTrue(connectionsQueue.size() == 1);
        assertEquals(CLOSED, itr.getLastRenewLeaseStatus()); // scanner closed and removed from the queue
        
        pconn.close();
        task.run();
        assertTrue(scannerQueue.size() == 0);
        assertTrue("Closing the connection should have removed it from the queue", connectionsQueue.size() == 0);
    }
    
    @Test
    public void testRenewLeaseTaskBehaviorOnError() throws Exception {
        // add connection to the queue
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        LinkedBlockingQueue<WeakReference<PhoenixConnection>> connectionsQueue = new LinkedBlockingQueue<>();
        connectionsQueue.add(new WeakReference<PhoenixConnection>(pconn));
        
        // create a scanner and add it to the queue
        int numLeaseRenewals = 4;
        int lockNotAcquiredAt = 1;
        int thresholdNotReachedCount = 2;
        int failLeaseRenewalAt = 3;
        RenewLeaseOnlyTableIterator itr = new RenewLeaseOnlyTableIterator(numLeaseRenewals, thresholdNotReachedCount, lockNotAcquiredAt, failLeaseRenewalAt);
        LinkedBlockingQueue<WeakReference<TableResultIterator>> scannerQueue = pconn.getScanners();
        scannerQueue.add(new WeakReference<TableResultIterator>(itr));
        
        RenewLeaseTask task = new RenewLeaseTask(connectionsQueue);
        assertTrue(connectionsQueue.size() == 1);
        assertTrue(scannerQueue.size() == 1);
        
        task.run();
        assertTrue(connectionsQueue.size() == 1); 
        assertTrue(scannerQueue.size() == 1); // lock not acquired
        assertEquals(LOCK_NOT_ACQUIRED, itr.getLastRenewLeaseStatus());
        
        task.run();
        assertTrue(scannerQueue.size() == 1);
        assertTrue(connectionsQueue.size() == 1); // renew lease skipped but scanner still in the queue
        assertEquals(THRESHOLD_NOT_REACHED, itr.getLastRenewLeaseStatus());
        
        task.run();
        assertTrue(scannerQueue.size() == 0);
        assertTrue(connectionsQueue.size() == 0); // there was only one connection in the connectionsQueue and it wasn't added back because of error
        
        pconn.close();
        task.run();
        assertTrue(scannerQueue.size() == 0);
        assertTrue("Closing the connection should have removed it from the queue", connectionsQueue.size() == 0);
    }
    
}