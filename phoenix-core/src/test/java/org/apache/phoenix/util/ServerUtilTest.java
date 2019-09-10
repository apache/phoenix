package org.apache.phoenix.util;

import org.apache.hadoop.hbase.client.Admin;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ServerUtilTest {

    String existingNamespaceOne = "existingNamespaceOne";
    String existingNamespaceTwo = "existingNamespaceTwo";
    String nonExistingNamespace = "nonExistingNamespace";

    String[] namespaces = { existingNamespaceOne, existingNamespaceTwo };

    @Test
    public void testIsHbaseNamespaceAvailableWithExistingNamespace() throws Exception {
        Admin mockAdmin = getMockedAdmin();
        assertTrue(ServerUtil.isHbaseNamespaceAvailable(mockAdmin, existingNamespaceOne));
    }

    @Test
    public void testIsHbaseNamespaceAvailableWithNonExistingNamespace() throws Exception{
        Admin mockAdmin = getMockedAdmin();
        assertFalse(ServerUtil.isHbaseNamespaceAvailable(mockAdmin,nonExistingNamespace));
    }

    private Admin getMockedAdmin() throws Exception {
        Admin mockAdmin = Mockito.mock(Admin.class);
        Mockito.when(mockAdmin.listNamespaces()).thenReturn(namespaces);
        return mockAdmin;
    }

}