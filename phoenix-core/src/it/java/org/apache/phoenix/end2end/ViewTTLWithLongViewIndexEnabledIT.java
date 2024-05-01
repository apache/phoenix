package org.apache.phoenix.end2end;

import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class ViewTTLWithLongViewIndexEnabledIT extends BaseViewTTLIT {

    @BeforeClass
    public static final void doSetup() throws Exception {
        // Turn on the TTL feature
        Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
            put(QueryServices.PHOENIX_TABLE_TTL_ENABLED, String.valueOf(true));
            put(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(true));
            // no max lookback
            put(BaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(0));
            put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(true));
            put(QueryServices.PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT, String.valueOf(1));
        }};

        setUpTestDriver(new ReadOnlyProps(ReadOnlyProps.EMPTY_PROPS,
                DEFAULT_PROPERTIES.entrySet().iterator()));
    }

    @Test
    public void testMajorCompactFromMultipleGlobalIndexes() throws Exception {
        super.testMajorCompactFromMultipleGlobalIndexes();
    }

    @Test
    public void testMajorCompactFromMultipleTenantIndexes() throws Exception {
        super.testMajorCompactFromMultipleTenantIndexes();
    }
    @Test
    public void testMajorCompactWithOnlyTenantView() throws Exception {
        super.testMajorCompactWithOnlyTenantView();
    }
    @Test
    public void testMajorCompactWithSaltedIndexedBaseTables() throws Exception {
        super.testMajorCompactWithSaltedIndexedBaseTables();
    }
    @Test
    public void testMajorCompactWithSaltedIndexedTenantView() throws Exception {
        super.testMajorCompactWithSaltedIndexedTenantView();
    }

    @Test
    public void testMajorCompactWithVariousViewsAndOptions() throws Exception {
        super.testMajorCompactWithVariousViewsAndOptions();
    }
    @Test
    public void testMajorCompactWithVariousTenantIdTypesAndRegions() throws Exception {
        super.testMajorCompactWithVariousTenantIdTypesAndRegions(PVarchar.INSTANCE);
    }
    @Test
    public void testMajorCompactWhenTTLSetForSomeTenants() throws Exception {
        super.testMajorCompactWhenTTLSetForSomeTenants();
    }
    @Test
    public void testTenantViewsWIthOverlappingRowPrefixes() throws Exception {
        super.testTenantViewsWIthOverlappingRowPrefixes();
    }

}
