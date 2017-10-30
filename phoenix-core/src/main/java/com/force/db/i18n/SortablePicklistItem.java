/* 
 * Copyright (c) 2017, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license. 
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.force.db.i18n;

/**
 * Interface for picklists that are backed in the database, where the value in
 * the DB is not the same as what is displaye din the use 
 * 
 * @author stamm
 * @since 164
 */
public interface SortablePicklistItem {

    /**
     * @return the distinct persistent db value of the picklist item.
     */
    String getDbValue();
    
    
    /**
     * @return the display value of the enum item according to the current user's language setting.
     */
    String getDisplay();
}
