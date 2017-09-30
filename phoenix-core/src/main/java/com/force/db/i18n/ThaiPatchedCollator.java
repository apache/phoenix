/* 
 * Copyright (c) 2017, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license. 
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.force.db.i18n;

import java.text.CollationKey;
import java.text.Collator;

/**
 * Patched java thai collator to prevent it from doing infinite loops / OOMs.
 * (see bug https://gus.soma.salesforce.com/a0790000000CeOH
 * and http://bugs.sun.com/view_bug.do?bug_id=5047314)
 * Delegates to original Collator.
 * This is awkward as Collator is an abstract class and not an interface.
 * (inheritance from RuleBasedCollator would be even worse though)
 *
 *
 * @author glestum
 * @since 162
 * @deprecated This was fixed in JDK7_84
 */
@Deprecated 
public final class ThaiPatchedCollator extends Collator {
    public static final char TRAILING_PHANTOM_CHAR = ' ';
    private final Collator originalCollator;

    ThaiPatchedCollator(Collator originalCollator) {
        this.originalCollator = originalCollator;
    }

    @Override
    public int compare(String source, String target) {
        return this.originalCollator.compare(source, target);
    }

    public Collator getOriginalCollator() {
        return originalCollator;
    }

    public static boolean isThaiCharacterOfDeath(char ch) {
        return ((ch & 0xfff0) == 0x0e40 && (ch & 0x000f) <= 4);
    }

    @Override
    public CollationKey getCollationKey(String source) {
        String patchedSource = source;
        if (source != null) {
            int srcLength = source.length();
            if (srcLength > 0) {
                char lastChar = source.charAt(srcLength-1);
                if (isThaiCharacterOfDeath(lastChar)) {
                    patchedSource = source + TRAILING_PHANTOM_CHAR;
                }
            }
        }
        return this.originalCollator.getCollationKey(patchedSource);
    }

    @Override
    public Object clone() {
        return new ThaiPatchedCollator((Collator)this.originalCollator.clone());
    }

    @Override
    public int hashCode() {
        return this.originalCollator.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) return true;
        if (that == null) return false;
        if (getClass() != that.getClass()) return false;
        ThaiPatchedCollator other = (ThaiPatchedCollator) that;
        return (this.originalCollator.equals(other.originalCollator));
    }

    @Override
    public synchronized int getDecomposition() {
        return this.originalCollator.getDecomposition();
    }

    @Override
    public synchronized void setDecomposition(int decompositionMode) {
        this.originalCollator.setDecomposition(decompositionMode);
    }

    @Override
    public synchronized int getStrength() {
        return this.originalCollator.getStrength();
    }
    @Override
    public synchronized void setStrength(int newStrength) {
        this.originalCollator.setStrength(newStrength);
    }

}
