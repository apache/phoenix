# Skip Scan

Phoenix uses Skip Scan for intra-row scanning which allows for [significant performance improvement](performance.html#Skip-Scan) over Range Scan when rows are retrieved based on a given set of keys.

The Skip Scan leverages <code>SEEK_NEXT_USING_HINT</code> of HBase Filter. It stores information about what set of keys/ranges of keys are being searched for in each column. It then takes a key (passed to it during filter evaluation), and figures out if it's in one of the combinations or range or not. If not, it figures out to which next highest key to jump.

Input to the <code>SkipScanFilter</code> is a <code>List&lt;List&lt;KeyRange&gt;&gt;</code> where the top level list represents each column in the row key (i.e. each primary key part), and the inner list represents ORed together byte array boundaries.


Consider the following query:

    SELECT * from T
    WHERE ((KEY1 >='a' AND KEY1 <= 'b') OR (KEY1 > 'c' AND KEY1 <= 'e')) 
    AND KEY2 IN (1, 2)

The <code>List&lt;List&lt;KeyRange&gt;&gt;</code> for <code>SkipScanFilter</code> for the above query would be [ [ [ a - b ], [ d - e ] ], [ 1, 2 ] ] where [ [ a - b ], [ d - e ] ] is the range for <code>KEY1</code> and [ 1, 2 ] keys for <code>KEY2</code>.

The following diagram illustrates graphically how the skip scan is able to jump around the key space:

![Skip Scan Example](http://4.bp.blogspot.com/-SAFH11n_bPY/UYvaWa0P4bI/AAAAAAAAAu4/rKOuKIaMwF4/s1600/SkipScan.png)


