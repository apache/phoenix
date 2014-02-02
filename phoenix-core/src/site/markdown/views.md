# Views
The standard SQL view syntax (with some limitations) is now supported by Phoenix to enable multiple virtual tables to all share the same underlying physical HBase table. This is especially important in HBase, as you cannot realistically expect to have more than perhaps up to a hundred physical tables and continue to get reasonable performance from HBase.

For example, given the following table definition that defines a base table to collect product metrics:

    CREATE  TABLE product_metrics (
        metric_type CHAR(1),
        created_by VARCHAR, 
        created_date DATE, 
        metric_id INTEGER
        CONSTRAINT pk PRIMARY KEY (metric_type, created_by, created_date, metric_id));

You may define the following view:

    CREATE VIEW mobile_product_metrics (carrier VARCHAR, dropped_calls BIGINT) AS
    SELECT * FROM product_metrics
    WHERE metric_type = 'm';
In this case, the same underlying physical HBase table (i.e. PRODUCT_METRICS) stores all of the data.
Notice that unlike with standard SQL views, you may define additional columns for your view. The view inherits all of the columns from its base table, in addition to being able to optionally add new KeyValue columns. You may also add these columns after-the-fact with an ALTER TABLE statement. 

## Updatable Views
If your view uses only simple equality expressions in the WHERE clause, you are also allowed to issue DML against the view. These views are termed *updatable views*. For example, in this case you could issue the following UPSERT statement:

    UPSERT INTO mobile_product_metrics(created_by, create_date, metric_id, carrier, dropped_calls)
    VALUES('John Doe', CURRENT_DATE(), NEXT VALUE FOR metric_seq, 'Verizon', 20);

In this case, the row will be stored in the PRODUCT_METRICS HBase table and the metric_type column value will be inferred to be 'm' since the VIEW defines it as such.

Also, queries done through the view will automatically apply the WHERE clause filter. For example:

    SELECT sum(dropped_calls) FROM mobile_product_metrics WHERE carrier='Verizon'

This would sum all the dropped_calls across all product_metrics with a metric_type of 'm' and a carrier of 'Verizon'.

## Read-only Views
Views may also be defined with more complex WHERE clauses, but in that case you cannot issue DML against them as you'll get a ReadOnlyException. You are still allowed to query through them and their WHERE clauses will be in effect as with standard SQL views. 

As expected, you may create a VIEW on another VIEW as well to further filter the data set. The same rules as above apply: if only simple equality expressions are used in the VIEW and its parent VIEW(s), the new view is updatable as well, otherwise it's read-only.

Note that the previous support for creating a read-only VIEW directly over an HBase table is still supported.

## Indexes on Views
In addition, you may create an INDEX over a VIEW, just as with a TABLE. This is particularly useful to improve query performance over newly added columns on a VIEW, since it provides a way of doing point lookups based on these column values.

## Limitations
In our Phoenix 3.0 release, views have the following restrictions:

1. The primary key constraint may not be changed by a VIEW.
2. A TABLE that has a VIEW may not be dropped, but instead all VIEWs must be dropped first. In the future, we may support the concept of a CASCADE delete.
2. Single-table only - you may not create a VIEW over multiple, joined tables. This will be supported in a future release.
3. All columns must be projected into a VIEW (i.e. only the CREATE VIEW ... AS SELECT * syntax is supported). Note, however, you may drop non primary key columns inherited from the base table in a VIEW after it is created through the ALTER TABLE command. Providing a subset of columns and or expressions in the SELECT clause will be supported in a future release.

