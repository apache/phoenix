# Sequences

Sequences are a standard SQL feature that allow for generating monotonically increasing numbers typically used to form an ID. To create a sequence, use the following command:

    CREATE SEQUENCE my_schema.my_sequence;

This will start the sequence from 1, increment by 1 each time, and cache on your session the default number of sequence values based on the phoenix.sequence.cacheSize config parameter. The specification of a sequence schema is optional. Caching sequence values on your session improves performance, as we don't need to ask the server for more sequence values until we run out of cached values. The tradeoff is that you may end up with gaps in your sequence values when other sessions also use the same sequence. 

All of these parameters can be overridden when the sequence is created like this:

    CREATE SEQUENCE my_schema.my_sequence START WITH 100 INCREMENT BY 2 CACHE 50;

Sequences are incremented using the NEXT VALUE FOR <sequence_name> expression in an UPSERT VALUES, UPSERT SELECT, or SELECT statement as shown below:

    UPSERT VALUES INTO my_table(id, col1, col2) 
    VALUES( NEXT VALUE FOR my_schema.my_sequence, 'foo', 'bar');

This will allocate a BIGINT based on the next value from the sequence (beginning with the START WITH value and incrementing from there based on the INCREMENT BY amount).

When used in an UPSERT SELECT or SELECT statement, each row returned by the statement would have a unique value associated with it. For example:

    UPSERT INTO my_table(id, col1, col2) 
    SELECT NEXT VALUE FOR my_schema.my_sequence, 'foo', 'bar' FROM my_other_table;

would allocate a new sequence value for each row returned from the SELECT expression. A sequence is only increment once for a given statement, so multiple references to the same sequence by NEXT VALUE FOR produce the same value. For example, in the following statement, my_table.col1 and my_table.col2 would end up with the same value:

    UPSERT VALUES INTO my_table(col1, col2) 
    VALUES( NEXT VALUE FOR my_schema.my_sequence, NEXT VALUE FOR my_schema.my_sequence);

You may also access the last sequence value allocated using a CURRENT VALUE FOR expression as shown below:

    SELECT CURRENT VALUE FOR my_schema.my_sequence, col1, col2 FROM my_table;

This would evaluate to the last sequence value allocated from the previous NEXT VALUE FOR expression for your session (i.e. all connections on the same JVM for the same client machine). If no NEXT VALUE FOR expression had been previously called, this would produce an exception. If in a given statement a CURRENT VALUE FOR expression is used together with a NEXT VALUE FOR expression for the same sequence, then both would evaluate to the value produced by the NEXT VALUE FOR expression.

The NEXT VALUE FOR and CURRENT VALUE FOR expressions may only be used as SELECT expressions or in the UPSERT VALUES statement. Use in WHERE, GROUP BY, HAVING, or ORDER BY will produce an exception. In addition, they cannot be used as the argument to an aggregate function.

To drop a sequence, issue the following command:

    DROP SEQUENCE my_schema.my_sequence;

To discover all sequences that have been created, you may query the SYSTEM.SEQUENCE table as shown here:

    SELECT sequence_schema, sequence_name, start_with, increment_by, cache_size FROM SYSTEM."SEQUENCE";

Note that only read-only access to the SYSTEM.SEQUENCE table is supported.

