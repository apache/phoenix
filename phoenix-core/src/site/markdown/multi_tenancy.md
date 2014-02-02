#Mult-tenancy
Support for multi-tenancy is built on top of the concepts of a [VIEW](http://phoenix.incubator.apache.org/views.html) in Phoenix. Users create a logical tenant-specific table as a VIEW and query and update it just like with regular Phoenix tables.  Data in these tenant-specific tables resides in a shared, regular Phoenix table (and thus in a shared HBase table) that is declared at table creation time to support multi-tenancy. All tenant-specific Phoenix tables whose data resides in the same physical HBase table have the same primary key structure but each tenant’s table can contain any number of non-PK columns unique to it. The main advantages afforded by this feature are:

1. It implements physical tenant data isolation, including automatically constraining tenants to only work with data that “belongs” to the each tenant.
2. It prevents a proliferation of HBase tables, minimizing operational complexity.

### Multi-tenant tables
The first primary key column of the physical multi-tenant table must be used to identify the tenant. For example:

    CREATE TABLE base.event (tenant_id VARCHAR, event_type CHAR(1), created_date DATE, event_id BIGINT)
    MULTI_TENANT=true;

In this case, the tenant_id column identifies the tenant and the table is declared to be multi-tenant. The column that identifies the tenant must of type VARCHAR or CHAR.

### Tenant-specific Tables
Tenants are identified by the presence or absence of a TenantId property at JDBC connection-time. A connection with a non-null TenantId is considered a tenant-specific connection. A connection with an unspecified or null TenantId is a regular connection.  A tenant specific connection may only query:

* **their own schema**, which is to say it only sees tenant-specific views that were created by that tenant.
* **non multi-tenant global tables**, that is tables created with a regular connection without the MULTI_TENANT=TRUE declaration.

Tenant-specific views may only be created using a tenant-specific connection and the base table must be a multi-tenant table.  Regular connections are used to create global tables, including those that can be used as base tables for tenant-specific tables.

For example, a tenant-specific connection is established like this:

    Properties props = new Properties();
    props.setProperty("TenantId", "Acme");
    Connection conn = DriverManager.getConnection("localhost", props);

through which a tenant-specific table may be defined like this:

    CREATE VIEW acme.event AS
    SELECT * FROM base.event;

The tenant_id column is neither visible nor accessible to a tenant-specific view. Any reference to it will cause a ColumnNotFoundException.

Alternately, a WHERE clause may be specified to further constrain the data as well:

    CREATE VIEW acme.login_event AS
    SELECT * FROM base.event
    WHERE event_type='L';

Just like any other Phoenix view, whether or not this view is updatable is based on the rules explained [here](http://phoenix.incubator.apache.org/views.html#Updatable-views). In addition, indexes may be added to tenant-specific tables just like with regular tables.

### Tenant Data Isolation
Any DML or query that is performed on a tenant-specific table is automatically constrained to only operate on the tenant’s data. For the upsert operation, this means that Phoenix automatically populates the tenantId column with the tenant’s id specified at connection-time. For querying and delete, a where clause is transparently added to constrain the operations to only see data belonging to the current tenant.
