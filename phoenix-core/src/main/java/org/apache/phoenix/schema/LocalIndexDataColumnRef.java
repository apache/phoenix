package org.apache.phoenix.schema;

import java.sql.SQLException;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

public class LocalIndexDataColumnRef extends ColumnRef {
    final private int position;
    final private Set<PColumn> columns;
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();

    // TODO: Need a good way to clone this - maybe implement Cloneable instead
//    public LocalIndexDataColumnRef(ColumnRef columnRef, long timeStamp) {
//        super(columnRef, timeStamp);
//    }
//
    public LocalIndexDataColumnRef(StatementContext context, String indexColumnName) throws MetaDataEntityNotFoundException, SQLException {
        super(FromCompiler.getResolver(
            FACTORY.namedTable(null, TableName.create(context.getCurrentTable().getTable()
                    .getSchemaName().getString(), context.getCurrentTable().getTable()
                    .getParentTableName().getString())), context.getConnection()).resolveTable(
            context.getCurrentTable().getTable().getSchemaName().getString(),
            context.getCurrentTable().getTable().getParentTableName().getString()), IndexUtil
                .getDataColumnFamilyName(indexColumnName), IndexUtil
                .getDataColumnName(indexColumnName));
        position = context.getDataColumnPosition(this.getColumn());
        columns = context.getDataColumns();
    }

    @Override
    public ColumnExpression newColumnExpression() {
        PTable table = this.getTable();
        PColumn column = this.getColumn();
        // TODO: util for this or store in member variable
        byte[] defaultFamily = table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : table.getDefaultFamilyName().getBytes();
        String displayName = SchemaUtil.getColumnDisplayName(Bytes.compareTo(defaultFamily, column.getFamilyName().getBytes()) == 0  ? null : column.getFamilyName().getBytes(), column.getName().getBytes());
        return new ProjectedColumnExpression(this.getColumn(), columns, position, displayName);
    }
}
