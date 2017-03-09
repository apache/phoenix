package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * Scan of a Phoenix table.
 */
public class PhoenixTableScan extends TableScan implements PhoenixQueryRel {
    public enum ScanOrder {
        NONE,
        FORWARD,
        REVERSE,
    }
    
    public final RexNode filter;
    public final ScanOrder scanOrder;
    public final ScanRanges scanRanges;
    public final ImmutableBitSet extendedColumnRef;
    
    protected final Long estimatedBytes;
    protected final float rowCountFactor;
    
    public static PhoenixTableScan create(RelOptCluster cluster, final RelOptTable table) {
        return create(cluster, table, null,
                getDefaultScanOrder(table.unwrap(PhoenixTable.class)), null);
    }

    public static PhoenixTableScan create(RelOptCluster cluster, final RelOptTable table, 
            RexNode filter, final ScanOrder scanOrder, ImmutableBitSet extendedColumnRef) {
        final RelTraitSet traits =
                cluster.traitSetOf(PhoenixConvention.SERVER)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        if (scanOrder == ScanOrder.NONE) {
                            return ImmutableList.of();
                        }
                        List<RelCollation> collations = table.getCollationList();
                        return scanOrder == ScanOrder.FORWARD ? collations : reverse(collations);
                    }
                });
        return new PhoenixTableScan(cluster, traits, table, filter, scanOrder, extendedColumnRef);
    }

    private PhoenixTableScan(RelOptCluster cluster, RelTraitSet traits,
            RelOptTable table, RexNode filter, ScanOrder scanOrder,
            ImmutableBitSet extendedColumnRef) {
        super(cluster, traits, table);
        this.filter = filter;
        this.scanOrder = scanOrder;
        final PhoenixTable phoenixTable = table.unwrap(PhoenixTable.class);
        this.rowCountFactor = phoenixTable.pc.getQueryServices()
                .getProps().getFloat(PhoenixRel.ROW_COUNT_FACTOR, 1f);
        try {
            // TODO simplify this code
            TableMapping tableMapping = phoenixTable.tableMapping;
            PTable pTable = tableMapping.getPTable();
            SelectStatement select = SelectStatement.SELECT_ONE;
            PhoenixStatement stmt = new PhoenixStatement(phoenixTable.pc);
            ColumnResolver resolver = FromCompiler.getResolver(tableMapping.getTableRef());
            StatementContext context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
            if (extendedColumnRef == null) {
                extendedColumnRef = tableMapping.getDefaultExtendedColumnRef();
            }
            this.extendedColumnRef = extendedColumnRef;
            if (filter != null) {
                assert this.extendedColumnRef.contains(
                        tableMapping.getExtendedColumnRef(ImmutableList.of(filter)));
                // We use a implementor with a special implementation for correlate variables
                // or bind parameters here, which translates them into a LiteralExpression
                // with a sample value. This will achieve 3 goals at a time:
                // 1) avoid getting exception when translating RexFieldAccess at this time when
                //    the correlate variable has not been defined yet.
                // 2) get a guess of ScanRange even if the runtime value is absent.
                //    TODO instead of getting a random sample value, we'd better get it from
                //    existing guidepost bytes.
                // 3) test whether this dynamic filter is worth a recompile at runtime.
                PhoenixRelImplementor tmpImplementor = new PhoenixRelImplementorImpl(
                        context, RuntimeContext.EMPTY_CONTEXT) {                    
                    @SuppressWarnings("rawtypes")
                    @Override
                    public Expression newBindParameterExpression(int index, PDataType type, Integer maxLength) {
                        try {
                            return LiteralExpression.newConstant(type.getSampleValue(maxLength), type);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    
                    @SuppressWarnings("rawtypes")
                    @Override
                    public Expression newFieldAccessExpression(String variableId, int index, PDataType type) {
                        try {
                            return LiteralExpression.newConstant(type.getSampleValue(), type);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }                    
                };
                tmpImplementor.setTableMapping(tableMapping);
                Expression filterExpr = CalciteUtils.toExpression(filter, tmpImplementor);
                filterExpr = WhereOptimizer.pushKeyExpressionsToScan(context, select, filterExpr);
                WhereCompiler.setScanFilter(context, select, filterExpr, true, false);
            }        
            this.scanRanges = context.getScanRanges();
            // TODO Get estimated byte count based on column reference list.
            this.estimatedBytes = BaseResultIterators.getEstimatedCount(context, pTable).getSecond();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static ScanOrder getDefaultScanOrder(PhoenixTable table) {
        //TODO why attribute value not correct in connectUsingModel??
        //return table.pc.getQueryServices().getProps().getBoolean(
        //        QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB,
        //        QueryServicesOptions.DEFAULT_FORCE_ROW_KEY_ORDER) ?
        //                ScanOrder.FORWARD : ScanOrder.NONE;
        return ScanOrder.NONE;
    }
    
    private static List<RelCollation> reverse(List<RelCollation> collations) {
        Builder<RelCollation> builder = ImmutableList.<RelCollation>builder();
        for (RelCollation collation : collations) {
            builder.add(CalciteUtils.reverseCollation(collation));
        }
        return builder.build();
    }
    
    public boolean isReverseScanEnabled() {
        return table.unwrap(PhoenixTable.class).pc
                .getQueryServices().getProps().getBoolean(
                        QueryServices.USE_REVERSE_SCAN_ATTRIB,
                        QueryServicesOptions.DEFAULT_USE_REVERSE_SCAN);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("filter", filter, filter != null)
            .itemIf("scanOrder", scanOrder, scanOrder != ScanOrder.NONE)
            .itemIf("extendedColumns", extendedColumnRef, !extendedColumnRef.isEmpty());
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof PhoenixTableScan)) {
            return false;
        }

        PhoenixTableScan other = (PhoenixTableScan) obj;
        return this.table.equals(other.table)
                && Objects.equals(this.filter, other.filter)
                && this.scanOrder == other.scanOrder
                && this.extendedColumnRef.equals(other.extendedColumnRef);
    }

    @Override public int hashCode() {
        return table.hashCode();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double byteCount;
        PhoenixTable phoenixTable = table.unwrap(PhoenixTable.class);
        if (estimatedBytes != null) {
            byteCount = estimatedBytes;
        } else {
            // If stats are not available, we estimate based on selectivity.
            int pkCount = scanRanges.getBoundPkColumnCount();
            if (pkCount > 0) {
                byteCount = phoenixTable.byteCount * Math.pow(mq.getSelectivity(this, filter), pkCount);
            } else {
                byteCount = phoenixTable.byteCount;
            }
        }
        Pair<Integer, Integer> columnRefCount =
                phoenixTable.tableMapping.getExtendedColumnReferenceCount(extendedColumnRef);
        double extendedColumnMultiplier = 1 + columnRefCount.getFirst() * 10 + columnRefCount.getSecond() * 0.1;
        byteCount *= extendedColumnMultiplier;
        byteCount *= rowCountFactor;
        if (scanOrder != ScanOrder.NONE) {
            // We don't want to make a big difference here. The idea is to avoid
            // forcing row key order whenever the order is absolutely useless.
            // E.g. in "select count(*) from t" we do not need the row key order;
            // while in "select * from t order by pk0" we should force row key
            // order to avoid sorting.
            // Another case is "select pk0, count(*) from t", where we'd like to
            // choose the row key ordered TableScan rel so that the Aggregate rel
            // above it can be an stream aggregate, although at runtime this will
            // eventually be an AggregatePlan, in which the "forceRowKeyOrder"
            // flag takes no effect.
            byteCount = addEpsilon(byteCount);
            if (scanOrder == ScanOrder.REVERSE) {
                byteCount = addEpsilon(byteCount);
            }
        }
        return planner.getCostFactory()
                .makeCost(byteCount + 1, byteCount + 1, 0)
                .multiplyBy(0.5) /* data scan only */
                .multiplyBy(SERVER_FACTOR)
                .multiplyBy(PHOENIX_FACTOR);
    }
    
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rows = super.estimateRowCount(mq);
        if (filter != null && !filter.isAlwaysTrue()) {
            rows = rows * mq.getSelectivity(this, filter);
        }
        
        return rows * rowCountFactor;
    }
    
    @Override
    public List<RelCollation> getCollationList() {
        return getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);        
    }

    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        final PhoenixTable phoenixTable = table.unwrap(PhoenixTable.class);
        TableMapping tableMapping = phoenixTable.tableMapping;
        implementor.setTableMapping(tableMapping);
        try {
            PhoenixStatement stmt = new PhoenixStatement(phoenixTable.pc);
            ColumnResolver resolver = FromCompiler.getResolver(tableMapping.getTableRef());
            StatementContext context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
            SelectStatement select = SelectStatement.SELECT_ONE;
            ImmutableIntList columnRefList = implementor.getCurrentContext().columnRefList;
            Expression filterExpr = LiteralExpression.newConstant(Boolean.TRUE);
            Expression dynamicFilter = null;
            if (filter != null) {
                ImmutableBitSet bitSet = InputFinder.analyze(filter).inputBitSet.addAll(columnRefList).build();
                columnRefList = ImmutableIntList.copyOf(bitSet.asList());
                filterExpr = CalciteUtils.toExpression(filter, implementor);
            }
            Expression rem = WhereOptimizer.pushKeyExpressionsToScan(context, select, filterExpr);
            WhereCompiler.setScanFilter(context, select, rem, true, false);
            // TODO This is not absolutely strict. We may have a filter like:
            // pk = '0' and pk = $cor0 where $cor0 happens to get a sample value
            // as '0', thus making the below test return false and adding an
            // unnecessary dynamic filter. This would only be a performance bug though.
            if (filter != null && !context.getScanRanges().equals(this.scanRanges)) {
                dynamicFilter = filterExpr;
            }
            tableMapping.setupScanForExtendedTable(context.getScan(), extendedColumnRef, context.getConnection());
            projectColumnFamilies(context.getScan(), tableMapping.getMappedColumns(), columnRefList);
            if (implementor.getCurrentContext().forceProject) {
                boolean retainPKColumns = implementor.getCurrentContext().retainPKColumns;
                TupleProjector tupleProjector = tableMapping.createTupleProjector(retainPKColumns);
                TupleProjector.serializeProjectorIntoScan(context.getScan(), tupleProjector);
                PTable projectedTable = tableMapping.createProjectedTable(retainPKColumns);
                implementor.setTableMapping(new TableMapping(projectedTable));
            }
            OrderBy orderBy = scanOrder == ScanOrder.NONE ?
                      OrderBy.EMPTY_ORDER_BY
                    : (scanOrder == ScanOrder.FORWARD ?
                              OrderBy.FWD_ROW_KEY_ORDER_BY
                            : OrderBy.REV_ROW_KEY_ORDER_BY);
            ParallelIteratorFactory iteratorFactory = null;
            TableRef tableRef = tableMapping.getTableRef();
            TableRef srcRef = tableMapping.getDataTableRef() == null ?
                    tableRef : tableMapping.getDataTableRef();
            // FIXME this is just a temporary fix for schema caching problem.
            tableRef.setTimeStamp(QueryConstants.UNSET_TIMESTAMP);
            srcRef.setTimeStamp(QueryConstants.UNSET_TIMESTAMP);
            return new ScanPlan(context, select, tableRef, srcRef, RowProjector.EMPTY_PROJECTOR, null, null, orderBy, iteratorFactory, true, dynamicFilter);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void projectColumnFamilies(Scan scan, List<PColumn> mappedColumns, ImmutableIntList columnRefList) {
        scan.getFamilyMap().clear();
        for (Integer index : columnRefList) {
            PColumn column = mappedColumns.get(index);
            PName familyName = column.getFamilyName();
            if (familyName != null) {
                scan.addFamily(familyName.getBytes());
            }
        }
    }

    private double addEpsilon(double d) {
      assert d >= 0d;
      final double d0 = d;
      if (d < 10) {
        // For small d, adding 1 would change the value significantly.
        d *= 1.001d;
        if (d != d0) {
          return d;
        }
      }
      // For medium d, add 1. Keeps integral values integral.
      ++d;
      if (d != d0) {
        return d;
      }
      // For large d, adding 1 might not change the value. Add .1%.
      // If d is NaN, this still will probably not change the value. That's OK.
      d *= 1.001d;
      return d;
    }
}
