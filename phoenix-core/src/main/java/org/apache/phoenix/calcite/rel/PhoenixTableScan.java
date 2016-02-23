package org.apache.phoenix.calcite.rel;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.PhoenixTable;
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
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * Scan of a Phoenix table.
 */
public class PhoenixTableScan extends TableScan implements PhoenixRel {
    public enum ScanOrder {
        NONE,
        FORWARD,
        REVERSE,
    }
    
    public final RexNode filter;
    public final ScanOrder scanOrder;
    public final ScanRanges scanRanges;
    
    protected final GuidePostsInfo filteredGuideposts;
    protected final float rowCountFactor;
    
    public static PhoenixTableScan create(RelOptCluster cluster, final RelOptTable table) {
        return create(cluster, table, null,
                getDefaultScanOrder(table.unwrap(PhoenixTable.class)));
    }

    public static PhoenixTableScan create(RelOptCluster cluster, final RelOptTable table, 
            RexNode filter, final ScanOrder scanOrder) {
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
        return new PhoenixTableScan(cluster, traits, table, filter, scanOrder);
    }

    private PhoenixTableScan(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RexNode filter, ScanOrder scanOrder) {
        super(cluster, traits, table);
        this.filter = filter;
        this.scanOrder = scanOrder;
        final PhoenixTable phoenixTable = table.unwrap(PhoenixTable.class);
        this.rowCountFactor = phoenixTable.pc.getQueryServices()
                .getProps().getFloat(PhoenixRel.ROW_COUNT_FACTOR, 1f);
        
        ScanRanges scanRanges = null;
        GuidePostsInfo info = null;
        HTableInterface statsHTable = null;
        if (filter != null) {
            try {
                // TODO simplify this code
                PTable pTable = phoenixTable.getTable();
                TableRef tableRef = new TableRef(CalciteUtils.createTempAlias(), pTable, HConstants.LATEST_TIMESTAMP, false);
                // We use a implementor with a special implementation for field access
                // here, which translates RexFieldAccess into a LiteralExpression
                // with a sample value. This will achieve 3 goals at a time:
                // 1) avoid getting exception when translating RexFieldAccess at this 
                //    time when the correlate variable has not been defined yet.
                // 2) get a guess of ScanRange even if the runtime value is absent.
                // 3) test whether this dynamic filter is worth a recompile at runtime.
                Implementor tmpImplementor = new PhoenixRelImplementorImpl(null) {                    
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
                tmpImplementor.setTableRef(tableRef);
                SelectStatement select = SelectStatement.SELECT_ONE;
                PhoenixStatement stmt = new PhoenixStatement(phoenixTable.pc);
                ColumnResolver resolver = FromCompiler.getResolver(tableRef);
                StatementContext context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
                Expression filterExpr = CalciteUtils.toExpression(filter, tmpImplementor);
                filterExpr = WhereOptimizer.pushKeyExpressionsToScan(context, select, filterExpr);
                scanRanges = context.getScanRanges();
                if (!scanRanges.isPointLookup()
                        && !scanRanges.isDegenerate()
                        && !scanRanges.isEverything()) {
                    // TODO get the cf and timestamp right.
                    Scan scan = context.getScan();
                    byte[] cf = SchemaUtil.getEmptyColumnFamily(pTable);
                    statsHTable = phoenixTable.pc.getQueryServices()
                            .getTable(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES);
                    info = StatisticsUtil.readStatistics(
                            statsHTable, pTable.getPhysicalName().getBytes(),
                            new ImmutableBytesPtr(cf),
                            scan.getStartRow(),
                            scan.getStopRow(),
                            HConstants.LATEST_TIMESTAMP).getGuidePosts().get(cf);
                }
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (statsHTable != null) {
                    try {
                        statsHTable.close();
                    } catch (IOException e) {
                    }
                }
            }
        }        
        this.scanRanges = scanRanges;
        this.filteredGuideposts = info;
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
            .itemIf("scanOrder", scanOrder, scanOrder != ScanOrder.NONE);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double byteCount;
        PhoenixTable phoenixTable = table.unwrap(PhoenixTable.class);
        if (scanRanges != null) {
            if (scanRanges.isPointLookup()) {
                byteCount = 1.0;
            } else if (scanRanges.isDegenerate()) {
                byteCount = 0.0;
            } else if (scanRanges.isEverything()) {
                byteCount = phoenixTable.byteCount;
            } else {
                if (filteredGuideposts != null) {
                    byteCount = 0;
                    for (long b : filteredGuideposts.getByteCounts()) {
                        byteCount += b;
                    }
                    // TODO why zero byteCount? a bug?
                    if (byteCount == 0 && filteredGuideposts.getGuidePostsCount() > 0) {
                        PTable pTable = phoenixTable.getTable();
                        byte[] emptyCf = SchemaUtil.getEmptyColumnFamily(pTable);
                        GuidePostsInfo info = pTable.getTableStats().getGuidePosts().get(emptyCf);
                        byteCount = phoenixTable.byteCount * filteredGuideposts.getGuidePostsCount() / info.getGuidePostsCount();                        
                    }
                } else {
                    PTable pTable = phoenixTable.getTable();
                    byte[] emptyCf = SchemaUtil.getEmptyColumnFamily(pTable);
                    GuidePostsInfo info = pTable.getTableStats().getGuidePosts().get(emptyCf);
                    if (info != null) {
                        byteCount = phoenixTable.byteCount / info.getGuidePostsCount() / 2;
                    } else {
                        int pkCount = scanRanges.getBoundPkColumnCount();
                        byteCount = phoenixTable.byteCount * Math.pow(mq.getSelectivity(this, filter), pkCount);
                    }
                }
            }
        } else {
            byteCount = phoenixTable.byteCount;
        }
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
        return planner.getCostFactory().makeCost(0, byteCount, 0)
                .multiplyBy(SERVER_FACTOR);
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
    public QueryPlan implement(Implementor implementor) {
        final PhoenixTable phoenixTable = table.unwrap(PhoenixTable.class);
        PTable pTable = phoenixTable.getTable();
        TableRef tableRef = new TableRef(CalciteUtils.createTempAlias(), pTable, HConstants.LATEST_TIMESTAMP, false);
        implementor.setTableRef(tableRef);
        try {
            PhoenixStatement stmt = new PhoenixStatement(phoenixTable.pc);
            ColumnResolver resolver = FromCompiler.getResolver(tableRef);
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
            projectColumnFamilies(context.getScan(), phoenixTable.getTable(), columnRefList);
            if (implementor.getCurrentContext().forceProject) {
                TupleProjector tupleProjector = implementor.createTupleProjector();
                TupleProjector.serializeProjectorIntoScan(context.getScan(), tupleProjector);
                PTable projectedTable = implementor.createProjectedTable();
                implementor.setTableRef(new TableRef(projectedTable));
            }
            Integer limit = null;
            OrderBy orderBy = scanOrder == ScanOrder.NONE ?
                      OrderBy.EMPTY_ORDER_BY
                    : (scanOrder == ScanOrder.FORWARD ?
                              OrderBy.FWD_ROW_KEY_ORDER_BY
                            : OrderBy.REV_ROW_KEY_ORDER_BY);
            ParallelIteratorFactory iteratorFactory = null;
            return new ScanPlan(context, select, tableRef, RowProjector.EMPTY_PROJECTOR, limit, orderBy, iteratorFactory, true, dynamicFilter);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void projectColumnFamilies(Scan scan, PTable table, ImmutableIntList columnRefList) {
        scan.getFamilyMap().clear();
        for (Integer index : columnRefList) {
            PColumn column = table.getColumns().get(index);
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
