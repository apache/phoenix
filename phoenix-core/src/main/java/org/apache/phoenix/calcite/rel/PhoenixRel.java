package org.apache.phoenix.calcite.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.phoenix.calcite.metadata.PhoenixRelMetadataProvider;
import org.apache.phoenix.compile.StatementPlan;

/**
 * Relational expression in Phoenix.
 *
 * <p>Phoenix evaluates relational expressions using {@link java.util.Iterator}s
 * over streams of {@link org.apache.phoenix.schema.tuple.Tuple}s.</p>
 */
public interface PhoenixRel extends RelNode {

  /** Metadata Provider for PhoenixRel */
  RelMetadataProvider METADATA_PROVIDER = new PhoenixRelMetadataProvider();
  
  /** For test purpose */
  String ROW_COUNT_FACTOR = "phoenix.calcite.metadata.rowcount.factor";

  /** Relative cost of Phoenix versus Enumerable convention.
   *
   * <p>Multiply by the value (which is less than unity), and you will get a cheaper cost.
   * Phoenix is cheaper.
   */
  double PHOENIX_FACTOR = 0.0001;

  /** Relative cost of server plan versus client plan.
   *
   * <p>Multiply by the value (which is less than unity), and you will get a cheaper cost.
   * Server is cheaper.
   */
  double SERVER_FACTOR = 0.2;

  StatementPlan implement(PhoenixRelImplementor implementor);
}
