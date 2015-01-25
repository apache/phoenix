package org.apache.phoenix.expression.function;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PLong;

/**
 * Random function that produces a unique value upon each invocation unless a seed is provided.
 * If a seed is provided the returned value is identical across each invocation for a single row, but different across multiple rows.
 * The seed must be a constant.
 * <p>
 * Example:
 * <pre>
 * 0: jdbc:phoenix:localhost> select rand(), rand(), rand(1), rand(2), rand(1) from t;
 * +----------------------------+----------------------------+----------------------------+----------------------------+-----------------------+
 * |           RAND()           |           RAND()           |          RAND(1)           |          RAND(2)           |          RAND(1)      |
 * +----------------------------+----------------------------+----------------------------+----------------------------+-----------------------+
 * | 0.18927325291276054        | 0.19335253869230284        | 0.7308781907032909         | 0.7311469360199058         | 0.7308781907032909    |
 * | 0.08156917775368278        | 0.10178318739559034        | 0.41008081149220166        | 0.9014476240300544         | 0.41008081149220166   |
 * +----------------------------+----------------------------+----------------------------+----------------------------+-----------------------+
 * 2 rows selected (0.096 seconds)
 * 0: jdbc:phoenix:localhost> select rand(), rand(), rand(1), rand(2), rand(1) from t;
 * +----------------------------+----------------------------+----------------------------+----------------------------+-----------------------+
 * |           RAND()           |           RAND()           |          RAND(1)           |          RAND(2)           |          RAND(1)      |
 * +----------------------------+----------------------------+----------------------------+----------------------------+-----------------------+
 * | 0.6452639556507597         | 0.8167638693890659         | 0.7308781907032909         | 0.7311469360199058         | 0.7308781907032909    |
 * | 0.8084646053276106         | 0.6969504742211767         | 0.41008081149220166        | 0.9014476240300544         | 0.41008081149220166   |
 * +----------------------------+----------------------------+----------------------------+----------------------------+-----------------------+
 * 2 rows selected (0.098 seconds)
 * </pre>
 */
@BuiltInFunction(name = RandomFunction.NAME, args = {@Argument(allowedTypes={PLong.class},defaultValue="null",isConstant=true)})
public class RandomFunction extends ScalarFunction {
    public static final String NAME = "RAND";
    private Random random;
    private boolean hasSeed;
    private Double current;

    public RandomFunction() {
    }

    public RandomFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        Number seed = (Number)((LiteralExpression)children.get(0)).getValue();
        random = seed == null ? new Random() : new Random(seed.longValue());
        hasSeed = seed != null;
        current = null;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (current == null) {
            current = random.nextDouble();
        }
        ptr.set(PDouble.INSTANCE.toBytes(current));
        return true;
    }

    // produce a new random value for each row
    @Override
    public void reset() {
        super.reset();
        current = null;
    }

    @Override
    public PDataType<?> getDataType() {
        return PDouble.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Determinism getDeterminism() {
        return hasSeed ? Determinism.PER_ROW : Determinism.PER_INVOCATION;
    }

    @Override
    public boolean isStateless() {
        return true;
    }

    // take the random object onto account
    @Override
    public int hashCode() {
        int hashCode = super.hashCode();
        return hasSeed ? hashCode : (hashCode + random.hashCode());
    }

    // take the random object onto account, as otherwise we'll potentially collapse two
    // RAND() calls into a single one.
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && (hasSeed || random.equals(((RandomFunction)obj).random));
    }

    // make sure we do not show the default 'null' parameter
    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder(getName() + "(");
        if (!hasSeed) return buf.append(")").toString();
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ", ");
        }
        buf.append(children.get(children.size()-1) + ")");
        return buf.toString();
    }
}
