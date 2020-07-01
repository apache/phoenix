package org.apache.phoenix.propagatetrace;


import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.PRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class TracePropagation {
    final static Logger logger = LoggerFactory.getLogger(TracePropagation.class);

    public static void setInitialTrace(PhoenixStatement stmt){
        Random rn = new Random();
        stmt.setTraceId(rn.nextInt());
        logTraceIdAssigned(stmt);
    }
    public static void propagateTraceId(PhoenixStatement src, MutationState.RowMutationState dest){
        dest.setTraceId(src.getTraceId());
        logTraceIdAssigned(dest);
    }
    public static void propagateTraceId(MutationState.RowMutationState src, PRow dest){
        dest.setTraceId((src.getTraceId()));
        logTraceIdAssigned(dest);
    }
    public static void propagateTraceId(PRow src, List<Mutation>dest){
        for(int it=0;it<dest.size();it++){
            dest.get(it).setId(Integer.toString(src.getTraceId()));
        }
    }

    public static int extractTraceId(PhoenixStatement stmt){
        return stmt.getTraceId();
    }

    public static int extractTraceId(MutationState.RowMutationState rowMutationState){
        return rowMutationState.getTraceId();
    }

    public static int extractTraceId(PRow row){
        return row.getTraceId();
    }

    public static void logTraceIdAssigned(PhoenixStatement stmt){
        logger.info("attached traceId {}. to query-statement {}.",TracePropagation.extractTraceId(stmt),stmt);
    }

    public static void logTraceIdAssigned(MutationState.RowMutationState rowMutationState){
        logger.debug("attached traceId {}. to RowMutationState {}.",extractTraceId(rowMutationState),rowMutationState);
    }

    public static void logTraceIdAssigned(PRow row){
        logger.debug("attached traceId {}. to PRow Object ",extractTraceId(row),row);
    }

    public static void logTraceIdAssigned(Mutation mutation){
        logger.info("mutation {}. attached to mutation id {}.",mutation,mutation.getId());
    }

    public static  void logTraceIdAssigned(List<Mutation>batch){
        for(int it=0;it<batch.size();it++){
            logTraceIdAssigned(batch.get(it));
        }
    }
}
