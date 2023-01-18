package org.apache.phoenix.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Scope;
import org.apache.phoenix.trace.TraceUtil;
import org.jline.reader.Completer;
import sqlline.CommandHandler;
import sqlline.DispatchCallback;
import sqlline.SqlLine;

import java.util.ArrayList;
import java.util.List;

public class TraceCommandHandler implements CommandHandler {
    public final String TRACE_COMMAND_NAME = "trace";
    public final String
            TRACE_COMMAND_HELP =
            "!trace start, traceId (to start a trace) \n" + "!trace stop (to stop trace)";
    private SqlLine sqlLine;
    private static Span span;
    private static Scope scope;
    private static String spanName;

    public TraceCommandHandler(SqlLine sqlLine) {
        this.sqlLine = sqlLine;
    }

    @Override public String getName() {
        return TRACE_COMMAND_NAME;
    }

    @Override public List<String> getNames() {
        ArrayList<String> names = new ArrayList<>(1);
        names.add(TRACE_COMMAND_NAME);
        return names;
    }

    @Override public String getHelpText() {
        return TRACE_COMMAND_HELP;
    }

    @Override public String matches(String line) {
        if (line == null || line.length() == 0) {
            return null;
        }
        String[] parts = line.split(" ");
        if (parts[0].equals(TRACE_COMMAND_NAME)) {
            return TRACE_COMMAND_NAME;
        }
        return null;
    }

    @Override public void execute(String line, DispatchCallback dispatchCallback) {
        try {
            String[] parts = line.trim().split(" ");
            String command = parts[0].trim();
            if (parts.length > 1 && command.equals(TRACE_COMMAND_NAME)) {
                String[] commandParts = parts[1].trim().split(",");
                String traceCmd = commandParts[0].trim();
                if (traceCmd.equals("start") && commandParts.length == 2) {
                    spanName = commandParts[1].trim();
                    span =
                            TraceUtil.getGlobalTracer().spanBuilder(spanName)
                                    .setSpanKind(SpanKind.CLIENT).startSpan();
                    scope = span.makeCurrent();
                    sqlLine.output("Trace started for: " + spanName);
                }
                if (traceCmd.equals("stop")) {
                    if (scope != null) {
                        scope.close();
                    }
                    if (span != null) {
                        sqlLine.output("Trace stopped for :" + spanName);
                        span.end();
                    }
                }
            }
            dispatchCallback.setToSuccess();
        } catch (Throwable e) {
            dispatchCallback.setToFailure();
            sqlLine.error(e);
            sqlLine.handleException(e);
        }
    }

    @Override public List<Completer> getParameterCompleters() {
        return null;
    }

    @Override public boolean echoToFile() {
        return false;
    }
}
