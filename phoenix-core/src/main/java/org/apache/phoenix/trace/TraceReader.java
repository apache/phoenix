/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package org.apache.phoenix.trace;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.LogUtil;
import org.apache.htrace.Span;

import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;

/**
 * Read the traces written to phoenix tables by the {@link PhoenixMetricsSink}.
 */
public class TraceReader {

    private static final Log LOG = LogFactory.getLog(TraceReader.class);
    private final Joiner comma = Joiner.on(',');
    private String knownColumns;
    {
        // the order here dictates the order we pull out the values below. For now, just keep them
        // in sync - so we can be efficient pulling them off the results.
        knownColumns =
                comma.join(MetricInfo.TRACE.columnName, MetricInfo.PARENT.columnName,
                    MetricInfo.SPAN.columnName, MetricInfo.DESCRIPTION.columnName,
                    MetricInfo.START.columnName, MetricInfo.END.columnName,
                    MetricInfo.HOSTNAME.columnName, PhoenixMetricsSink.TAG_COUNT,
                        PhoenixMetricsSink.ANNOTATION_COUNT);
    }

    private Connection conn;
    private String table;
    private int pageSize;

    public TraceReader(Connection conn, String statsTableName) throws SQLException {
        this.conn = conn;
        this.table = statsTableName;
        String ps = conn.getClientInfo(QueryServices.TRACING_PAGE_SIZE_ATTRIB);
        this.pageSize = ps == null ? QueryServicesOptions.DEFAULT_TRACING_PAGE_SIZE : Integer.parseInt(ps);
    }

    public TraceReader(Connection conn) throws SQLException {
        this(conn, QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME);
    }

    /**
     * Read all the currently stored traces.
     * <p>
     * <b>Be Careful!</b> This could cause an OOME if there are a lot of traces.
     * @param limit max number of traces to return. If -1, returns all known traces.
     * @return the found traces
     * @throws SQLException
     */
    public Collection<TraceHolder> readAll(int limit) throws SQLException {
        Set<TraceHolder> traces = new HashSet<TraceHolder>();
        // read all the known columns from the table, sorting first by trace column (so the same
        // trace
        // goes together), and then by start time (so parent spans always appear before child spans)
        String query =
                "SELECT " + knownColumns + " FROM " + QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME
                        + " ORDER BY " + MetricInfo.TRACE.columnName + " DESC, "
                        + MetricInfo.START.columnName + " ASC" + " LIMIT " + pageSize;
        int resultCount = 0;
        ResultSet results = conn.prepareStatement(query).executeQuery();
        TraceHolder trace = null;
        // the spans that are not the root span, but haven't seen their parent yet
        List<SpanInfo> orphans = null;
        while (results.next()) {
            int index = 1;
            long traceid = results.getLong(index++);
            long parent = results.getLong(index++);
            long span = results.getLong(index++);
            String desc = results.getString(index++);
            long start = results.getLong(index++);
            long end = results.getLong(index++);
            String host = results.getString(index++);
            int tagCount = results.getInt(index++);
            int annotationCount = results.getInt(index++);
            // we have a new trace
            if (trace == null || traceid != trace.traceid) {
                // only increment if we are on a new trace, to ensure we get at least one
                if (trace != null) {
                    resultCount++;
                }
                // we beyond the limit, so we stop
                if (resultCount >= limit) {
                    break;
                }
                trace = new TraceHolder();
                // add the orphans, so we can track them later
                orphans = new ArrayList<SpanInfo>();
                trace.orphans = orphans;
                trace.traceid = traceid;
                traces.add(trace);
            }

            // search the spans to determine the if we have a known parent
            SpanInfo parentSpan = null;
            if (parent != Span.ROOT_SPAN_ID) {
                // find the parent
                for (SpanInfo p : trace.spans) {
                    if (p.id == parent) {
                        parentSpan = p;
                        break;
                    }
                }
            }
            SpanInfo spanInfo =
                    new SpanInfo(parentSpan, parent, span, desc, start, end, host, tagCount,
                            annotationCount);
            // search the orphans to see if this is the parent id

            for (int i = 0; i < orphans.size(); i++) {
                SpanInfo orphan = orphans.get(i);
                // we found the parent for the orphan
                if (orphan.parentId == span) {
                    // update the bi-directional relationship
                    orphan.parent = spanInfo;
                    spanInfo.children.add(orphan);
                    // / its no longer an orphan
                    LOG.trace(addCustomAnnotations("Found parent for span: " + span));
                    orphans.remove(i--);
                }
            }

            if (parentSpan != null) {
                // add this as a child to the parent span
                parentSpan.children.add(spanInfo);
            } else if (parent != Span.ROOT_SPAN_ID) {
                // add the span to the orphan pile to check for the remaining spans we see
                LOG.info(addCustomAnnotations("No parent span found for span: " + span + " (root span id: "
                        + Span.ROOT_SPAN_ID + ")"));
                orphans.add(spanInfo);
            }

            // add the span to the full known list
            trace.spans.add(spanInfo);

            // go back and find the tags for the row
            spanInfo.tags.addAll(getTags(traceid, parent, span, tagCount));

            spanInfo.annotations.addAll(getAnnotations(traceid, parent, span, annotationCount));
        }

        // make sure we clean up after ourselves
        results.close();

        return traces;
    }

    private Collection<? extends String> getTags(long traceid, long parent, long span, int count)
            throws SQLException {
        return getDynamicCountColumns(traceid, parent, span, count,
                PhoenixMetricsSink.TAG_FAMILY, MetricInfo.TAG.columnName);
    }

    private Collection<? extends String> getAnnotations(long traceid, long parent, long span,
            int count) throws SQLException {
        return getDynamicCountColumns(traceid, parent, span, count,
                PhoenixMetricsSink.ANNOTATION_FAMILY, MetricInfo.ANNOTATION.columnName);
    }

    private Collection<? extends String> getDynamicCountColumns(long traceid, long parent,
            long span, int count, String family, String columnName) throws SQLException {
        if (count == 0) {
            return Collections.emptyList();
        }

        // build the column strings, family.column<index>
        String[] parts = new String[count];
        for (int i = 0; i < count; i++) {
            parts[i] = PhoenixMetricsSink.getDynamicColumnName(family, columnName, i);
        }
        // join the columns together
        String columns = comma.join(parts);

        // redo them and add "VARCHAR to the end, so we can specify the columns
        for (int i = 0; i < count; i++) {
            parts[i] = parts[i] + " VARCHAR";
        }

        String dynamicColumns = comma.join(parts);
        String request =
                "SELECT " + columns + " from " + table + "(" + dynamicColumns + ") WHERE "
                        + MetricInfo.TRACE.columnName + "=" + traceid + " AND "
                        + MetricInfo.PARENT.columnName + "=" + parent + " AND "
                        + MetricInfo.SPAN.columnName + "=" + span;
        LOG.trace(addCustomAnnotations("Requesting columns with: " + request));
        ResultSet results = conn.createStatement().executeQuery(request);
        List<String> cols = new ArrayList<String>();
        while (results.next()) {
            for (int index = 1; index <= count; index++) {
                cols.add(results.getString(index));
            }
        }
        if (cols.size() < count) {
            LOG.error(addCustomAnnotations("Missing tags! Expected " + count + ", but only got " + cols.size()
                    + " tags from rquest " + request));
        }
        return cols;
    }
    
    private String addCustomAnnotations(String logLine) throws SQLException {
    	if (conn.isWrapperFor(PhoenixConnection.class)) {
    		PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
    		logLine = LogUtil.addCustomAnnotations(logLine, phxConn);
    	}
    	return logLine;
    }
    
    /**
     * Holds information about a trace
     */
    public static class TraceHolder {
        public List<SpanInfo> orphans;
        public long traceid;
        public TreeSet<SpanInfo> spans = new TreeSet<SpanInfo>();

        @Override
        public int hashCode() {
            return new Long(traceid).hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TraceHolder) {
                return traceid == ((TraceHolder) o).traceid;
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Trace: " + traceid + "\n");
            // get the first span, which is always going to be the root span
            SpanInfo root = spans.iterator().next();
            if (root.parent != null) {
                sb.append("Root span not present! Just printing found spans\n");
                for (SpanInfo span : spans) {
                    sb.append(span.toString() + "\n");
                }
            } else {
                // print the tree of spans
                List<SpanInfo> toPrint = new ArrayList<SpanInfo>();
                toPrint.add(root);
                while (!toPrint.isEmpty()) {
                    SpanInfo span = toPrint.remove(0);
                    sb.append(span.toString() + "\n");
                    toPrint.addAll(span.children);
                }
            }
            if (orphans.size() > 0) {
                sb.append("Found orphan spans:\n" + orphans);
            }
            return sb.toString();
        }
    }

    public static class SpanInfo implements Comparable<SpanInfo> {
        public SpanInfo parent;
        public List<SpanInfo> children = new ArrayList<SpanInfo>();
        public String description;
        public long id;
        public long start;
        public long end;
        public String hostname;
        public int tagCount;
        public List<String> tags = new ArrayList<String>();
        public int annotationCount;
        public List<String> annotations = new ArrayList<String>();
        private long parentId;

        public SpanInfo(SpanInfo parent, long parentid, long span, String desc, long start,
                long end, String host, int tagCount, int annotationCount) {
            this.parent = parent;
            this.parentId = parentid;
            this.id = span;
            this.description = desc;
            this.start = start;
            this.end = end;
            this.hostname = host;
            this.tagCount = tagCount;
            this.annotationCount = annotationCount;
        }

        @Override
        public int hashCode() {
            return new Long(id).hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof SpanInfo) {
                return id == ((SpanInfo) o).id;
            }
            return false;
        }

        /**
         * Do the same sorting that we would get from reading the table with a {@link TraceReader},
         * specifically, by trace and then by start/end. However, these are only every stored in a
         * single trace, so we can just sort on start/end times.
         */
        @Override
        public int compareTo(SpanInfo o) {
            // root span always comes first
            if (this.parentId == Span.ROOT_SPAN_ID) {
                return -1;
            } else if (o.parentId == Span.ROOT_SPAN_ID) {
                return 1;
            }

            int compare = Longs.compare(start, o.start);
            if (compare == 0) {
                compare = Longs.compare(end, o.end);
                if (compare == 0) {
                    return Longs.compare(id, o.id);
                }
            }
            return compare;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Span: " + id + "\n");
            sb.append("\tdescription=" + description);
            sb.append("\n");
            sb.append("\tparent="
                    + (parent == null ? (parentId == Span.ROOT_SPAN_ID ? "ROOT" : "[orphan - id: "
                            + parentId + "]") : parent.id));
            sb.append("\n");
            sb.append("\tstart,end=" + start + "," + end);
            sb.append("\n");
            sb.append("\telapsed=" + (end - start));
            sb.append("\n");
            sb.append("\thostname=" + hostname);
            sb.append("\n");
            sb.append("\ttags=(" + tagCount + ") " + tags);
            sb.append("\n");
            sb.append("\tannotations=(" + annotationCount + ") " + annotations);
            sb.append("\n");
            sb.append("\tchildren=");
            for (SpanInfo child : children) {
                sb.append(child.id + ", ");
            }
            sb.append("\n");
            return sb.toString();
        }

        public long getParentIdForTesting() {
            return parentId;
        }
    }
}