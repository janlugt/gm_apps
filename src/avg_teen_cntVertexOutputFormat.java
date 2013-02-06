import java.io.IOException;
import org.apache.giraph.graph.*;
import org.apache.giraph.vertex.*;
import org.apache.giraph.io.formats.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class avg_teen_cntVertexOutputFormat
extends TextVertexOutputFormat<LongWritable, avg_teen_cntVertex.VertexData, NullWritable> {
    int outtype;
    @SuppressWarnings("unchecked")
    @Override
    public TextVertexWriter createVertexWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
        outtype = context.getConfiguration().getInt("GMOutputFormat",avg_teen_cnt.GM_FORMAT_ADJ);
        return new avg_teen_cntVertexWriter();
    }

    private class avg_teen_cntVertexWriter
    extends TextVertexOutputFormat<LongWritable, avg_teen_cntVertex.VertexData, NullWritable>.TextVertexWriterToEachLine {

        @Override
        protected Text convertVertexToLine(Vertex<LongWritable, avg_teen_cntVertex.VertexData, NullWritable, ?> vertex)
        throws IOException {
            StringBuffer sb = new StringBuffer(vertex.getId().toString());
            //--------------------------------------
            // Output format is as follows:
            // <vertex_idlong> <teen_cnt(int)> {<dest_id(long)> }*
            // (Entries are separated with \t). Edges and Edge values are NOT dumped if outtype is GM_FORMAT_NODE_PROP.
            //--------------------------------------
            avg_teen_cntVertex.VertexData v = vertex.getValue();
            sb.append('\t').append(v.teen_cnt);

            if (outtype == avg_teen_cnt.GM_FORMAT_ADJ) {
                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sb.append('\t').append(edge.getTargetVertexId());
                }
            }

            return new Text(sb.toString());
        }
    }
}
