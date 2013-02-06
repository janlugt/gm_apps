import java.io.IOException;
import org.apache.giraph.graph.*;
import org.apache.giraph.vertex.*;
import org.apache.giraph.io.formats.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class hop_distVertexOutputFormat
extends TextVertexOutputFormat<LongWritable, hop_distVertex.VertexData, NullWritable> {
    int outtype;
    @SuppressWarnings("unchecked")
    @Override
    public TextVertexWriter createVertexWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
        outtype = context.getConfiguration().getInt("GMOutputFormat",hop_dist.GM_FORMAT_ADJ);
        return new hop_distVertexWriter();
    }

    private class hop_distVertexWriter
    extends TextVertexOutputFormat<LongWritable, hop_distVertex.VertexData, NullWritable>.TextVertexWriterToEachLine {

        @Override
        protected Text convertVertexToLine(Vertex<LongWritable, hop_distVertex.VertexData, NullWritable, ?> vertex)
        throws IOException {
            StringBuffer sb = new StringBuffer(vertex.getId().toString());
            //--------------------------------------
            // Output format is as follows:
            // <vertex_idlong> <dist(int)> {<dest_id(long)> }*
            // (Entries are separated with \t). Edges and Edge values are NOT dumped if outtype is GM_FORMAT_NODE_PROP.
            //--------------------------------------
            hop_distVertex.VertexData v = vertex.getValue();
            sb.append('\t').append(v.dist);

            if (outtype == hop_dist.GM_FORMAT_ADJ) {
                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sb.append('\t').append(edge.getTargetVertexId());
                }
            }

            return new Text(sb.toString());
        }
    }
}
