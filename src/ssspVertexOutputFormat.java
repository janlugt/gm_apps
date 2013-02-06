import java.io.IOException;
import org.apache.giraph.graph.*;
import org.apache.giraph.vertex.*;
import org.apache.giraph.io.formats.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ssspVertexOutputFormat
extends TextVertexOutputFormat<LongWritable, ssspVertex.VertexData, ssspVertex.EdgeData> {
    int outtype;
    @SuppressWarnings("unchecked")
    @Override
    public TextVertexWriter createVertexWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
        outtype = context.getConfiguration().getInt("GMOutputFormat",sssp.GM_FORMAT_ADJ);
        return new ssspVertexWriter();
    }

    private class ssspVertexWriter
    extends TextVertexOutputFormat<LongWritable, ssspVertex.VertexData, ssspVertex.EdgeData>.TextVertexWriterToEachLine {

        @Override
        protected Text convertVertexToLine(Vertex<LongWritable, ssspVertex.VertexData, ssspVertex.EdgeData, ?> vertex)
        throws IOException {
            StringBuffer sb = new StringBuffer(vertex.getId().toString());
            //--------------------------------------
            // Output format is as follows:
            // <vertex_idlong> <dist(int)> {<dest_id(long)> }*
            // (Entries are separated with \t). Edges and Edge values are NOT dumped if outtype is GM_FORMAT_NODE_PROP.
            //--------------------------------------
            ssspVertex.VertexData v = vertex.getValue();
            sb.append('\t').append(v.dist);

            if (outtype == sssp.GM_FORMAT_ADJ) {
                for (Edge<LongWritable, ssspVertex.EdgeData> edge : vertex.getEdges()) {
                    sb.append('\t').append(edge.getTargetVertexId());
                }
            }

            return new Text(sb.toString());
        }
    }
}
