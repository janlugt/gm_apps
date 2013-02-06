import java.io.IOException;
import java.util.List;
import org.apache.giraph.graph.*;
import org.apache.giraph.io.formats.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.google.common.collect.Lists;

public class ssspVertexInputFormat extends TextVertexInputFormat<LongWritable, ssspVertex.VertexData, ssspVertex.EdgeData, ssspVertex.MessageData> {
    int intype;
    @Override
    public TextVertexReader
    createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        intype = context.getConfiguration().getInt("GMInputFormat",sssp.GM_FORMAT_ADJ);
        return new ssspVertexReader();
    }

    //--------------------------------------
    // Input format is assumed as follows:
    // <vertex_idlong> <dist(int)> {<dest_id(long)> <len(int)> }*
    private class ssspVertexReader extends TextVertexInputFormat<LongWritable, ssspVertex.VertexData, ssspVertex.EdgeData, ssspVertex.MessageData>.TextVertexReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            // Split current line with any space
            return line.toString().split("\\s+");
        }

        @Override
        protected LongWritable getId(String[] values) throws IOException {
            return new LongWritable(Long.parseLong(values[0]));
        }

        @Override
        protected ssspVertex.VertexData getValue(String[] values) throws IOException {
            return new ssspVertex.VertexData(
                Integer.parseInt(values[1]));
        }
        @Override
        protected Iterable< Edge<LongWritable, ssspVertex.EdgeData> > getEdges(String[] values) throws IOException {
            List< Edge<LongWritable, ssspVertex.EdgeData> > edges = Lists.newLinkedList();
            for (int i = 2; i < values.length; i += 2) {
                LongWritable edgeId = new LongWritable(Long.parseLong(values[i]));
                edges.add(new DefaultEdge<LongWritable, ssspVertex.EdgeData>(edgeId, new ssspVertex.EdgeData(
                            Integer.parseInt(values[i+1]))));
            }
            return edges;
        }
    }
}
