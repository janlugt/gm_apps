import java.io.IOException;
import java.util.List;
import org.apache.giraph.graph.*;
import org.apache.giraph.io.formats.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.google.common.collect.Lists;

public class hop_distVertexInputFormat extends TextVertexInputFormat<LongWritable, hop_distVertex.VertexData, NullWritable, hop_distVertex.MessageData> {
    int intype;
    @Override
    public TextVertexReader
    createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        intype = context.getConfiguration().getInt("GMInputFormat",hop_dist.GM_FORMAT_ADJ);
        return new hop_distVertexReader();
    }

    //--------------------------------------
    // Input format is assumed as follows:
    // <vertex_idlong> <dist(int)> {<dest_id(long)> }*
    private class hop_distVertexReader extends TextVertexInputFormat<LongWritable, hop_distVertex.VertexData, NullWritable, hop_distVertex.MessageData>.TextVertexReaderFromEachLineProcessed<String[]> {

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
        protected hop_distVertex.VertexData getValue(String[] values) throws IOException {
            return new hop_distVertex.VertexData(
                Integer.parseInt(values[1]));
        }
        @Override
        protected Iterable< Edge<LongWritable, NullWritable> > getEdges(String[] values) throws IOException {
            List< Edge<LongWritable, NullWritable> > edges = Lists.newLinkedList();
            for (int i = 2; i < values.length; i += 1) {
                LongWritable edgeId = new LongWritable(Long.parseLong(values[i]));
                edges.add(new DefaultEdge<LongWritable, NullWritable>(edgeId, NullWritable.get()));
            }
            return edges;
        }
    }
}
