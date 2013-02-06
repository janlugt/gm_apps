import java.io.IOException;
import java.util.List;
import org.apache.giraph.graph.*;
import org.apache.giraph.io.formats.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.google.common.collect.Lists;

public class avg_teen_cntVertexInputFormat extends TextVertexInputFormat<LongWritable, avg_teen_cntVertex.VertexData, NullWritable, avg_teen_cntVertex.MessageData> {
    int intype;
    @Override
    public TextVertexReader
    createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        intype = context.getConfiguration().getInt("GMInputFormat",avg_teen_cnt.GM_FORMAT_ADJ);
        return new avg_teen_cntVertexReader();
    }

    //--------------------------------------
    // Input format is assumed as follows:
    // <vertex_idlong> <age(int)> <teen_cnt(int)> {<dest_id(long)> }*
    private class avg_teen_cntVertexReader extends TextVertexInputFormat<LongWritable, avg_teen_cntVertex.VertexData, NullWritable, avg_teen_cntVertex.MessageData>.TextVertexReaderFromEachLineProcessed<String[]> {

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
        protected avg_teen_cntVertex.VertexData getValue(String[] values) throws IOException {
            return new avg_teen_cntVertex.VertexData(
                Integer.parseInt(values[1]), Integer.parseInt(values[2]));
        }
        @Override
        protected Iterable< Edge<LongWritable, NullWritable> > getEdges(String[] values) throws IOException {
            List< Edge<LongWritable, NullWritable> > edges = Lists.newLinkedList();
            for (int i = 3; i < values.length; i += 1) {
                LongWritable edgeId = new LongWritable(Long.parseLong(values[i]));
                edges.add(new DefaultEdge<LongWritable, NullWritable>(edgeId, NullWritable.get()));
            }
            return edges;
        }
    }
}
