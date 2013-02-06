import java.io.IOException;
import java.util.List;
import org.apache.giraph.graph.*;
import org.apache.giraph.io.formats.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.google.common.collect.Lists;

public class pagerankVertexInputFormat extends TextVertexInputFormat<LongWritable, pagerankVertex.VertexData, NullWritable, pagerankVertex.MessageData> {
    int intype;
    @Override
    public TextVertexReader
    createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        intype = context.getConfiguration().getInt("GMInputFormat",pagerank.GM_FORMAT_ADJ);
        return new pagerankVertexReader();
    }

    //--------------------------------------
    // Input format is assumed as follows:
    // <vertex_idlong> {<dest_id(long)> }*
    private class pagerankVertexReader extends TextVertexInputFormat<LongWritable, pagerankVertex.VertexData, NullWritable, pagerankVertex.MessageData>.TextVertexReaderFromEachLineProcessed<String[]> {

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
        protected pagerankVertex.VertexData getValue(String[] values) throws IOException {
            return new pagerankVertex.VertexData();
        }
        @Override
        protected Iterable< Edge<LongWritable, NullWritable> > getEdges(String[] values) throws IOException {
            List< Edge<LongWritable, NullWritable> > edges = Lists.newLinkedList();
            for (int i = 1; i < values.length; i += 1) {
                LongWritable edgeId = new LongWritable(Long.parseLong(values[i]));
                edges.add(new DefaultEdge<LongWritable, NullWritable>(edgeId, NullWritable.get()));
            }
            return edges;
        }
    }
}
