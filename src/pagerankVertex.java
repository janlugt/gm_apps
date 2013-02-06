import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.Random;
import org.apache.giraph.aggregators.*;
import org.apache.giraph.graph.*;
import org.apache.giraph.master.*;
import org.apache.giraph.vertex.*;
import org.apache.giraph.worker.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

@SuppressWarnings("unused")

public class pagerankVertex
    extends EdgeListVertex< LongWritable, pagerankVertex.VertexData, NullWritable, pagerankVertex.MessageData > {

    // Vertex logger
    private static final Logger LOG = Logger.getLogger(pagerankVertex.class);

    // Keys for shared_variables 
    private static final String KEY_d = "d";
    private static final String KEY_diff = "diff";
    private static final String KEY_N = "N";

    //----------------------------------------------
    // MasterCompute Class
    //----------------------------------------------
    public static class Master extends MasterCompute {
        // Control fields
        private int     _master_state                = 0;
        private int     _master_state_nxt            = 0;
        private boolean _master_should_start_workers = false;
        private boolean _master_should_finish        = false;

        public void initialize() throws InstantiationException, IllegalAccessException {
            registerPersistentAggregator("__gm_gps_state", IntOverwriteAggregator.class);
            registerPersistentAggregator("_is_first_4", BooleanOverwriteAggregator.class);
            registerPersistentAggregator(KEY_diff, DoubleSumAggregator.class);
            registerPersistentAggregator(KEY_N, DoubleOverwriteAggregator.class);
            e = getContext().getConfiguration().getFloat("e", -1.0f);
            max = getContext().getConfiguration().getInt("max", -1);
        }

        //save output
        public void writeOutput() {
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private double e;
        private int max;
        private double diff;
        private int cnt;
        private double N;
        private boolean _is_first_4 = true;

        //----------------------------------------------------------
        // Master's State-machine 
        //----------------------------------------------------------
        private void _master_state_machine() {
            _master_should_start_workers = false;
            _master_should_finish = false;
            do {
                _master_state = _master_state_nxt ;
                switch(_master_state) {
                    case 0: _master_state_0(); break;
                    case 2: _master_state_2(); break;
                    case 3: _master_state_3(); break;
                    case 8: _master_state_8(); break;
                    case 16: _master_state_16(); break;
                    case 12: _master_state_12(); break;
                    case 17: _master_state_17(); break;
                    case 4: _master_state_4(); break;
                    case 7: _master_state_7(); break;
                }
            } while (!_master_should_start_workers && !_master_should_finish);
        }

        //@ Override
        public void compute() {
            // Graph size is not available in superstep 0
            if (getSuperstep() == 0) {
                return;
            }

            _master_state_machine();

            if (_master_should_finish) { 
                haltComputation();
                writeOutput();
            }
        }

        private void _master_state_0() {
            /*------
            cnt = 0;
            N =  (Double ) G.NumNodes();
            -----*/
            LOG.info("Running _master_state 0");

            cnt = 0 ;
            N = (double)(getTotalNumVertices()) ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (t0 : G.Nodes)
                t0.pg_rank = 1 / N;
            -----*/
            LOG.info("Running _master_state 2");
            setAggregatedValue(KEY_N, new DoubleWritable(N));
            setAggregatedValue("__gm_gps_state", new IntWritable(_master_state));

            _master_state_nxt = 3;
            _master_should_start_workers = true;
        }
        private void _master_state_3() {
            /*------
            -----*/
            LOG.info("Running _master_state 3");

            _master_state_nxt = 8;
        }
        private void _master_state_8() {
            /*------
            diff = 0.000000;
            -----*/
            LOG.info("Running _master_state 8");

            diff = (float)(0.000000) ;
            _master_state_nxt = 16;
        }
        private void _master_state_16() {
            /*------
            //Receive Nested Loop
            Foreach (t : w.Nbrs)
            {
                _m4 = w.pg_rank /  (Double ) w.OutDegree();
                t.__S1prop += _m4 @ w ;
            }
            Foreach (t : G.Nodes)
            {
                val = (1 - d)  / N + d * t.__S1prop;
                diff +=  | (val - t.pg_rank)  |  @ t ;
                t.pg_rank_nxt = val;
                t.pg_rank = t.pg_rank_nxt;
            }

            Foreach (t : G.Nodes)
            {
                t.__S1prop = 0.000000;
            }

            Foreach (w : G.Nodes)
            {
                Foreach (t : w.Nbrs)
                {
                    _m4 = w.pg_rank /  (Double ) w.OutDegree();
                    t.__S1prop += _m4 @ w ;
                }
            }
            -----*/
            LOG.info("Running _master_state 16");
            setAggregatedValue(KEY_diff, new DoubleWritable(0));
            setAggregatedValue(KEY_N, new DoubleWritable(N));
            setAggregatedValue("__gm_gps_state", new IntWritable(_master_state));
            setAggregatedValue("_is_first_4", new BooleanWritable(_is_first_4));

            _master_state_nxt = 12;
            _master_should_start_workers = true;
        }
        private void _master_state_12() {
            /*------
            cnt = cnt + 1;
            -----*/
            LOG.info("Running _master_state 12");
            diff = diff+this.<DoubleWritable>getAggregatedValue(KEY_diff).get();

            if (!_is_first_4) {
                cnt = cnt + 1 ;
            }
            _master_state_nxt = 17;
        }
        private void _master_state_17() {
            /*------
            -----*/
            LOG.info("Running _master_state 17");
            // Intra-Loop Merged
            if (_is_first_4) _master_state_nxt = 16;
            else _master_state_nxt = 4;
            _is_first_4 = false;

        }
        private void _master_state_4() {
            /*------
            (diff > e)  && (cnt < max) 
            -----*/
            LOG.info("Running _master_state 4");
            // Do-While(...)

            boolean _expression_result = (diff > e) && (cnt < max);
            if (_expression_result) _master_state_nxt = 8;
            else _master_state_nxt = 7;

            if (!_expression_result) _is_first_4=true; // reset is_first


        }
        private void _master_state_7() {
            /*------
            -----*/
            LOG.info("Running _master_state 7");

            _master_should_finish = true;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(_master_state);
            out.writeInt(_master_state_nxt);
            out.writeBoolean(_master_should_start_workers);
            out.writeBoolean(_master_should_finish);
            out.writeDouble(e);
            out.writeInt(max);
            out.writeDouble(diff);
            out.writeInt(cnt);
            out.writeDouble(N);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            _master_state = in.readInt();
            _master_state_nxt = in.readInt();
            _master_should_start_workers = in.readBoolean();
            _master_should_finish = in.readBoolean();
            e = in.readDouble();
            max = in.readInt();
            diff = in.readDouble();
            cnt = in.readInt();
            N = in.readDouble();
        }
    } // end of mastercompute

    @Override
    public void compute(Iterable<MessageData> _msgs) {
        int _state_vertex = this.<IntWritable>getAggregatedValue("__gm_gps_state").get();
        switch(_state_vertex) { 
            case 2: _vertex_state_2(_msgs); break;
            case 16: _vertex_state_16(_msgs); break;
        }
    }
    private void _vertex_state_2(Iterable<MessageData> _msgs) {
        VertexData _this = getValue();
        double N = this.<DoubleWritable> getAggregatedValue(KEY_N).get();
        /*------
            Foreach (t0 : G.Nodes)
                t0.pg_rank = 1 / N;
        -----*/

        _this.pg_rank = 1 / N ;
    }
    private void _vertex_state_16(Iterable<MessageData> _msgs) {
        VertexData _this = getValue();
        double d = getConf().getFloat("d", -1.0f);
        double N = this.<DoubleWritable> getAggregatedValue(KEY_N).get();
        double val;
        double _m4;
        boolean _is_first_4 = this.<BooleanWritable>getAggregatedValue("_is_first_4").get();
        if (!_is_first_4) {
            // Begin msg receive
            for (MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                    Foreach (t : w.Nbrs)
                    {
                        _m4 = w.pg_rank /  (Double ) w.OutDegree();
                        t.__S1prop += _m4 @ w ;
                    }
                -----*/
                _m4 = _msg.d0;
                _this.__S1prop = _this.__S1prop + (_m4);
            }
        }

        /*------
            Foreach (t : G.Nodes)
            {
                val = (1 - d)  / N + d * t.__S1prop;
                diff +=  | (val - t.pg_rank)  |  @ t ;
                t.pg_rank_nxt = val;
                t.pg_rank = t.pg_rank_nxt;
            }

            Foreach (t : G.Nodes)
            {
                t.__S1prop = 0.000000;
            }

            Foreach (w : G.Nodes)
            {
                Foreach (t : w.Nbrs)
                {
                    _m4 = w.pg_rank /  (Double ) w.OutDegree();
                    t.__S1prop += _m4 @ w ;
                }
            }
        -----*/

        if (!_is_first_4)
        {
            val = (1 - d) / N + d * _this.__S1prop ;
            aggregate(KEY_diff, new DoubleWritable(Math.abs((val - _this.pg_rank))));
            _this.pg_rank_nxt = val ;
            _this.pg_rank = _this.pg_rank_nxt ;
        }

        {
            _this.__S1prop = (float)(0.000000) ;
        }

        {
            // Sending messages to all neighbors (if there is a neighbor)
            if (getNumEdges() > 0) {
                MessageData _msg = new MessageData((byte) 0);
                _m4 = _this.pg_rank / ((double)(getNumEdges())) ;
                _msg.d0 = _m4;
                sendMessageToAllEdges(_msg);
            }
        }
    }

    //----------------------------------------------
    // WorkerContext Class
    //----------------------------------------------
    public static class Context extends WorkerContext {

        @Override
        public void preApplication() throws InstantiationException, IllegalAccessException {
        }

        @Override
        public void postApplication() {
        }

        @Override
        public void preSuperstep() {
        }

        @Override
        public void postSuperstep() {
        }

    } // end of workercontext

    //----------------------------------------------
    // Vertex Property Class
    //----------------------------------------------
    public static class VertexData implements Writable {
        // properties
        public double pg_rank;
        public double pg_rank_nxt;
        public double __S1prop;

        public VertexData() {
            // Default constructor needed for Giraph
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(pg_rank);
            out.writeDouble(pg_rank_nxt);
            out.writeDouble(__S1prop);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            pg_rank = in.readDouble();
            pg_rank_nxt = in.readDouble();
            __S1prop = in.readDouble();
        }
        @Override
        public String toString() {
            // Implement output fields here for VertexOutputWriter
            return "1.0";
        }
    } // end of vertex property class

    //----------------------------------------------
    // Message Data 
    //----------------------------------------------
    public static class MessageData implements Writable {
        public MessageData() {}

        //single message type; argument ignored
        public MessageData(byte type) {}

        // union of all message fields  
        double d0;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(d0);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            d0 = in.readDouble();
        }
    } // end of message-data class

    public static long gmGetRandomVertex(long gsize) {
        if (gsize < 1 * 1024 * 1024 * 1024) 
        return (long) (new Random()).nextInt((int) gsize);
        else {
            while(true) {
                long l = (new Random()).nextLong(); 
                if (l < 0) l = l * -1; 
                if (l < gsize) return l;
            }
        }
    }
}
