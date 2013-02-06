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

public class avg_teen_cntVertex
    extends EdgeListVertex< LongWritable, avg_teen_cntVertex.VertexData, NullWritable, avg_teen_cntVertex.MessageData > {

    // Vertex logger
    private static final Logger LOG = Logger.getLogger(avg_teen_cntVertex.class);

    // Keys for shared_variables 
    private static final String KEY_K = "K";
    private static final String KEY___S2 = "__S2";
    private static final String KEY__cnt3 = "_cnt3";

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
            registerPersistentAggregator(KEY___S2, IntSumAggregator.class);
            registerPersistentAggregator(KEY__cnt3, LongSumAggregator.class);
        }

        //save output
        public void writeOutput() {
            System.out.println("_ret_value:\t" + _ret_value + "\n");
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private double _avg4;
        private float avg;
        private int __S2;
        private long _cnt3;
        private float _ret_value; // the final return value of the procedure

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
                    case 9: _master_state_9(); break;
                    case 5: _master_state_5(); break;
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
            __S2 = 0;
            _cnt3 = 0;
            -----*/
            LOG.info("Running _master_state 0");

            __S2 = 0 ;
            _cnt3 = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (n : G.Nodes)
            {
                n.__S1prop = 0;
            }

            Foreach (t : G.Nodes)
                If (((t.age >= 10)  && (t.age < 20) ) )
                {
                    Foreach (n : t.Nbrs)
                    {
                        n.__S1prop += 1 @ t ;
                    }
                }
            -----*/
            LOG.info("Running _master_state 2");
            setAggregatedValue("__gm_gps_state", new IntWritable(_master_state));

            _master_state_nxt = 3;
            _master_should_start_workers = true;
        }
        private void _master_state_3() {
            /*------
            -----*/
            LOG.info("Running _master_state 3");

            _master_state_nxt = 9;
        }
        private void _master_state_9() {
            /*------
            //Receive Nested Loop
            Foreach (n : t.Nbrs)
            {
                n.__S1prop += 1 @ t ;
            }
            Foreach (n : G.Nodes)
            {
                n.teen_cnt = n.__S1prop;
                If ((n.age > K) )
                {
                    __S2 += n.teen_cnt @ n ;
                    _cnt3 += 1 @ n ;
                }
            }
            -----*/
            LOG.info("Running _master_state 9");
            setAggregatedValue(KEY___S2, new IntWritable(0));
            setAggregatedValue(KEY__cnt3, new LongWritable(0));
            setAggregatedValue("__gm_gps_state", new IntWritable(_master_state));

            _master_state_nxt = 5;
            _master_should_start_workers = true;
        }
        private void _master_state_5() {
            /*------
            _avg4 = (0 == _cnt3)  ? 0.000000 : (__S2 /  (Double ) _cnt3) ;
            avg =  (Float ) _avg4;
            Return avg;
            -----*/
            LOG.info("Running _master_state 5");
            __S2 = __S2+this.<IntWritable>getAggregatedValue(KEY___S2).get();
            _cnt3 = _cnt3+this.<LongWritable>getAggregatedValue(KEY__cnt3).get();
            double _avg4;
            float avg;

            _avg4 = (0 == _cnt3)?(float)(0.000000):(__S2 / ((double)_cnt3)) ;
            avg = (float)_avg4 ;
            _ret_value = avg;
            _master_should_finish = true;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(_master_state);
            out.writeInt(_master_state_nxt);
            out.writeBoolean(_master_should_start_workers);
            out.writeBoolean(_master_should_finish);
            out.writeDouble(_avg4);
            out.writeFloat(avg);
            out.writeInt(__S2);
            out.writeLong(_cnt3);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            _master_state = in.readInt();
            _master_state_nxt = in.readInt();
            _master_should_start_workers = in.readBoolean();
            _master_should_finish = in.readBoolean();
            _avg4 = in.readDouble();
            avg = in.readFloat();
            __S2 = in.readInt();
            _cnt3 = in.readLong();
        }
    } // end of mastercompute

    @Override
    public void compute(Iterable<MessageData> _msgs) {
        int _state_vertex = this.<IntWritable>getAggregatedValue("__gm_gps_state").get();
        switch(_state_vertex) { 
            case 2: _vertex_state_2(_msgs); break;
            case 9: _vertex_state_9(_msgs); break;
        }
    }
    private void _vertex_state_2(Iterable<MessageData> _msgs) {
        VertexData _this = getValue();
        /*------
            Foreach (n : G.Nodes)
            {
                n.__S1prop = 0;
            }

            Foreach (t : G.Nodes)
                If (((t.age >= 10)  && (t.age < 20) ) )
                {
                    Foreach (n : t.Nbrs)
                    {
                        n.__S1prop += 1 @ t ;
                    }
                }
        -----*/

        {
            _this.__S1prop = 0 ;
        }

        if (((_this.age >= 10) && (_this.age < 20)))
        {
            // Sending messages to all neighbors (if there is a neighbor)
            if (getNumEdges() > 0) {
                MessageData _msg = new MessageData((byte) 0);
                sendMessageToAllEdges(_msg);
            }
        }
    }
    private void _vertex_state_9(Iterable<MessageData> _msgs) {
        VertexData _this = getValue();
        int K = getConf().getInt("K", -1);

        // Begin msg receive
        for (MessageData _msg : _msgs) {
            /*------
            (Nested Loop)
                Foreach (n : t.Nbrs)
                {
                    n.__S1prop += 1 @ t ;
                }
            -----*/
            _this.__S1prop = _this.__S1prop + (1);
        }

        /*------
            Foreach (n : G.Nodes)
            {
                n.teen_cnt = n.__S1prop;
                If ((n.age > K) )
                {
                    __S2 += n.teen_cnt @ n ;
                    _cnt3 += 1 @ n ;
                }
            }
        -----*/

        {
            _this.teen_cnt = _this.__S1prop ;
            if ((_this.age > K))
            {
                aggregate(KEY___S2, new IntWritable(_this.teen_cnt));
                aggregate(KEY__cnt3, new LongWritable(1));
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
        public int age;
        public int teen_cnt;
        public int __S1prop;

        public VertexData() {
            // Default constructor needed for Giraph
        }

        // Initalize with initData
        public VertexData(int __i0, int __i1) {
            age = __i0; 
            teen_cnt = __i1; 
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(age);
            out.writeInt(teen_cnt);
            out.writeInt(__S1prop);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            age = in.readInt();
            teen_cnt = in.readInt();
            __S1prop = in.readInt();
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

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte((byte)0); // empty message
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            in.readByte(); // consume empty message byte
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
