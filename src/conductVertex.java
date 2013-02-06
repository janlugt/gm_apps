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

public class conductVertex
    extends EdgeListVertex< LongWritable, conductVertex.VertexData, NullWritable, conductVertex.MessageData > {

    // Vertex logger
    private static final Logger LOG = Logger.getLogger(conductVertex.class);

    // Keys for shared_variables 
    private static final String KEY___S2 = "__S2";
    private static final String KEY___S3 = "__S3";
    private static final String KEY_num = "num";
    private static final String KEY___S4 = "__S4";

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
            registerPersistentAggregator(KEY___S3, IntSumAggregator.class);
            registerPersistentAggregator(KEY___S4, IntSumAggregator.class);
        }

        //save output
        public void writeOutput() {
            System.out.println("_ret_value:\t" + _ret_value + "\n");
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private int __S2;
        private int __S3;
        private float m;
        private int __S4;
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
                    case 4: _master_state_4(); break;
                    case 5: _master_state_5(); break;
                    case 3: _master_state_3(); break;
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
            __S3 = 0;
            __S4 = 0;
            -----*/
            LOG.info("Running _master_state 0");

            __S2 = 0 ;
            __S3 = 0 ;
            __S4 = 0 ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (u : G.Nodes)
            {
                If ((u.member == num) )
                    __S2 += u.Degree() @ u ;
                If ((u.member != num) )
                    __S3 += u.Degree() @ u ;
                If ((u.member == num) )
                {
                    Foreach (j : u.Nbrs)
                    {
                        If ((j.member != num) )
                            __S4 += 1 @ u ;
                    }
                }
            }
            -----*/
            LOG.info("Running _master_state 2");
            setAggregatedValue(KEY___S2, new IntWritable(0));
            setAggregatedValue(KEY___S3, new IntWritable(0));
            setAggregatedValue(KEY___S4, new IntWritable(0));
            setAggregatedValue("__gm_gps_state", new IntWritable(_master_state));

            _master_state_nxt = 4;
            _master_should_start_workers = true;
        }
        private void _master_state_4() {
            /*------
            -----*/
            LOG.info("Running _master_state 4");
            __S2 = __S2+this.<IntWritable>getAggregatedValue(KEY___S2).get();
            __S3 = __S3+this.<IntWritable>getAggregatedValue(KEY___S3).get();
            __S4 = __S4+this.<IntWritable>getAggregatedValue(KEY___S4).get();

            _master_state_nxt = 5;
        }
        private void _master_state_5() {
            /*------
            //Receive Nested Loop
            Foreach (j : u.Nbrs)
            {
                If ((j.member != num) )
                    __S4 += 1 @ u ;
            }
            -----*/
            LOG.info("Running _master_state 5");
            setAggregatedValue(KEY___S4, new IntWritable(0));
            setAggregatedValue("__gm_gps_state", new IntWritable(_master_state));

            _master_state_nxt = 3;
            _master_should_start_workers = true;
        }
        private void _master_state_3() {
            /*------
            m =  (Float ) ((__S2 < __S3)  ? __S2 : __S3);
            If (m == 0)
                Return (__S4 == 0)  ? 0.000000 : +INF;
            Else
                Return  (Float ) __S4 / m;
            -----*/
            LOG.info("Running _master_state 3");
            __S4 = __S4+this.<IntWritable>getAggregatedValue(KEY___S4).get();
            float m;

            m = (float)((__S2 < __S3)?__S2:__S3) ;
            if (m == 0)
                _ret_value = (__S4 == 0)?(float)(0.000000):Float.MAX_VALUE;
            else
                _ret_value = ((float)__S4) / m;
            _master_should_finish = true;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(_master_state);
            out.writeInt(_master_state_nxt);
            out.writeBoolean(_master_should_start_workers);
            out.writeBoolean(_master_should_finish);
            out.writeInt(__S2);
            out.writeInt(__S3);
            out.writeFloat(m);
            out.writeInt(__S4);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            _master_state = in.readInt();
            _master_state_nxt = in.readInt();
            _master_should_start_workers = in.readBoolean();
            _master_should_finish = in.readBoolean();
            __S2 = in.readInt();
            __S3 = in.readInt();
            m = in.readFloat();
            __S4 = in.readInt();
        }
    } // end of mastercompute

    @Override
    public void compute(Iterable<MessageData> _msgs) {
        int _state_vertex = this.<IntWritable>getAggregatedValue("__gm_gps_state").get();
        switch(_state_vertex) { 
            case 2: _vertex_state_2(_msgs); break;
            case 5: _vertex_state_5(_msgs); break;
        }
    }
    private void _vertex_state_2(Iterable<MessageData> _msgs) {
        VertexData _this = getValue();
        int num = getConf().getInt("num", -1);
        /*------
            Foreach (u : G.Nodes)
            {
                If ((u.member == num) )
                    __S2 += u.Degree() @ u ;
                If ((u.member != num) )
                    __S3 += u.Degree() @ u ;
                If ((u.member == num) )
                {
                    Foreach (j : u.Nbrs)
                    {
                        If ((j.member != num) )
                            __S4 += 1 @ u ;
                    }
                }
            }
        -----*/

        {
            if ((_this.member == num))
                aggregate(KEY___S2, new IntWritable(getNumEdges()));
            if ((_this.member != num))
                aggregate(KEY___S3, new IntWritable(getNumEdges()));
            if ((_this.member == num))
            {
                // Sending messages to all neighbors (if there is a neighbor)
                if (getNumEdges() > 0) {
                    MessageData _msg = new MessageData((byte) 0);
                    sendMessageToAllEdges(_msg);
                }
            }
        }
    }
    private void _vertex_state_5(Iterable<MessageData> _msgs) {
        VertexData _this = getValue();
        int num = getConf().getInt("num", -1);

        // Begin msg receive
        for (MessageData _msg : _msgs) {
            /*------
            (Nested Loop)
                Foreach (j : u.Nbrs)
                {
                    If ((j.member != num) )
                        __S4 += 1 @ u ;
                }
            -----*/
            if ((_this.member != num))
                aggregate(KEY___S4, new IntWritable(1));
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
        public int member;

        public VertexData() {
            // Default constructor needed for Giraph
        }

        // Initalize with initData
        public VertexData(int __i0) {
            member = __i0; 
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(member);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            member = in.readInt();
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
