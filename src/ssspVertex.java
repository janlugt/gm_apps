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

public class ssspVertex
    extends EdgeListVertex< LongWritable, ssspVertex.VertexData, ssspVertex.EdgeData, ssspVertex.MessageData > {

    // Vertex logger
    private static final Logger LOG = Logger.getLogger(ssspVertex.class);

    // Keys for shared_variables 
    private static final String KEY_root = "root";
    private static final String KEY___E8 = "__E8";

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
            registerPersistentAggregator(KEY___E8, BooleanOrAggregator.class);
        }

        //save output
        public void writeOutput() {
        }

        //----------------------------------------------------------
        // Scalar Variables 
        //----------------------------------------------------------
        private boolean __E8;
        private boolean fin;
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
                    case 4: _master_state_4(); break;
                    case 5: _master_state_5(); break;
                    case 14: _master_state_14(); break;
                    case 10: _master_state_10(); break;
                    case 15: _master_state_15(); break;
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
            fin = False;
            -----*/
            LOG.info("Running _master_state 0");

            fin = false ;
            _master_state_nxt = 2;
        }
        private void _master_state_2() {
            /*------
            Foreach (t0 : G.Nodes)
            {
                t0.dist = (t0 == root)  ? 0 : +INF;
                t0.updated = (t0 == root)  ? True : False;
                t0.dist_nxt = t0.dist;
                t0.updated_nxt = t0.updated;
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

            _master_state_nxt = 4;
        }
        private void _master_state_4() {
            /*------
            !fin
            -----*/
            LOG.info("Running _master_state 4");
            // While (...)

            boolean _expression_result =  !fin;
            if (_expression_result) _master_state_nxt = 5;
            else _master_state_nxt = 7;

            if (!_expression_result) _is_first_4=true; // reset is_first


        }
        private void _master_state_5() {
            /*------
            fin = True;
            __E8 = False;
            -----*/
            LOG.info("Running _master_state 5");

            fin = true ;
            __E8 = false ;
            _master_state_nxt = 14;
        }
        private void _master_state_14() {
            /*------
            //Receive Nested Loop
            Foreach (s : n.Nbrs)
            {
                e = s.ToEdge();
                _m9 = n.dist + e.len;
                <s.dist_nxt ; s.updated_nxt> min= <_m9 ; True> @ n ;
            }
            Foreach (t4 : G.Nodes)
            {
                t4.dist = t4.dist_nxt;
                t4.updated = t4.updated_nxt;
                t4.updated_nxt = False;
                __E8 |= t4.updated @ t4 ;
            }

            Foreach (n : G.Nodes)
            {
                If (n.updated)
                {
                    Foreach (s : n.Nbrs)
                    {
                        e = s.ToEdge();
                        _m9 = n.dist + e.len;
                        <s.dist_nxt ; s.updated_nxt> min= <_m9 ; True> @ n ;
                    }
                }
            }
            -----*/
            LOG.info("Running _master_state 14");
            setAggregatedValue(KEY___E8, new BooleanWritable(false));
            setAggregatedValue("__gm_gps_state", new IntWritable(_master_state));
            setAggregatedValue("_is_first_4", new BooleanWritable(_is_first_4));

            _master_state_nxt = 10;
            _master_should_start_workers = true;
        }
        private void _master_state_10() {
            /*------
            fin = !__E8;
            -----*/
            LOG.info("Running _master_state 10");
            __E8 = __E8||this.<BooleanWritable>getAggregatedValue(KEY___E8).get();

            if (!_is_first_4) {
                fin =  !__E8 ;
            }
            _master_state_nxt = 15;
        }
        private void _master_state_15() {
            /*------
            -----*/
            LOG.info("Running _master_state 15");
            // Intra-Loop Merged
            if (_is_first_4) _master_state_nxt = 5;
            else _master_state_nxt = 4;
            _is_first_4 = false;

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
            out.writeBoolean(__E8);
            out.writeBoolean(fin);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            _master_state = in.readInt();
            _master_state_nxt = in.readInt();
            _master_should_start_workers = in.readBoolean();
            _master_should_finish = in.readBoolean();
            __E8 = in.readBoolean();
            fin = in.readBoolean();
        }
    } // end of mastercompute

    @Override
    public void compute(Iterable<MessageData> _msgs) {
        int _state_vertex = this.<IntWritable>getAggregatedValue("__gm_gps_state").get();
        switch(_state_vertex) { 
            case 2: _vertex_state_2(_msgs); break;
            case 14: _vertex_state_14(_msgs); break;
        }
    }
    private void _vertex_state_2(Iterable<MessageData> _msgs) {
        VertexData _this = getValue();
        long root = getConf().getLong("root", -1L);
        /*------
            Foreach (t0 : G.Nodes)
            {
                t0.dist = (t0 == root)  ? 0 : +INF;
                t0.updated = (t0 == root)  ? True : False;
                t0.dist_nxt = t0.dist;
                t0.updated_nxt = t0.updated;
            }
        -----*/

        {
            _this.dist = (getId().get() == root)?0:Integer.MAX_VALUE ;
            _this.updated = (getId().get() == root)?true:false ;
            _this.dist_nxt = _this.dist ;
            _this.updated_nxt = _this.updated ;
        }
    }
    private void _vertex_state_14(Iterable<MessageData> _msgs) {
        VertexData _this = getValue();
        int _m9;
        boolean _is_first_4 = this.<BooleanWritable>getAggregatedValue("_is_first_4").get();
        if (!_is_first_4) {
            // Begin msg receive
            for (MessageData _msg : _msgs) {
                /*------
                (Nested Loop)
                    Foreach (s : n.Nbrs)
                    {
                        e = s.ToEdge();
                        _m9 = n.dist + e.len;
                        <s.dist_nxt ; s.updated_nxt> min= <_m9 ; True> @ n ;
                    }
                -----*/
                _m9 = _msg.i0;
                if (_this.dist_nxt > _m9) {
                    _this.dist_nxt = _m9;
                    _this.updated_nxt = true;
                }
            }
        }

        /*------
            Foreach (t4 : G.Nodes)
            {
                t4.dist = t4.dist_nxt;
                t4.updated = t4.updated_nxt;
                t4.updated_nxt = False;
                __E8 |= t4.updated @ t4 ;
            }

            Foreach (n : G.Nodes)
            {
                If (n.updated)
                {
                    Foreach (s : n.Nbrs)
                    {
                        e = s.ToEdge();
                        _m9 = n.dist + e.len;
                        <s.dist_nxt ; s.updated_nxt> min= <_m9 ; True> @ n ;
                    }
                }
            }
        -----*/

        if (!_is_first_4)
        {
            _this.dist = _this.dist_nxt ;
            _this.updated = _this.updated_nxt ;
            _this.updated_nxt = false ;
            aggregate(KEY___E8, new BooleanWritable(_this.updated));
        }

        {
            if (_this.updated)
            {
                // Sending messages to each neighbor
                for (Edge<LongWritable, EdgeData> edge : getEdges()) {
                    LongWritable _neighborId = edge.getTargetVertexId();
                    EdgeData _outEdgeData = edge.getValue();
                    MessageData _msg = new MessageData((byte) 0);
                    _m9 = _this.dist + _outEdgeData.len ;
                    _msg.i0 = _m9;
                    sendMessage(_neighborId, _msg);
                }
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
        public int dist;
        public boolean updated;
        public boolean updated_nxt;
        public int dist_nxt;

        public VertexData() {
            // Default constructor needed for Giraph
        }

        // Initalize with initData
        public VertexData(int __i0) {
            dist = __i0; 
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(dist);
            out.writeBoolean(updated);
            out.writeBoolean(updated_nxt);
            out.writeInt(dist_nxt);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            dist = in.readInt();
            updated = in.readBoolean();
            updated_nxt = in.readBoolean();
            dist_nxt = in.readInt();
        }
        @Override
        public String toString() {
            // Implement output fields here for VertexOutputWriter
            return "1.0";
        }
    } // end of vertex property class

    //----------------------------------------------
    // Edge Property Class
    //----------------------------------------------
    public static class EdgeData implements Writable {
        // properties
        public int len;

        public EdgeData() {
            // Default constructor needed for Giraph
        }

        // Initalize with initData
        public EdgeData(int __i0) {
            len = __i0; 
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(len);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            len = in.readInt();
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
        int i0;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(i0);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            i0 = in.readInt();
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
