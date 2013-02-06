package manual;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.vertex.LongDoubleFloatDoubleVertex;
import org.apache.hadoop.io.DoubleWritable;

public class pagerankVertex extends LongDoubleFloatDoubleVertex {
	public static final String GLOBAL_DIFF = "global_diff";
	public static final double E = 0.001, D = 0.85;
	public static final int MAX_SUPERSTEPS = 100;

	static class MasterCompute extends org.apache.giraph.master.MasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(GLOBAL_DIFF, DoubleSumAggregator.class);
		}

		@Override
		public void compute() {
			// Don't do anything for the first two supersteps
			if (getSuperstep() >= 3 && (
					this.<DoubleWritable> getAggregatedValue(GLOBAL_DIFF).get() < E
					|| getSuperstep() >= MAX_SUPERSTEPS)) {
				haltComputation();
			} else {
				setAggregatedValue(GLOBAL_DIFF, new DoubleWritable(0.0));
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
		}

		@Override
		public void write(DataOutput out) throws IOException {
		}
	}

	private enum PageRankState {
		WAITING, INITIALIZING_VALUES, RUNNING
	}

	private PageRankState getState() {
		if (getSuperstep() == 0) {
			return PageRankState.WAITING;
		} else if (getSuperstep() == 1) {
			return PageRankState.INITIALIZING_VALUES;
		} else {
			return PageRankState.RUNNING;
		}
	}

	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
		switch (getState()) {
		case WAITING:
			// do nothing
			break;
		case INITIALIZING_VALUES:
			setValue(new DoubleWritable(1.0 / getTotalNumVertices()));
			sendPageRankMessages();
			break;
		case RUNNING:
			processMessages(messages);
			sendPageRankMessages();
			break;
		}
	}

	private void sendPageRankMessages() {
		DoubleWritable pagerankValue = new DoubleWritable(getValue().get()
				/ getNumEdges());
		sendMessageToAllEdges(pagerankValue);
	}

	private void processMessages(Iterable<DoubleWritable> messages) {
		double oldValue = getValue().get();

		double sum = 0.0;
		for (DoubleWritable m : messages) {
			sum += m.get();
		}
		double newValue = (1.0 - D) / getTotalNumVertices() + D * sum;
		getValue().set(newValue);

		aggregate(GLOBAL_DIFF,
				new DoubleWritable(Math.abs(newValue - oldValue)));
	}
}
