import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * A Breadth First Search implemention of the reducer
 * 
 * @author Jesse Anderson
 *
 */
public class BreadthFirstSearchReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public static final Logger LOGGER = Logger.getLogger("breadthfirstsearch");

	/**
	 * Reduces the breadth first search map output
	 * 
	 * @param key
	 *            The name of the node that is being reduced
	 * @param values
	 *            The values emitted by the mapper. These values are either distances to the desired node or the
	 *            adjacency list
	 * @param output
	 *            The name of the node with the smallest known distance or unknown
	 * @param reporter
	 *            The reporter object <br>
	 * <br>
	 *            PRE-CONDITION (adjacency list): A B,C<br>
	 *            PRE-CONDITION (distance): A 2<br>
	 *            POST-CONDITION: A (B,C),2
	 */
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String adjacencyList = null;

		long smallestDistance = BreadthFirstSearchDriver.UNKNOWN_DISTANCE;

		StringBuilder distanceValues = new StringBuilder();

		Long currentDistance;

		while (values.hasNext()) {
			Text nextValue = values.next();

			if ((currentDistance = parseValue(nextValue.toString())) != null) {
				// The value passed in is a distance emitted by the mapper

				// Append the distance so we can log it later
				distanceValues.append(nextValue.toString()).append(",");

				// Process the distance to see if we know enough about it
				if (currentDistance == BreadthFirstSearchDriver.UNKNOWN_DISTANCE) {
					// The distance is still unknown
					continue;
				} else if (smallestDistance == BreadthFirstSearchDriver.UNKNOWN_DISTANCE
						&& currentDistance != BreadthFirstSearchDriver.UNKNOWN_DISTANCE) {
					// Our first real distance came in, save it to compare against other distances
					smallestDistance = currentDistance;
				} else if (currentDistance < smallestDistance) {
					// A smaller distance came in, save it to keep comparing
					smallestDistance = currentDistance;
				}
			} else {
				// The value passed in is the adjacency list emitted by the mapper
				// Save this list to emit once we know the smallest distance
				adjacencyList = nextValue.toString();
			}
		}

		output.collect(key, new Text("(" + adjacencyList + ")," + smallestDistance));
		LOGGER.info("Reducer Key Passed in for " + key + " Values " + distanceValues.toString());
		LOGGER.info("  Emitting " + key + " " + adjacencyList + " " + smallestDistance);
	}

	/**
	 * Parses a long value in a String
	 * 
	 * @param value
	 *            The value to parse
	 * @return The String parsed to long or null if the value is not a long
	 */
	private Long parseValue(String value) {
		try {
			long currentDistance = Long.parseLong(value);

			return currentDistance;
		} catch (Exception e) {
			return null;
		}
	}
}
