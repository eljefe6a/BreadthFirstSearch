import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * A Breadth First Search implemention of the reducer
 * 
 * @author Jesse Anderson
 * 
 */
public class BreadthFirstSearchMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	private static final Logger LOGGER = Logger.getLogger("breadthfirstsearch");

	/** Regular expression to parse the mapper input value */
	private static final Pattern pattern = Pattern.compile("\\((.*)\\),([-]?\\d*)");

	/**
	 * Creates the map output for the breadth first search
	 * 
	 * @param key
	 *            The name of the node that is being reduced
	 * @param values
	 *            The values contained in the input. These value is an adjacency list followed by the known or unknown
	 *            (-1) distance
	 * @param output
	 *            These values are either distances to the desired node or the adjacency list
	 * @param reporter
	 *            The reporter object <br>
	 * <br>
	 *            PRE-CONDITION (adjacency list): A (B,D),0<br>
	 *            POST-CONDITION (adjacency list): A B,C<br>
	 *            POST-CONDITION (distance): A 2<br>
	 */
	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Matcher matcher = pattern.matcher(value.toString());

		// Verify input data matches the expected Regular Expression
		if (!matcher.matches()) {
			LOGGER.error("Input does not match regex.  Input was \"" + value.toString() + "\"");
			return;
		}

		// Pull the values out of the Regular Expression groups
		String adjacencies = matcher.group(1);
		long distance = Integer.parseInt(matcher.group(2));

		// Emit adjacencies
		output.collect(key, new Text(adjacencies));
		LOGGER.info("Mapper Emitting " + key + " adjacencies " + adjacencies);

		// Emit distance from yourself
		output.collect(key, new Text(String.valueOf(distance)));
		LOGGER.info("  Self distance " + key + " " + distance);

		// Figure out the distance for the rest of the adjacent nodes
		long adjacencyDistance;

		if (distance != BreadthFirstSearchDriver.UNKNOWN_DISTANCE) {
			adjacencyDistance = distance + 1;
		} else {
			adjacencyDistance = BreadthFirstSearchDriver.UNKNOWN_DISTANCE;
		}

		// Create an iterable list of nodes to emit
		String[] adjacenciesArray = adjacencies.split(",");

		// Emit newly found distances
		for (String adjacency : adjacenciesArray) {
			if (adjacency.length() != 0) {
				output.collect(new Text(adjacency), new Text(String.valueOf(adjacencyDistance)));
				LOGGER.info("  Adjacency distance " + adjacency + " " + adjacencyDistance);
			}
		}
	}
}