import java.io.BufferedInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A Breadth First Search implementation of the driver<br>
 * <br>
 * This example favors straightforward, readable, and easy to read log output than production worthy code.<br>
 * This includes things like:
 * <ul>
 * <li>Too much log output</li>
 * <li>Not using a custom WritableComparable</li>
 * <li>Not using counters to stop the driver loop</li>
 * </ul>
 * <br>
 * Refer to graphdata-5simple for the input file format.
 * 
 * @author Jesse Anderson
 * 
 */
public class BreadthFirstSearchDriver extends Configured implements Tool {
	private static final Logger LOGGER = Logger.getLogger("breadthfirstsearch");

	/* The constant defining an unknown distance between nodes */
	public static final long UNKNOWN_DISTANCE = -1;

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			throw new IllegalArgumentException("Incorrect number of arguments: Got: " + args.length
					+ " Want: 1 argument that is path to input data.");
		}

		// Instantiate some variables to keep to track of state
		String input = args[0];
		String firstInput = input;
		int iteration = 1;
		String output = firstInput + "-" + iteration;

		// Change Log4J to only output the map and reducer data
		// This helps view what's happening with the data easier
		// Comment this out to get all log data
		Logger.getLogger("breadthfirstsearch").setLevel(Level.INFO);
		Logger.getRootLogger().setLevel(Level.ERROR);

		boolean reduceOutputsMatch = false;

		do {
			// Boilerplate MapReduce job setup
			JobConf conf = new JobConf(getConf(), BreadthFirstSearchDriver.class);
			conf.setJobName("Parallel Breadth First Search");
			conf.setMapperClass(BreadthFirstSearchMapper.class);
			conf.setReducerClass(BreadthFirstSearchReducer.class);
			conf.setInputFormat(KeyValueTextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			JobClient.runJob(conf);
			LOGGER.info("**Iteration " + iteration + ". Output in " + output + "**");

			// Check to see if nothing has changed between iterations
			// That means all nodes have been processed as much as possible
			// NOTE: This assumes a single reducer and in production this should use
			// counters. Let's keep this example simple.
			reduceOutputsMatch = compareInputAndReducerFiles(input, output, conf);

			iteration++;
			input = output;
			output = firstInput + iteration;
		} while (!reduceOutputsMatch);

		LOGGER.info(iteration - 1 + " iterations to compute shortest path.");
		return 0;
	}

	/**
	 * Compares the input and output files byte by byte
	 * 
	 * @param input
	 *            The input file to compare
	 * @param output
	 *            The output file to compare
	 * @param conf
	 *            The JobConf
	 * @return The false if the 2 files differ
	 * @throws IOException
	 */
	private boolean compareInputAndReducerFiles(String input, String output, JobConf conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		Path inputPath = new Path(input, "part-00000");

		if (!fileSystem.exists(inputPath)) {
			inputPath = new Path(input);
		}

		BufferedInputStream inputStream = new BufferedInputStream(fileSystem.open(inputPath));
		BufferedInputStream reduceStream = new BufferedInputStream(fileSystem.open(new Path(output, "part-00000")));

		try {
			while (true) {
				int inputByte = inputStream.read();
				int reduceByte = reduceStream.read();

				if (inputByte != reduceByte) {
					return false;
				}

				if (inputByte == -1) {
					return true;
				}
			}
		} finally {
			inputStream.close();
			reduceStream.close();
		}
	}

	public static void main(String[] args) throws Exception {
		BreadthFirstSearchDriver driver = new BreadthFirstSearchDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}