package manual;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.examples.SimpleShortestPathsVertex;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class sssp implements Tool {
    // Class logger
    private static final Logger LOG = Logger.getLogger(sssp.class);

    // Configuration
    private Configuration conf;

    //----------------------------------------------
    // Job Configuration
    //----------------------------------------------
    @Override
    public final int run(final String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "Help");
        options.addOption("v", "verbose", false, "Verbose");
        options.addOption("w", "workers", true, "Number of workers");
        options.addOption("i", "input", true, "Input filename");
        options.addOption("o", "output", true, "Output filename");
        options.addOption("_root", "root", true, "root");
        HelpFormatter formatter = new HelpFormatter();
        if (args.length == 0) {
            formatter.printHelp(getClass().getName(), options, true);
            return 0;
        }
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption('h')) {
            formatter.printHelp(getClass().getName(), options, true);
            return 0;
        }
        if (!cmd.hasOption('w')) {
            LOG.info("Need to choose the number of workers (-w)");
            return -1;
        }
        if (!cmd.hasOption('i')) {
            LOG.info("Need to set input path (-i)");
            return -1;
        }
        if (!cmd.hasOption("root")) {
            LOG.info("Need to set procedure argument (--root)");
            return -1;
        }

        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        job.getConfiguration().setInt(GiraphConfiguration.CHECKPOINT_FREQUENCY, 0);
        job.getConfiguration().setVertexClass(SimpleShortestPathsVertex.class);
        job.getConfiguration().setVertexInputFormatClass(LongDoubleFloatAdjacencyListVertexInputFormat.class);
        GiraphFileInputFormat.addVertexInputPath(job.getInternalJob(), new Path(cmd.getOptionValue('i')));
        if (cmd.hasOption('o')) {
            job.getConfiguration().setVertexOutputFormatClass(TextVertexOutputFormat.class);
            FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(cmd.getOptionValue('o')));
        }
        int workers = Integer.parseInt(cmd.getOptionValue('w'));
        job.getConfiguration().setWorkerConfiguration(workers, workers, 100.0f);
        job.getConfiguration().setLong(SimpleShortestPathsVertex.SOURCE_ID, Long.parseLong(cmd.getOptionValue("root")));

        boolean isVerbose = cmd.hasOption('v') ? true : false;
        if (job.run(isVerbose)) {
            return 0;
        } else {
            return -1;
        }
    } // end of job configuration

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new sssp(), args));
    }
}
