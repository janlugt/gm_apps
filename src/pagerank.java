import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class pagerank implements Tool {
    // Class logger
    private static final Logger LOG = Logger.getLogger(pagerank.class);

    // Configuration
    private Configuration conf;

    // I/O File Format
    final static int GM_FORMAT_ADJ=0;
    final static int GM_FORMAT_NODE_PROP=1;

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
        options.addOption("_GMInputFormat", "GMInputFormat", true, "Input filetype (ADJ: adjacency list)");
        options.addOption("_GMOutputFormat", "GMOutputFormat", true, "Output filename (ADJ:adjacency list, NODE_PROP:node property)");
        options.addOption("_e", "e", true, "e");
        options.addOption("_d", "d", true, "d");
        options.addOption("_max", "max", true, "max");
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
        if (!cmd.hasOption("e")) {
            LOG.info("Need to set procedure argument (--e)");
            return -1;
        }
        if (!cmd.hasOption("d")) {
            LOG.info("Need to set procedure argument (--d)");
            return -1;
        }
        if (!cmd.hasOption("max")) {
            LOG.info("Need to set procedure argument (--max)");
            return -1;
        }
        int intype = GM_FORMAT_ADJ; // default in format
        int outtype = GM_FORMAT_ADJ; // default out format
        if (cmd.hasOption("GMInputFormat")){
            String s= cmd.getOptionValue("GMInputFormat");
            if (s.equals("ADJ")) intype = GM_FORMAT_ADJ;
            else {
                LOG.info("Invalid Input Format:"+s);
                formatter.printHelp(getClass().getName(), options, true);
                return -1;
            }
        }
        if (cmd.hasOption("GMOutputFormat")){
            String s= cmd.getOptionValue("GMOutputFormat");
            if (s.equals("ADJ")) outtype = GM_FORMAT_ADJ;
            else if (s.equals("NODE_PROP")) outtype = GM_FORMAT_NODE_PROP;
            else {
                LOG.info("Invalid Output Format:"+s);
                formatter.printHelp(getClass().getName(), options, true);
                return -1;
            }
        }

        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        job.getConfiguration().setInt(GiraphConfiguration.CHECKPOINT_FREQUENCY, 0);
        job.getConfiguration().setVertexClass(pagerankVertex.class);
        job.getConfiguration().setMasterComputeClass(pagerankVertex.Master.class);
        job.getConfiguration().setWorkerContextClass(pagerankVertex.Context.class);
        job.getConfiguration().setVertexInputFormatClass(pagerankVertexInputFormat.class);
        GiraphFileInputFormat.addVertexInputPath(job.getInternalJob(), new Path(cmd.getOptionValue('i')));
        if (cmd.hasOption('o')) {
            job.getConfiguration().setVertexOutputFormatClass(pagerankVertexOutputFormat.class);
            FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(cmd.getOptionValue('o')));
        }
        int workers = Integer.parseInt(cmd.getOptionValue('w'));
        job.getConfiguration().setWorkerConfiguration(workers, workers, 100.0f);
        job.getConfiguration().setInt("GMInputFormat", new Integer(intype));
        job.getConfiguration().setInt("GMOutputFormat", new Integer(outtype));
        job.getConfiguration().setFloat("e", Float.parseFloat(cmd.getOptionValue("e")));
        job.getConfiguration().setFloat("d", Float.parseFloat(cmd.getOptionValue("d")));
        job.getConfiguration().setInt("max", Integer.parseInt(cmd.getOptionValue("max")));

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
        System.exit(ToolRunner.run(new pagerank(), args));
    }
}
