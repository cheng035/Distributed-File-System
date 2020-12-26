import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StopWatch;
import org.apache.log4j.Logger;

public class TreeCount {

    static Logger log = Logger.getLogger(ContactTracing.class.getName());

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // Skip header line (first line) of CSV
            if(value.toString().startsWith("X,Y,OBJECTID")){
                return;
            }

            String[] data = value.toString().split(",", -1);
            String street = data[6];
            String condition = data[16];

            /*
            if (street.toUpperCase().equals("ABBEY FIELDS DR")){
                log.info(Arrays.toString(data));
                log.info(condition);
            }
            */

            // emit <Street, 1>
            if (condition.equals("Good") && !street.equals("")){
                word.set(street.toUpperCase());
                context.write(word, one);
            }


        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            // Iterate through the values for this key
            for (IntWritable val : values) {
                // sum up
                sum += 1;
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        StopWatch swatch = new StopWatch();
        swatch.start();
        Logger log = Logger.getLogger(ContactTracing.class.getName());
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        // Job job = new Job(conf, "TreeCount");

        job.setJarByClass(TreeCount.class);

        // Mapper, Combiner, Reducer
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0] + "City_Owned_Trees.csv"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true)){
            log.warn("Time elapsed: " + (Double.parseDouble(String.valueOf(swatch.now())) / 1000000000) + " s");
            System.exit(0);
        }
    }
}
