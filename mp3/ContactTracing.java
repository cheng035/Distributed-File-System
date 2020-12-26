import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StopWatch;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;


class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) {
        super(Text.class);
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
        {
            sb.append(s).append(",");
        }
        return sb.toString();
    }
}


public class ContactTracing {

    static Logger log = Logger.getLogger(ContactTracing.class.getName());

    // First mapper
    // Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class FirstReaderMapper
            extends Mapper<Object, Text, Text, TextArrayWritable> {

        TextArrayWritable valueArray = new TextArrayWritable();

        public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {

            // Skip empty lines
            if(value.toString().length() < 5){
                return;
            }

            String[] data = value.toString().split(",", -1);
            String name = data[0];

            // <name, location, s_time, e_time, "UNKNOWN">
            Text[] textArray = new Text[5];

            textArray[0] = new Text(name);
            textArray[1] = new Text(data[1]);
            textArray[2] = new Text(data[2]);
            textArray[3] = new Text(data[3]);
            textArray[4] = new Text("UNKNOWN");

            valueArray.set(textArray);
            // emit <name, (location, start, end)>>
            if(!name.equals("")) {
                context.write(new Text(name), valueArray);
            }
        }
    }

    // Second mapper
    public static class SecondReaderMapper
            extends Mapper<Object, Text, Text, TextArrayWritable> {

        TextArrayWritable valueArray = new TextArrayWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] names = value.toString().split(",", -1);
            for (String name : names) {
                if(!name.equals("")){
                    Text name_text = new Text(name);
                    Text[] textArray = new Text[5];
                    textArray[0] = new Text(name);
                    textArray[1] = new Text("X");
                    textArray[2] = new Text("X");
                    textArray[3] = new Text("X");
                    textArray[4] = new Text("TRUE");
                    valueArray.set(textArray);
                    context.write(name_text, valueArray);
                }

            }
        
        }
    }

    public static class SecondReaderMapper_DiffRow
            extends Mapper<Object, Text, Text, TextArrayWritable> {

        TextArrayWritable valueArray = new TextArrayWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String name = value.toString();
            if(!name.equals("")){
                Text name_text = new Text(name);
                Text[] textArray = new Text[5];
                textArray[0] = new Text(name);
                textArray[1] = new Text("X");
                textArray[2] = new Text("X");
                textArray[3] = new Text("X");
                textArray[4] = new Text("TRUE");
                valueArray.set(textArray);
                context.write(name_text, valueArray);
            }
        }
    }

    // Test Input Reducer
    public static class TestInputReducer
            extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

        public void reduce(Text key, Iterable<TextArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (TextArrayWritable list : values) {
                context.write(key, list);
            }
        }
    }


    // Input Reducer
    public static class InputReducer
            extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

        public void reduce(Text key, Iterable<TextArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<TextArrayWritable> copyList = new ArrayList<TextArrayWritable>();

            boolean posFlag = false;
            for (TextArrayWritable list : values) {
                Writable[] infoList = list.get();
                try {
                    String result = infoList[4].toString();
                    if(result.equals("TRUE")) {
                        // log.info("Pos name: " + infoList[0].toString());
                        // log.info("Pos sample: " + Arrays.toString(infoList));
                        posFlag = true;
                    } else {
                        // log.info("Neg sample: " + Arrays.toString(infoList));
                        //
                        TextArrayWritable new_list = new TextArrayWritable();
                        new_list.set(infoList);
                        copyList.add(new_list);
                    }
                } catch (Exception e) {
                    log.info(infoList.length);
                    log.info((infoList[0]).toString());
                    log.info((infoList[1]).toString());
                    log.info((infoList[2]).toString());
                }
            }

            // log.info("Copy length: " + copyList.size());
            // log.info("Ori length: " + counter);

            // Second iteration
            for (TextArrayWritable s_list : copyList) {
                Writable[] infoList = s_list.get();

                // log.info("Length " + infoList.length);
                String name = infoList[0].toString();
                String location = infoList[1].toString();

                Text[] textArray = new Text[3];
                TextArrayWritable valueArray = new TextArrayWritable();

                // <name, (s_time, e_time, name/POSITIVE)>
                textArray[0] = new Text(infoList[2].toString());
                textArray[1] = new Text(infoList[3].toString());
                if(posFlag) {
                    textArray[2] = new Text("POSITIVE");
                } else {
                    textArray[2] = new Text(name);
                }
                // s_time, e_time, name/POSITIVE
                valueArray.set(textArray);
                context.write(new Text(location), valueArray);
            }
        }
    }

    // Combine mapper
    public static class CombineMapper
            extends Mapper<Object, Text, Text, TextArrayWritable> {

        public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {

            String[] data = value.toString().split(";", -1);
            String location = data[0];

            //
            String[] list = data[1].split(",", -1);
            Text[] values = new Text[3];
            values[0] = new Text(list[0]);
            values[1] = new Text(list[1]);
            values[2] = new Text(list[2]);

            TextArrayWritable valueArray = new TextArrayWritable();
            valueArray.set(values);

            context.write(new Text(location), valueArray);

        }
    }

    // Combined reducer
    public static class CombineReducer
            extends Reducer<Text, TextArrayWritable, Text, Text> {

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);

        public void reduce(Text key, Iterable<TextArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<ArrayList<Date>> posDateArray = new ArrayList<ArrayList<Date>>();
            // Find positive date
            ArrayList<TextArrayWritable> copyList = new ArrayList<TextArrayWritable>();
            for (TextArrayWritable list : values) {
                Writable[] infoList = list.get();
                String s_date = infoList[0].toString();
                String e_date = infoList[1].toString();
                String status = infoList[2].toString();

                if(status.equals("POSITIVE")) {
                    try {
                        ArrayList<Date> positiveDate = new ArrayList<Date>();
                        positiveDate.add(formatter.parse(s_date));
                        positiveDate.add(formatter.parse(e_date));
                        posDateArray.add(positiveDate);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                } else {
                    TextArrayWritable new_list = new TextArrayWritable();
                    new_list.set(infoList);
                    copyList.add(new_list);
                }
            }

            // Filtering suspects
            for (TextArrayWritable list : copyList) {
                Writable[] infoList = list.get();
                String s_date = infoList[0].toString();
                String e_date = infoList[1].toString();
                String s_name = infoList[2].toString();

                try {
                    Date startDate = formatter.parse(s_date);
                    Date endDate = formatter.parse(e_date);
                    for (ArrayList<Date> posDate: posDateArray){
                        Date posStartDate = posDate.get(0);
                        Date posEndDate = posDate.get(1);
                        if (startDate.before(posEndDate) && posStartDate.before(endDate)){
                            context.write(new Text(s_name), new Text("TO-CHECK"));
                            // members.add(s_name);
                            break;
                        }
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }

        }
    }


    // Final mapper ---------------------------
    public static class SummeryMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] data = value.toString().split(";", -1);
            context.write(new Text(data[0]), new Text(data[1]));
        }
    }

    public static class SummeryReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // key=name, values = TO_CHECK
            for (Text value: values){
                context.write(key, value);
                break;
            }
        }
    }


    public static <ControlledJob> void main(String[] args) throws Exception {
        /*
        Configuration conf = new Configuration();
        Job job = new Job(conf, "ContactTracing");

        job.setJarByClass(ContactTracing.class);

        // Mapper, Combiner, Reducer
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        // ----------------------------------------
        */
        /*
        // First MP -------------------------------------------------------------
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "ContactTracing");

        job1.setJarByClass(ContactTracing.class);

        // Mapper, Combiner, Reducer
        job1.setMapperClass(ReaderMapper.class);
        job1.setCombinerClass(ReadSumReducer.class);
        job1.setReducerClass(ReadSumReducer.class);

        // Output types
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0] + "Personal_Info.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("./output_MP_1"));


        // Second MP -------------------------------------------------------------
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "ContactTracing");

        job2.setJarByClass(ContactTracing.class);

        // Mapper, Combiner, Reducer
        job2.setMapperClass(ReaderMapper.class);
        job2.setCombinerClass(ReadSumReducer.class);
        job2.setReducerClass(ReadSumReducer.class);

        // Output types
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[0] + "If_Positive.csv"));
        FileOutputFormat.setOutputPath(job2, new Path("./output_MP_2"));

        // Third MP -------------------------------------------------------------

        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "ContactTracing");

        job3.setJarByClass(ContactTracing.class);

        // Mapper, Combiner, Reducer
        job3.setMapperClass(ReaderMapper.class);
        job3.setCombinerClass(ReadSumReducer.class);
        job3.setReducerClass(ReadSumReducer.class);

        // Output types
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job3, new Path("./output_MP_1"),
                TextInputFormat.class, BooleanInputFormat.class);
        MultipleInputs.addInputPath(job3, new Path("./output_MP_2"),
                TextInputFormat.class, SumStepByToolWithCommaMapper.class);
        FileOutputFormat.setOutputPath(job3, new Path("./output_MP_3"));
        */


        // ---------------------------------------------------------------

        StopWatch swatch = new StopWatch();
        Logger log = Logger.getLogger(ContactTracing.class.getName());
        swatch.start();
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ";");
        String temp_1_path = "temp_1/";
        String temp_2_path = "temp_2/";

        Job job1 = Job.getInstance(conf, "ContactTracing_1");
        job1.setJarByClass(ContactTracing.class);

        /*
        MultipleInputs.addInputPath(job1, new Path(args[0] + "D1.csv"), TextInputFormat.class,
                FirstReaderMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[0] + "D2.csv"), TextInputFormat.class,
                SecondReaderMapper.class);
        */
        MultipleInputs.addInputPath(job1, new Path(args[0] + "org.csv"), TextInputFormat.class,
                FirstReaderMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[0] + "inf.csv"), TextInputFormat.class,
                SecondReaderMapper_DiffRow.class);
        job1.setReducerClass(InputReducer.class);
        // job1.setReducerClass(TestInputReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TextArrayWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(TextArrayWritable.class);
        FileOutputFormat.setOutputPath(job1, new Path(temp_1_path));

        // Second job -------------------------
        Job job2 = Job.getInstance(conf, "ContactTracing");

        job2.setJarByClass(ContactTracing.class);

        // Mapper, Combiner, Reducer
        job2.setMapperClass(CombineMapper.class);
        job2.setReducerClass(CombineReducer.class);

        // Output types
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TextArrayWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(temp_1_path));
        FileOutputFormat.setOutputPath(job2, new Path(temp_2_path));


        // Summary job -------------------------
        Job job3 = Job.getInstance(conf, "ContactTracing_sum");

        job3.setJarByClass(ContactTracing.class);

        // Mapper, Combiner, Reducer
        job3.setMapperClass(SummeryMapper.class);
        job3.setReducerClass(SummeryReducer.class);

        // Output types
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(temp_2_path));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        if (job1.waitForCompletion(true)) {
            if (job2.waitForCompletion(true)){
                if (job3.waitForCompletion(true)){
                    log.warn("Time elapsed: " + (Double.parseDouble(String.valueOf(swatch.now())) / 1000000000) + " s");
                    System.exit(0);
                }
            }
        }

    }
}
