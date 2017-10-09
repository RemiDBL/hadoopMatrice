
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TransposeMatrix {

    public static class Map
            extends Mapper<LongWritable, Text, LongWritable, MapWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            MapWritable mw = new MapWritable();
            int i = 0;
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                mw.put(key, new Text((String) itr.nextElement()));
                context.write(new LongWritable(i), mw);
                i++;
            }
        }
    }

    public static class MatrixReducer
            extends Reducer<LongWritable, MapWritable, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<MapWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            SortedMap<LongWritable, Text> rowVals = new TreeMap<>();
            for (MapWritable map : values) {
                map.entrySet().stream().forEach((entry) -> {
                    rowVals.put((LongWritable) entry.getKey(), (Text) entry.getValue());
                });
            }
            StringBuilder sb = new StringBuilder();
            rowVals.values().stream().map((rowVal) -> {
                sb.append(rowVal);
                return rowVal;
            }).forEach((_item) -> {
                sb.append(",");
            });
            sb.deleteCharAt(sb.length()-1);
            context.write(key, new Text(sb.toString()));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(TransposeMatrix.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MapWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
