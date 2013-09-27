import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Phase2 {
	static double THETA = 0;
	
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Text outputKey = new Text();
			ClassPath2 classpath = new ClassPath2();
			
			String line = value.toString();
			String[] lineInfo = line.split("\t");
			String id1 = lineInfo[0];
			String id2 = lineInfo[1];
			int length = Integer.parseInt(lineInfo[2]);
			double count = Double.parseDouble(lineInfo[3]);
			
			outputKey = new Text(id1 + "\t" + id2);
			classpath.setLength(length);
			classpath.setCount(count);
			
			output.collect(outputKey, new Text(classpath.toString()));
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Text outputKey = new Text();
			Text outputValue = new Text();
			double score = 0;
			
			String[] lineInfo;
			ClassPath2 value =  new ClassPath2();
			// add the Katz score of a path
			while (values.hasNext()) {
				lineInfo = values.next().toString().split("\t");
				value.setLength(Integer.parseInt(lineInfo[0]));
				value.setCount(Double.parseDouble(lineInfo[1]));
				score += value.getCount();
			}
			
//			if(score > THETA) {
				outputKey = key;
				outputValue = new Text(String.valueOf(score));
				output.collect(outputKey, outputValue);
//			}

		}
	}

	public void run(String path, int maxLen, double beta, double theta, int reducerNum) throws IOException {
		THETA = theta;
		
		JobConf job = new JobConf(Phase2.class);
		job.setJobName("Phase2");
		job.setNumReduceTasks(reducerNum);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(path + "/iter_" + String.valueOf(maxLen) + "/"));
		FileOutputFormat.setOutputPath(job, new Path(path + "/output"));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		JobClient.runJob(job);
	}
}