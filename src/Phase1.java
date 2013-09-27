import java.io.IOException;
import java.util.ArrayList;
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

public class Phase1 {
	private static int length;
	private static int L, LCurr = 1;
	public static double beta = 0.5;

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Text outputKey = new Text();
			ClassPath1 classpath = new ClassPath1();
			
			String line = value.toString();
			String[] lineInfo = line.split("\t");
			length = lineInfo.length;
			
			// input is the original edges
			if (length == 2) {
				String id1 = lineInfo[0];
				String id2 = lineInfo[1];
						
				// the outgoing edge
				outputKey = new Text(id1);
				classpath.setNodeId(id2);
				classpath.setLength(1);
				classpath.setCount(0);
				classpath.setDirection(1);
				classpath.setPreviousNodeIds(new ArrayList<String>());		
				output.collect(outputKey, new Text(classpath.toString()));
				
				// the incoming edge
				outputKey = new Text(id2);
				classpath.setNodeId(id1);
				classpath.setDirection(0);
				output.collect(outputKey, new Text(classpath.toString()));
			}
			// input is the generated paths
			else {				
				outputKey = new Text(lineInfo[0]);
				classpath.setNodeId(lineInfo[1]);
				classpath.setLength(Integer.parseInt(lineInfo[2]));
				classpath.setCount(Double.parseDouble(lineInfo[3]));
				classpath.setDirection(Integer.parseInt(lineInfo[4]));
				ArrayList<String> previousNodeIds = new ArrayList<String>();
				if(lineInfo.length == 6) {
					for(String s: lineInfo[5].split(",")) {
						previousNodeIds.add(s);
					}
				}
				classpath.setPreviousNodeIds(previousNodeIds);
				output.collect(outputKey, new Text(classpath.toString()));
			}	
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Text outputKey = new Text();
			ClassPath1 classpath = new ClassPath1();
			ArrayList<ClassPath1> originalInPairs = new ArrayList<ClassPath1>();
			ArrayList<ClassPath1> originalOutPairs = new ArrayList<ClassPath1>();
			ArrayList<ClassPath1> intermediatePairs = new ArrayList<ClassPath1>();
			ArrayList<ClassPath1> producedPairs = new ArrayList<ClassPath1>();			
			
			ClassPath1 value = new ClassPath1();;
			String[] lineInfo;
			// read the paths into four lists for further use
			while (values.hasNext()) {
				lineInfo = values.next().toString().split("\t");
				value.setNodeId(lineInfo[0]);
				value.setLength(Integer.parseInt(lineInfo[1]));
				value.setCount(Double.parseDouble(lineInfo[2]));
				value.setDirection(Integer.parseInt(lineInfo[3]));
				ArrayList<String> previousNodeIds = new ArrayList<String>();
				if(lineInfo.length == 5) {
					for(String s: lineInfo[4].split(",")) {
						previousNodeIds.add(s);
					}
				}
				value.setPreviousNodeIds(previousNodeIds);
				
				if(value.getLength() == 1) {
					if(value.getDirection() == 0) {
						originalInPairs.add(new ClassPath1(value));
					} else {
						originalOutPairs.add(new ClassPath1(value));
					}
					// output the original paths as it it
					output.collect(key, new Text(value.toString()));
				} else if(value.getLength() <= LCurr) {
					if(value.getLength() < LCurr) {
						output.collect(key, new Text(value.toString()));
						continue;
					}
					intermediatePairs.add(new ClassPath1(value));
					producedPairs.add(new ClassPath1(value));
				}
			}
			
			// compute the Katz score for the intermediate pairs
			Iterator<ClassPath1> iter = intermediatePairs.iterator();
			while(iter.hasNext()) {
				outputKey = key;
				classpath = iter.next();
				classpath.setPreviousNodeIds(new ArrayList<String>());
				classpath.setCount(Math.pow(beta, classpath.getLength()));
				output.collect(outputKey, new Text(classpath.toString()));
			}
			
			if(LCurr == 1) {
				producedPairs = new ArrayList<ClassPath1>(originalOutPairs);
			}
			
			classpath = new ClassPath1();
			//  combine the incoming length 1 edges with the paths of length LCurr to create new paths 
			for (ClassPath1 node2 : producedPairs) {
				for (ClassPath1 node1 : originalInPairs) {
					if(!node1.equals(node2)) {
						outputKey = new Text(node1.getNodeId());
						classpath.setNodeId(node2.getNodeId());
						classpath.setDirection(1);
						classpath.setLength(LCurr + 1);
						classpath.setPreviousNodeIds(node2.getPreviousNodeIds());
						ArrayList<String> prevIds = classpath.getPreviousNodeIds();
						prevIds.add(key.toString());
						classpath.setPreviousNodeIds(new ArrayList<String>(prevIds));
						output.collect(outputKey, new Text(classpath.toString()));
					}
				}
				
				// check cycle
				if(!originalOutPairs.contains(node2)) {
					outputKey = key;
					classpath = node2;
					output.collect(outputKey, new Text(classpath.toString()));
				}
			}
		}
	}

	public void run(String path, int maxLen, double beta, int reducerNum) throws IOException {
		L = maxLen;
		Phase1.beta = beta;
		JobConf job;
		
		while (LCurr <= L) {
			job = new JobConf(Phase1.class);
			job.setJobName("Phase1");
			job.setNumReduceTasks(reducerNum);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			FileInputFormat.setInputPaths(job, new Path(path + "/iter_" + String.valueOf(LCurr - 1) + "/"));
			FileOutputFormat.setOutputPath(job, new Path(path + "/iter_" + String.valueOf(LCurr) + "/"));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			JobClient.runJob(job);
			LCurr ++;
		}
	}
}