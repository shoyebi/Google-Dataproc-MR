package com.hike.analytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bsb.hike.analytics.hive.udtf.HikeUDTFConstants;
import com.hike.analytics.common.Utils;
import com.hike.analytics.transform.LoglineTransform;

public class MapReduceDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		MapReduceDriver driver = new MapReduceDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

	public static class MapperJob extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			LoglineTransform transform = new LoglineTransform(con);
			ArrayList<Object[]> objList = transform.processElement(value.toString());
			if (objList == null)
				return;
			
			for (Object[] obj : objList) {
				String kingdom = String.valueOf(obj[HikeUDTFConstants.KINGDOM_COLUMN_INDEX]);
				Text outputKey = new Text(kingdom);
				StringBuffer outputVal = new StringBuffer();
				outputVal.append(String.valueOf(obj[0]) + "\t");
				outputVal.append(String.valueOf(obj[1]) + "\t");
				outputVal.append(String.valueOf(obj[HikeUDTFConstants.KINGDOM_COLUMN_INDEX]) + "\t");
				outputVal.append(String.valueOf(obj[HikeUDTFConstants.KINGDOM_COLUMN_INDEX + 1]) + "\t");
				
				con.write(outputKey, new Text(outputVal.toString()));
			}
		}
		
		/*public void run(Context context) throws IOException, InterruptedException {
	        setup(context);
	        while (context.nextKeyValue()) {
	            map(context.getCurrentKey(), context.getCurrentValue(), context);
	        }
	        cleanup(context);
	    }*/
	}

	public static class ReducerJob extends Reducer<Text, Text, Text, OrcStruct> {

		private MultipleOutputs<Text, OrcStruct> multipleOutputs;
		private Random random = new Random();

		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
			Iterator<Text> iterator = values.iterator();
			StringBuffer stringBuffer = new StringBuffer();
			
			String typeStr = Utils.typeStr;
			TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
			OrcStruct struct = (OrcStruct)  OrcStruct.createValue(Utils.schema);
			while (iterator.hasNext()) {
				Text mapOut = iterator.next();
				String [] mapOutRow = mapOut.toString().split("\t");
				struct.setFieldValue("row_id", new Text(String.valueOf(mapOutRow[0])));
		        struct.setFieldValue("phylum", new Text(String.valueOf(mapOutRow[1])));
		        struct.setFieldValue("kingdom", new Text(mapOutRow[2]));
		        struct.setFieldValue("dt", new Text(mapOutRow[3]));
				multipleOutputs.write(key, struct, generateFileName(key));
			}
			//Text outputValue = new Text(stringBuffer.toString());
			
		}

		String generateFileName(Text key) {
			return key.toString() + "_" + String.valueOf(random.nextInt(100));
		}

		@Override
		public void setup(Context context) {
			multipleOutputs = new MultipleOutputs<Text, OrcStruct>(context);
		}

		@Override
		public void cleanup(final Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	public int run(String[] args) throws Exception {
		/*
		 * Configuration c = new Configuration(); String[] files = new
		 * GenericOptionsParser(c, args).getRemainingArgs(); Path input = new
		 * Path(files[0]); Path output = new Path(files[1]);
		 */
		Job j = new Job();
		j.setJarByClass(MapReduceDriver.class);
		j.setMapperClass(MapperJob.class);
		j.setReducerClass(ReducerJob.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		j.setOutputFormatClass(OrcOutputFormat.class);
		MultipleOutputs.addNamedOutput(j, "text", OrcOutputFormat.class, NullWritable.class, OrcStruct.class);
		// FileInputFormat.setInputDirRecursive(j);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		FileSystem fs = FileSystem.newInstance(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
		System.exit(j.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
