package Count;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Tools.MProperties;

public class CountProdu {
	/**
	 * 产品统计
	 * 产品 访问次数  访问人数
	 * 
	 * @author Administrator
	 *car,car000466,513049888252,1
	 */
    public static class mapper extends Mapper<LongWritable, Text, Text, Text>{
    	Text k=new Text();
        Text v=new Text();
    	@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
    		String[] line=value.toString().split(MProperties.getValue("outsplit"));
    		k.set(line[1]);
    		v.set(line[3]);
   	        context.write(k, v);
		}
    }
      public static class reducer extends Reducer<Text, Text, Text, NullWritable>{
           Text k=new Text();
        //car000466,1
		@Override
		  protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int sum=0;
			int sump=0;
			for (Text x:values){
				sum+=Integer.parseInt(x.toString());				
			}
			//产品 访问次数  访问人数
			sump+=1;
			k.set(key.toString()+","+sum+","+sump);
			context.write(k, NullWritable.get());   	  
      }
		   	
    }
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(CountProdu.class);		
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);					
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data6"));
		Path out=new Path("hdfs://192.168.245.20:9000/telecom/data7");			
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);

	}

}
