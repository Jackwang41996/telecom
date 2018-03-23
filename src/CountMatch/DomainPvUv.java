package CountMatch;

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

public class DomainPvUv {
   public static class mapper extends Mapper<LongWritable, Text, Text, Text>{
    //4510002507,001914000000000000,20
	//用户ID     行为ID    用户访问次数
	   Text k=new Text();
	   Text v=new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line=value.toString().split(MProperties.getValue("outsplit"));
		k.set(line[1]);
		v.set(line[0]+MProperties.getValue("outsplit")+line[2]);
		context.write(k, v);
	}	   
   }
	
	public static class reducer extends Reducer<Text, Text, NullWritable, Text>{
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sump=0;
			int sumu=0;
			for(Text x:values){
				String[] str=x.toString().split(MProperties.getValue("outsplit"));
				sump+=Integer.parseInt(str[1]);
				sumu+=1;
			}
			v.set(key.toString()+MProperties.getValue("outsplit")+sump+MProperties.getValue("outsplit")+sumu);
			context.write(NullWritable.get(), v);
			
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(DomainPvUv.class);		
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);					
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data3"));
		Path out=new Path("hdfs://192.168.245.20:9000/telecom/data4");			
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);
	}

}
