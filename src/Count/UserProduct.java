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

public class UserProduct {
/**
 * 
 * 统计用户高频访问地址
 * 用户产品统计   某一个用户对某一产品访问次数
 * 用户id 产品id 访问次数   产品类型
 * @param args 
 */
//003942000037000113|house|house000044|其他-null-70平米到100平米|其他|300万以上|null|null|70平米到100平米|513042437238
//	行为id 产品类型 产品id 产品名称 产品特征值1 产品特征值2 产品特征值 3 产品特征值4 产品特征值5  用户id
	public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    Text k=new Text();
    IntWritable v=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line=value.toString().split(MProperties.getValue("msgaddrspilt"));
		StringBuffer sb=new StringBuffer();
		sb.append(line[1]+","+line[2]+","+line[9]);
		k.set(sb.toString());
	     context.write(k, v);
	}
		
	}
	public static class reducer extends Reducer<Text, IntWritable, Text, NullWritable>{
		 Text k=new Text();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable a:values){
				sum+=a.get();
			}
			k.set(key.toString()+","+sum);
			context.write(k,NullWritable.get());
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(UserProduct.class);		
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);					
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data5"));
		Path out=new Path("hdfs://192.168.245.20:9000/telecom/data6");			
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);
	}

}
