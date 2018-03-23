package Count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import Tools.MProperties;
public class TopCount {
     /**
      * 
      * 某个用户关注的主网页的前五
      * 用户id 行为id 网页访问次数 排名
      * @param args
      */
	public static class myboin implements WritableComparable<myboin>{
		String  url;
        int  num;
		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}
		public int getNum() {
			return num;
		}

		public void setNum(int num) {
			this.num = num;
		}

		
        public void set(String url,int num){
        	this.url=url;
        	this.num=num;
        }
        // //反序列化，从流中的二进制转换myboin
		@Override
		public void readFields(DataInput in) throws IOException {
			url=in.readUTF();
			num=in.readInt();
			
		}
		 //序列化，将myboin转化成使用流传送的二进制 
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(url);
			out.writeInt(num);
		}

		@Override
		public int compareTo(myboin o) {
		   int m=o.url.compareTo(this.url);
		   return m==0?(o.num==this.num? 0:(o.num<this.num ? -1:1)):m;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + num;
			result = prime * result + ((url == null) ? 0 : url.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			myboin other = (myboin) obj;
			return this.url.equals(other.url);}
	}
	//4510002507,001914000000000000,20
	public static class mapper extends Mapper<LongWritable, Text, myboin, Text>{
        Text v=new Text();
        myboin k=new myboin();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line=value.toString().split(MProperties.getValue("outsplit"));
			k.set(line[0],Integer.parseInt(line[2]));
			v.set(line[1]);
			context.write(k, v);
		}		
	}
	 public static class FirstPartitioner extends Partitioner<myboin,Text>{  
	        public int getPartition(myboin key, Text value,   
	                                int numPartitions) {  
	          return key.url.hashCode() & Integer.MAX_VALUE % numPartitions;  
	        }  
	      }  
	 static class MyGroupCompactor extends WritableComparator{
			protected  MyGroupCompactor() {
			      super(myboin.class,true);
				}
			@Override
			public int compare(WritableComparable a, WritableComparable b) {
				myboin x1 = (myboin)a;
				myboin x2 = (myboin)b;
				String a1=x1.getUrl();
				String b1=x2.getUrl();
				if(a1.equals(b1)){
					return 0;
				}else{  
					int m=a1.compareTo(b1);
					if(m>0){
					return 1;
					}else{
						return -1;
					}
				}
				
			
			}
	 }
	public static class reducer extends Reducer<myboin,Text, Text, NullWritable>{
        Text k=new Text();
		@Override
		protected void reduce(myboin key, Iterable< Text> values, Context context)
				throws IOException, InterruptedException {
			//4510002507 001914000000000000,20
			StringBuffer sb=new StringBuffer();	
			int i=0;
			sb.append(key.getUrl()).append(",");
			for(Text x:values){
				i++;				
				if(i>5){
					break;
				}
				sb.append(x).append(",").append(key.getNum()).append(",");
				k.set(sb.toString()); 				
			}
			
			context.write(k,NullWritable.get());					
		}		
		}
		
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(CountProdu.class);		
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		job.setMapOutputKeyClass(myboin.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(MyGroupCompactor.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.245.20:9000/telecom/data3"));		
		String url="hdfs://192.168.245.20:9000/telecom/data8";
		Path out=new Path(url);		
		FileOutputFormat.setOutputPath(job, out);
		boolean res=job.waitForCompletion(true);
	}

}
