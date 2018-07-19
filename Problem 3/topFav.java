import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.NullWritable;

public class topFav {

	  public static class MapMaker {

      public static Map<Integer, String> transformCSVToMap(String csv) {

         Map<Integer, String> map = new HashMap<Integer, String>();
         try {
               boolean flag = true;
               for (int i = 0; i < csv.length(); i++) {
                     if (csv.charAt(i) == '\"') {
                        flag = !flag;
                     }
                     else if ((csv.charAt(i) == ',') && (flag == false)) {
                        StringBuilder sb = new StringBuilder(csv);
                        sb.setCharAt(i, '%');
                        csv = sb.toString();
                     }
                }
              String[] tokens = csv.trim().split(",");
              int key = 0;
              String value = new String();
              for (int j = 0; j < tokens.length -1; j++) {
                 key = j;
                 if (tokens[j] == null || tokens[j].isEmpty())
                     tokens[j] = "0";
                 	if(tokens[j].equals("Favs"))
                 		tokens[j]="0";
                 		
                 value = tokens[j];
                 map.put(key, value);
              }
           }  catch (StringIndexOutOfBoundsException e) {
                                System.err.println("OutOfBoundsException");
              }
              return map;
        }
   }


	public static class topTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> favToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			Map<Integer, String> parsed = MapMaker.transformCSVToMap(value.toString());
			

			if(parsed == null){
				return;
			}

			String userId = parsed.get(0);
			String favs = parsed.get(2);

			if(userId == null || favs == null) {
				return;
			}

			favToRecordMap.put(Integer.parseInt(favs), new Text(value));

			if(favToRecordMap.size() > 10) {
				favToRecordMap.remove(favToRecordMap.firstKey());
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Text t : favToRecordMap.values()){
				context.write(NullWritable.get(), t);
			}
		}


	}


	public static class topFavReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> favToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value : values) {
				Map<Integer, String> parsed = MapMaker.transformCSVToMap(value.toString());
				
				String userId = parsed.get(0);
				String favs = parsed.get(2);
				System.out.println(favs);

				if(userId == null || favs == null || favs.isEmpty()) {
					continue;
					
				}
				
				
				favToRecordMap.put(Integer.parseInt(favs), new Text(userId));

				if(favToRecordMap.size() > 10){
					favToRecordMap.remove(favToRecordMap.firstKey());
				}
			}

			for(Text t : favToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}
	
	//Joining the files. Adding the Biography and all the information
	
	public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outUserId = new Text();
		private Text outValue = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String s = value.toString();
			String parsed[] = s.split("\",");
			
			String skip[] = new String[1];
			skip[0] = "Handle,Name,Bio,Country,Location,Followers,Following";
			if(parsed[0].equals(skip[0]))
				return;

			String user = parsed[0].replace("\"", "").toLowerCase();

			
				outUserId.set(user);
				outValue.set("A");
				context.write(outUserId, outValue);

		}

	}

	public static class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outTuple = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String s = value.toString();
			String parsed[] = s.split("\",");

			String skip[] = new String[1];
			skip[0] = "Handle,Name,Bio,Country,Location,Followers,Following";
			if(parsed[0].equals(skip[0]))
				return;

			String user = parsed[0].replace("\"", "").toLowerCase();
			String location = parsed[4].replace("\"", "");
      location=location.replace(",",""); 
			
			
			String text = "\t"+parsed[1].replace("\"", "")+"\t"+parsed[2].replace("\"", "")+"\t"+parsed[3].replace("\"", "")+
					"\t"+location+"\t"+parsed[5].replace("\"", "");
			

			if (user == null || text == null){
				return;
			}

			outkey.set(user);
			outTuple.set("B" + text);

			context.write(outkey, outTuple);

		}	
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

			private static final Text EMPTY_TEXT = new Text("");
			private Text tmp = new Text();
			private ArrayList<Text> listA = new ArrayList<Text>();
			private ArrayList<Text> listB = new ArrayList<Text>();
			private String joinType = null;


			@Override
			public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				listA.clear();
				listB.clear();

				for(Text tmp : values) {
					if(tmp.charAt(0) == 'A'){
						listA.add(new Text(tmp.toString().substring(1)));
					} else if(tmp.charAt(0) == 'B') {
						listB.add(new Text(tmp.toString().substring(1)));
					}
				}

				if(!listA.isEmpty() && !listB.isEmpty()) {
					for (Text A : listA) {
						for(Text B : listB) {
							context.write(key, B);
						}
					}
				}
			}
	}
	
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		

		if(otherArgs.length != 3) {
			System.err.println("Usage: topFav <in> <in> <out>");
			System.exit(2);
		}
		
		Path output_Inter = new Path(otherArgs[2] + "_int");

		Job job = new Job(conf, "Top Ten Favorite Handles");
		job.setJarByClass(topFav.class);
		job.setMapperClass(topTenMapper.class);
		job.setReducerClass(topFavReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, output_Inter);
		
		int code=job.waitForCompletion(true) ? 0 : 1;
		
		//Job 2 started 
		Job job2 = new Job(conf, "Merge top 10 With Bio");
		job2.setJarByClass(topFav.class);


		MultipleInputs.addInputPath(job2,output_Inter, TextInputFormat.class, UserJoinMapper.class);
		MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, CommentJoinMapper.class);
		job2.setReducerClass(JoinReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		
		code=job2.waitForCompletion(true) ? 0 : 2;

		System.exit(code);
	}

}
