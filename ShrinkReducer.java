import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ShrinkReducer extends Reducer<LongWritable, Text, Text, Text> {
	static final double INF = Double.MAX_VALUE;
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double Gmax1=-INF,Gmax2=-INF;
		for(Text text:values){
			String split[]=text.toString().split(" ");
			double local_Gmax1=Double.parseDouble(split[0]);
			double local_Gmax2=Double.parseDouble(split[1]);
			if(local_Gmax1>Gmax1){
				Gmax1=local_Gmax1;
			}
			if(local_Gmax2>Gmax2){
				Gmax2=local_Gmax2;
			}
			
		}
		context.write(new Text(Double.toString(Gmax1)), new Text(Double.toString(Gmax2)));
	}
}
