import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SelectWorkingSetTwoReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	static final double INF = Double.MAX_VALUE;

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int Gmin_idx = -1;
		double obj_diff_min = INF;
		double Gmax2 = -INF;
		Configuration config=context.getConfiguration();
		String i_data=config.get("data");
		double Gmax=Double.parseDouble(config.get("Gmax"));
		String[] i_record=i_data.split(" ");
		SvmData i=new SvmData(i_record);
		//TODO
		//load eps
		double eps=0;
		String i_string=null;
		String result=null;
		for (Text text : values) {
//			String[] split=text.toString().split(" ",2);
			String[] split=text.toString().split("\t");
			String[] record=split[1].split(" ");
			double obj_diff = Double.parseDouble(split[0]);
			SvmData data=new SvmData(record);
			if (data.y == +1) {
				if (!SvmUtil.is_lower_bound(data)) {
					double grad_diff = Gmax + data.G;
					if (data.G >= Gmax2)
						Gmax2 = data.G;
					if (grad_diff > 0) {
						if (obj_diff <= obj_diff_min&&i.index!=data.index) {
							result=split[1];
							obj_diff_min = obj_diff;
							Gmin_idx=data.index;
						}
					}
				}
			} else {
				if (!SvmUtil.is_upper_bound(data)) {
					double grad_diff = Gmax- data.G;
					if (-data.G >= Gmax2)
						Gmax2 = -data.G;
					if (grad_diff > 0) {
						if (obj_diff <= obj_diff_min&&i.index!=data.index) {
							result=split[1];
							obj_diff_min = obj_diff;
							Gmin_idx=data.index;
						}
					}
				}
			}
		}
		if(i.G + Gmax2 < eps){
			context.write(new LongWritable(1), new Text());
		}else{
			context.write(new LongWritable(0), new Text(result));
		}
		
	}

}
