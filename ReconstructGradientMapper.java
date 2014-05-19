import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReconstructGradientMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	static final byte LOWER_BOUND = 0;
	static final byte UPPER_BOUND = 1;
	static final byte FREE = 2;

	static double gamma=0,coef0=0;
	static int degree=0;
	
	public boolean is_free(SvmData data) {
		return data.alpha_status == FREE;
	}

	static double kernel_function(int kernel_type, float[] i, float[] j) {
		switch (kernel_type) {
		case svm_parameter.LINEAR:
			return SvmUtil.dot(i, j);
		case svm_parameter.POLY:
			return SvmUtil.powi(gamma * SvmUtil.dot(i, j) + coef0, degree);
		case svm_parameter.RBF:
			return Math.exp(-gamma
					* (SvmUtil.square(i) + SvmUtil.square(j) - 2 * SvmUtil.dot(
							i, j)));
		case svm_parameter.SIGMOID:
			return Math.tanh(gamma * SvmUtil.dot(i, j) + coef0);
		case svm_parameter.PRECOMPUTED:
			return i[(int) (j[0])];
		default:
			return 0; // java
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		int i, j;
		int nr_free = 0;
		SvmData[] actives = null;
		int kernel_type = 0;
		// TODO
		// load active set
		// value come from inactive set
		String[] record = value.toString().split(" ");
		SvmData data = new SvmData(record);
		if (data.active) {
			data.G = data.G_bar - 1;
		} else {
			if (is_free(data))
				nr_free++;
		}

		for (j = 0; j < actives.length; j++)
			if (is_free(actives[j])) {
				double Q_i = data.y
						* actives[j].y
						* kernel_function(kernel_type, data.values,
								actives[j].values);
				data.G += actives[j].alpha * Q_i;
			}

	}

}
