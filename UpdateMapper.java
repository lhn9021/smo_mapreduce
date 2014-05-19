import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UpdateMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	static final byte LOWER_BOUND = 0;
	static final byte UPPER_BOUND = 1;
	static final byte FREE = 2;
	double Cp, Cn;
	int kernel_type;
	static double gamma, coef0;
	static int degree;

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
		// TODO load
		double delta_alpha_i, delta_alpha_j;
		SvmData data_i = null, data_j = null;
		Configuration config = context.getConfiguration();
		data_i = new SvmData(config.get("i").split(" "));
		data_j = new SvmData(config.get("j").split(" "));
		coef0 = Double.parseDouble(config.get("coef0"));
		kernel_type = Integer.parseInt(config.get("kernel_type"));
		degree = Integer.parseInt(config.get("degree"));
		gamma = Double.parseDouble(config.get("gamma"));
		Cp = Double.parseDouble(config.get("C"));
		Cn = Double.parseDouble(config.get("C"));
		delta_alpha_i = Double.parseDouble(config.get("delta_alpha_i"));
		delta_alpha_j = Double.parseDouble(config.get("delta_alpha_j"));
		String[] record = value.toString().split("\t")[1].split(" ");
		SvmData data = new SvmData(record);
		double Q_i = data_i.y * data.y
				* kernel_function(kernel_type, data_i.values, data.values);
		double Q_j = data_j.y * data.y
				* kernel_function(kernel_type, data_j.values, data.values);
		if (data.active) {
			data.G += Q_i * delta_alpha_i + Q_j * delta_alpha_j;
		}

		boolean ui = SvmUtil.is_upper_bound(data);
		boolean uj = SvmUtil.is_upper_bound(data);
		update_alpha_status(data);
		update_alpha_status(data);
		if (ui != SvmUtil.is_upper_bound(data)) {
			if (ui)
				data.G_bar -= get_C(data_i) * Q_i;
			else
				data.G_bar += get_C(data_i) * Q_i;
		}

		if (uj != SvmUtil.is_upper_bound(data)) {
			if (uj)
				data.G_bar -= get_C(data_j) * Q_j;
			else
				data.G_bar += get_C(data_j) * Q_j;
		}

		if (data.index == data_i.index) {
			data.alpha = data_i.alpha;
		} else if (data.index == data_j.index) {
			data.alpha = data_j.alpha;
		}
		context.write(new LongWritable(data.index), new Text(data.toString()));

	}

	private void update_alpha_status(SvmData data) {
		if (data.alpha >= get_C(data))
			data.alpha_status = UPPER_BOUND;
		else if (data.alpha <= 0)
			data.alpha_status = LOWER_BOUND;
		else
			data.alpha_status = FREE;

	}

	double get_C(SvmData data) {
		return (data.y > 0) ? Cp : Cn;
	}
}
