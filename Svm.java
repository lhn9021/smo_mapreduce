import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Svm {
	static int kernel_type = 2;
	static double gamma = 0, coef0 = 0, C = 1;
	static int degree = 0;

	public static void main(String[] args) throws Exception {

		// TODO
		// Load C kernel_type

		// Initialize
		String output_path = args[1];

		String active_set_addr = args[1] + "active/";
		Path active_set_path = new Path(active_set_addr);

		String inactive_set_addr = args[1] + "inactive/";
		Path inactive_set_path = new Path(inactive_set_addr);

		String active_update_addr = args[1] + "active_update/";
		Path active_update_path = new Path(active_update_addr);

		String inactive_update_addr = args[1] + "inactive_update/";
		Path inactive_update_path = new Path(inactive_update_addr);

		String shrink_step_one_addr = args[1] + "shrink_step_one/";
		Path shrink_step_one_path = new Path(shrink_step_one_addr);

		String select_working_set_step_one_addr = args[1]
				+ "select_working_set_step_one/";
		Path select_working_set_step_one_path = new Path(
				select_working_set_step_one_addr);

		String select_working_set_step_two_addr = output_path
				+ "select_working_set_step_two/";
		Path select_working_set_step_two_path = new Path(
				select_working_set_step_two_addr);

		String shrink_step_two_addr = output_path + "shrink_step_two/";
		Path shrink_step_two_path = new Path(shrink_step_two_addr);

		FileSystem fs;

		int kernel_type = 2;
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(output_path));

		Configuration initialize_config = new Configuration();
		initialize_config.set("kernel_type", Integer.toString(kernel_type));
		initialize_config.set("coef0", Double.toString(coef0));
		initialize_config.set("degree", Integer.toString(degree));
		initialize_config.set("gamma", Double.toString(gamma));
		initialize_config.set("C", Double.toString(C));
		Job initialize = new Job(initialize_config);
		initialize.setJobName("Initialize");
		FileInputFormat.addInputPath(initialize, new Path(args[0]));
		FileOutputFormat.setOutputPath(initialize, active_set_path);
		initialize.setJarByClass(Svm.class);
		initialize.setMapperClass(InitializeMapper.class);
		initialize.waitForCompletion(true);
		int counter=1000;
		int max_iter = 10000000;
		int iter = 0;
		boolean unshrink = false;
		while (iter < max_iter) {
			if (--counter == 0) {
				counter = 1000;
				conf = new Configuration();
				hdfs = FileSystem.get(conf);
				hdfs.delete(shrink_step_one_path);

				Job shrink_step_one = new Job();
				shrink_step_one.setJobName("Shrink Step One");
				FileInputFormat.addInputPath(shrink_step_one, active_set_path);
				FileOutputFormat.setOutputPath(shrink_step_one,
						shrink_step_one_path);
				shrink_step_one.setJarByClass(Svm.class);
				shrink_step_one.setMapperClass(ShrinkMapper.class);
				shrink_step_one.setNumReduceTasks(1);
				shrink_step_one.setReducerClass(ShrinkReducer.class);
				shrink_step_one.waitForCompletion(true);

				String shrink_result_addr = shrink_step_one_addr
						+ "/part-r-00000";
				Path shrink_one_result_path = new Path(shrink_result_addr);
				fs = FileSystem.get(URI.create(shrink_result_addr), conf);
				double shrink_Gmax1 = 0, shrink_Gmax2 = 0;
				String shrink_one_result = null;
				if (fs.exists(shrink_one_result_path)) {
					FSDataInputStream is = fs.open(shrink_one_result_path);
					FileStatus stat = fs.getFileStatus(shrink_one_result_path);
					byte[] buffer = new byte[Integer.parseInt(String
							.valueOf(stat.getLen()))];
					is.readFully(0, buffer);
					is.close();
					fs.close();
					shrink_one_result = new String(buffer);
				} else {
					throw new Exception("the file is not found .");
				}
				String[] shrink_split = shrink_one_result.split("\t");
				shrink_Gmax1 = Double.parseDouble(shrink_split[0]);
				shrink_Gmax2 = Double.parseDouble(shrink_split[1]);

				conf = new Configuration();
				hdfs = FileSystem.get(conf);
				hdfs.delete(shrink_step_two_path);
				Configuration shrink_two_config = new Configuration();
				shrink_two_config.set("Gmax1", Double.toString(shrink_Gmax1));
				shrink_two_config.set("Gmax2", Double.toString(shrink_Gmax2));
				shrink_two_config.set("iter", Integer.toString(iter));
				Job shrink_step_two = new Job(shrink_two_config);
				shrink_step_two.setJobName("Shrink Step Two");
				FileInputFormat.addInputPath(shrink_step_two, active_set_path);
				FileOutputFormat.setOutputPath(shrink_step_two,
						shrink_step_two_path);
				MultipleOutputs.addNamedOutput(shrink_step_two, "MOSText",
						TextOutputFormat.class, LongWritable.class, Text.class);
				shrink_step_two.setJarByClass(Svm.class);
				shrink_step_two.setMapperClass(ShrinkTwoMapper.class);
				shrink_step_two.setNumReduceTasks(1);
				shrink_step_two.waitForCompletion(true);
				conf = new Configuration();
				hdfs = FileSystem.get(conf);
				if (hdfs.exists(active_set_path)) {
					hdfs.delete(active_set_path);
					hdfs.rename(new Path(shrink_step_two + "active_" + iter
							+ "/"), active_set_path);
				}
				hdfs.rename(
						new Path(shrink_step_two + "inactive_" + iter + "/"),
						inactive_set_path);

			}
			// if(unshrink == false && Gmax1 + Gmax2 <= eps*10)
			// {
			// unshrink = true;
			// reconstruct_gradient();
			// System.out.println("shrink reconstrcut gradient");
			// active_size = l;//move inactive to active
			// }

			conf = new Configuration();
			hdfs = FileSystem.get(conf);
			hdfs.delete(select_working_set_step_one_path);
			Job selectWorkingSet_step_one = new Job();
			selectWorkingSet_step_one.setJobName("Select Workingset Step One");
			FileInputFormat.addInputPath(selectWorkingSet_step_one,
					active_set_path);
			FileOutputFormat.setOutputPath(selectWorkingSet_step_one,
					select_working_set_step_one_path);
			selectWorkingSet_step_one.setJarByClass(Svm.class);
			selectWorkingSet_step_one
					.setMapperClass(SelectWorkingSetMapper.class);
			selectWorkingSet_step_one.setNumReduceTasks(1);
			selectWorkingSet_step_one
					.setReducerClass(SelectWorkingSetReducer.class);
			selectWorkingSet_step_one.waitForCompletion(true);

			String i_data = null;
			double Gmax;
			Path select_working_set_one_file_path = new Path(
					select_working_set_step_one_addr + "/part-r-00000");
			fs = FileSystem.get(URI.create(select_working_set_step_one_addr),
					conf);
			if (fs.exists(select_working_set_one_file_path)) {
				FSDataInputStream is = fs
						.open(select_working_set_one_file_path);
				FileStatus stat = fs
						.getFileStatus(select_working_set_one_file_path);
				byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat
						.getLen()))];
				is.readFully(0, buffer);
				is.close();
				fs.close();
				String select_i_result = new String(buffer);
				String[] select_split = select_i_result.split("\t");
				i_data = select_split[1].trim();
				Gmax = Double.parseDouble(select_split[0]);
			} else {
				throw new Exception("the file is not found .");
			}

			conf = new Configuration();
			hdfs = FileSystem.get(conf);
			hdfs.delete(select_working_set_step_two_path);
			Configuration select_working_set_step_two_config = new Configuration();
			select_working_set_step_two_config.set("data", i_data);
			select_working_set_step_two_config.set("Gmax",
					Double.toString(Gmax));
			Job select_working_set_step_two = new Job(
					select_working_set_step_two_config);
			select_working_set_step_two.setJobName("Select Working Set Step Two");
			FileInputFormat.addInputPath(select_working_set_step_two,
					active_set_path);
			FileOutputFormat.setOutputPath(select_working_set_step_two,
					select_working_set_step_two_path);
			select_working_set_step_two.setJarByClass(Svm.class);
			select_working_set_step_two
					.setMapperClass(SelectWorkingSetTwoMapper.class);
			select_working_set_step_two.setNumReduceTasks(1);
			select_working_set_step_two
					.setReducerClass(SelectWorkingSetTwoReducer.class);
			select_working_set_step_two.waitForCompletion(true);

			Path select_working_set_two_file_path = new Path(
					select_working_set_step_two_addr + "/part-r-00000");
			fs = FileSystem.get(URI.create(select_working_set_step_two_addr),
					conf);
			String j_data = null;
			if (fs.exists(select_working_set_two_file_path)) {
				FSDataInputStream is = fs
						.open(select_working_set_two_file_path);
				FileStatus stat = fs
						.getFileStatus(select_working_set_two_file_path);
				byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat
						.getLen()))];
				is.readFully(0, buffer);
				is.close();
				fs.close();
				String select_j_result = new String(buffer);
				String[] select_two_split = select_j_result.split("\t");
				j_data = select_two_split[1].trim();
			}
			SvmData i_svmdata = new SvmData(i_data.split(" "));
			SvmData j_svmdata = new SvmData(j_data.split(" "));
			double original_i_alpha = i_svmdata.alpha;
			double original_j_alpha = j_svmdata.alpha;
			update_alpha(i_svmdata, j_svmdata);
			double delta_alpha_i = i_svmdata.alpha - original_i_alpha;
			double delta_alpha_j = j_svmdata.alpha - original_j_alpha;

			Configuration update_config = new Configuration();
			update_config.set("i", i_svmdata.toString());
			update_config.set("j", j_svmdata.toString());
			update_config.set("delta_alpha_i", Double.toString(delta_alpha_i));
			update_config.set("delta_alpha_j", Double.toString(delta_alpha_j));
			update_config.set("kernel_type", Integer.toString(kernel_type));
			update_config.set("coef0", Double.toString(coef0));
			update_config.set("degree", Integer.toString(degree));
			update_config.set("gamma", Double.toString(gamma));
			update_config.set("C", Double.toString(C));

			conf = new Configuration();
			hdfs = FileSystem.get(conf);
			if (hdfs.exists(new Path(active_set_addr))) {
				Job update_active = new Job(update_config);
				update_active.setJobName("Update Active Set");
				FileInputFormat.addInputPath(update_active, active_set_path);
				FileOutputFormat.setOutputPath(update_active,
						active_update_path);
				update_active.setJarByClass(Svm.class);
				update_active.setMapperClass(UpdateMapper.class);
				update_active.waitForCompletion(true);
				hdfs.delete(active_set_path);
				hdfs.rename(active_update_path, active_set_path);
			}

			conf = new Configuration();
			hdfs = FileSystem.get(conf);
			if (hdfs.exists(new Path(inactive_set_addr))) {
				Job update_inactive = new Job(update_config);
				update_inactive.setJobName("Update Inactive Set");
				FileInputFormat
						.addInputPath(update_inactive, inactive_set_path);
				FileOutputFormat.setOutputPath(update_inactive,
						inactive_update_path);
				update_inactive.setJarByClass(Svm.class);
				update_inactive.setMapperClass(UpdateMapper.class);
				update_inactive.waitForCompletion(true);
				hdfs.delete(inactive_set_path);
				hdfs.rename(inactive_update_path, inactive_set_path);
			}
			iter++;
		}
		// }

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

	public static void update_alpha(SvmData i, SvmData j) {
		double Q = i.y * j.y * kernel_function(kernel_type, i.values, j.values);
		if (i.y != j.y) {
			double quad_coef = i.kernel_value + j.kernel_value + 2 * Q;
			if (quad_coef <= 0)
				quad_coef = 1e-12;
			double delta = (-i.G - j.G) / quad_coef;
			double diff = i.alpha - j.alpha;
			i.alpha += delta;
			j.alpha += delta;

			if (diff > 0) {
				if (j.alpha < 0) {
					j.alpha = 0;
					i.alpha = diff;
				}
			} else {
				if (i.alpha < 0) {
					i.alpha = 0;
					j.alpha = -diff;
				}
			}
			if (diff > 0) {
				if (i.alpha > C) {
					i.alpha = C;
					j.alpha = C - diff;
				}
			} else {
				if (j.alpha > C) {
					j.alpha = C;
					i.alpha = C + diff;
				}
			}
		} else {
			double quad_coef = i.kernel_value + j.kernel_value - 2 * Q;
			if (quad_coef <= 0)
				quad_coef = 1e-12;
			double delta = (i.G - j.G) / quad_coef;
			double sum = i.alpha + j.alpha;
			i.alpha -= delta;
			j.alpha += delta;

			if (sum > C) {
				if (i.alpha > C) {
					i.alpha = C;
					j.alpha = sum - C;
				}
			} else {
				if (j.alpha < 0) {
					j.alpha = 0;
					i.alpha = sum;
				}
			}
			if (sum > C) {
				if (j.alpha > C) {
					j.alpha = C;
					i.alpha = sum - C;
				}
			} else {
				if (i.alpha < 0) {
					i.alpha = 0;
					j.alpha = sum;
				}
			}
		}
	}
}
