package com.microsoft.example.obfuscate;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.mindrot.jbcrypt.BCrypt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class ObfuscateRunner extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			writeUsage();
			return 1;
		}
		Path secretsPath = new Path(args[0]);
		Path saltFilePath = new Path(args[1]);
		Path inputPath = new Path(args[2]);
		Path outputPath = new Path(args[3]);
		// Make sure the salt file exists
		generateSaltIfNeeded(saltFilePath, secretsPath);
		// Configure the job
		Job job = configureJob(secretsPath, saltFilePath, inputPath, outputPath);
		// Run it
		long startTime = System.currentTimeMillis();
		job.submit();
		if (job.waitForCompletion(true)) {
			System.out.printf("Done obfuscating - took %d seconds.\n",
					(System.currentTimeMillis() - startTime) / 1000);
		} else {
			System.err.printf("Job finished with errors: %s\n", job.getStatus().getFailureInfo());
			return 2;
		}
		return 0;
	}

	private void writeUsage() {
		System.out.printf(
				"Usage: hadoop jar <jarPath> %s <secretsPath> <saltFilePath> <inputPath> <outputPath>\n",
				getClass().getName());
	}

	private void generateSaltIfNeeded(Path saltFilePath, Path secretsPath) throws Exception {
		FileSystem fileSystem = FileSystem.get(saltFilePath.toUri(), getConf());
		if (!fileSystem.exists(saltFilePath)) {
			FSDataOutputStream outputStream = fileSystem.create(saltFilePath);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
			int numSaltsToGenerate = getNumSecrets(secretsPath);
			System.out.printf("Generating %d salts\n", numSaltsToGenerate);
			for (int i = 0; i < numSaltsToGenerate; i++) {
				writer.write(BCrypt.gensalt());
				writer.newLine();
			}
			writer.close();
			outputStream.close();
		}
	}

	private int getNumSecrets(Path secretsPath) throws Exception {
		FileSystem fileSystem = FileSystem.get(secretsPath.toUri(), getConf());
		FSDataInputStream inputStream = fileSystem.open(secretsPath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		String currentLine;
		int numLines = 0;
		while ((currentLine = reader.readLine()) != null) {
			if (!currentLine.isEmpty()) {
				numLines++;
			}
		}
		reader.close();
		return numLines;
	}

	private Job configureJob(Path secretsPath, Path saltFilePath,
													 Path inputPath, Path outputPath) throws Exception {
		Job job = Job.getInstance(getConf());
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.getConfiguration().set(ObfuscateMapper.SECRET_WORDS_FILE_KEY, secretsPath.toString());
		job.getConfiguration().set(ObfuscateMapper.SALT_FILE_KEY, saltFilePath.toString());
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ObfuscateMapper.class);
		job.setNumReduceTasks(0);
		job.setJarByClass(getClass());
		FileSystem.get(outputPath.toUri(), getConf()).delete(outputPath, true);
		return job;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ObfuscateRunner(), args);
	}}
