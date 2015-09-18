package com.microsoft.example.obfuscate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mindrot.jbcrypt.BCrypt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class ObfuscateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	public static final String SECRET_WORDS_FILE_KEY = "obfuscate.secrets.path";
	public static final String SALT_FILE_KEY = "obfuscate.salt.path";
	private final Map<String, String> _secretsWithHashes = new HashMap<>();
	private Text _outputValue = new Text();
	private final ArrayList<String> _salts = new ArrayList<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path secretsFilePath = new Path(context.getConfiguration().get(SECRET_WORDS_FILE_KEY));
		Path saltFilePath = new Path(context.getConfiguration().get(SALT_FILE_KEY));
		readSalt(saltFilePath, context.getConfiguration());
		readSecrets(secretsFilePath, context.getConfiguration());
	}

	private void readSalt(Path saltFilePath, Configuration configuration) throws IOException {
		FileSystem fileSystem = FileSystem.get(saltFilePath.toUri(), configuration);
		FSDataInputStream inputStream = fileSystem.open(saltFilePath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		String currentLine;
		while ((currentLine = reader.readLine()) != null) {
			if (!currentLine.isEmpty()) {
				_salts.add(currentLine);
			}
		}
		reader.close();
	}

	private void readSecrets(Path secretsFilePath, Configuration configuration) throws IOException {
		FileSystem fileSystem = FileSystem.get(secretsFilePath.toUri(), configuration);
		FSDataInputStream inputStream = fileSystem.open(secretsFilePath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		String currentLine;
		int saltIndex = 0;
		while ((currentLine = reader.readLine()) != null) {
			if (!currentLine.isEmpty()) {
				_secretsWithHashes.put(currentLine, BCrypt.hashpw(currentLine, _salts.get(saltIndex++)));
			}
		}
		reader.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		_outputValue.set(value.toString());
		for (Map.Entry<String, String> secret : _secretsWithHashes.entrySet()) {
			_outputValue.set(_outputValue.toString().replace(secret.getKey(), secret.getValue()));
		}
		context.write(NullWritable.get(), _outputValue);
	}
}
