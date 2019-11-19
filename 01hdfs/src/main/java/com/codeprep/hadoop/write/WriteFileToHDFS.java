package com.codeprep.hadoop.write;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class WriteFileToHDFS {
	
	public static void main(String[] args) {
		
		String localSrc = args[0];
		String dest = args[1];
		
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
			Configuration configuration = new Configuration();
			System.out.println("Connecting to -- " + configuration.get("fs.defaultFS"));
			FileSystem fileSystem = FileSystem.get(URI.create(dest), configuration);
			OutputStream outputStream = fileSystem.create(new Path(dest));
			IOUtils.copyBytes(in, outputStream, 4096, true);
			System.out.println(dest + " copied the file to HDFS");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
