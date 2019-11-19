package com.codeprep.hadoop.read;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ReadFileFromHDFS {

	public static void main(String[] args) {
		
		String uri = args[0];
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			FSDataInputStream fsdIn = fs.open(new Path(uri));
			IOUtils.copyBytes(fsdIn, System.out, 4096, false);
			System.out.println("End of file read complete!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
