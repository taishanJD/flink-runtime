package com.quark.datastream.runtime.topologygenerator;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.quark.datastream.runtime.common.Settings;

@SpringBootApplication
public class TopologyGeneratorApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(TopologyGeneratorApplication.class);

	private static Settings settings = Settings.getInstance();

	private static void initialize() throws Exception {
		// 检查资源目录是否创建
		makeResourceDirectoryIfNecessary();
	}

	private static void makeResourceDirectoryIfNecessary() throws Exception {
		File fwJarPath = new File(settings.getFwJarPath());
		makeDirectory(fwJarPath);

		File customJarPath = new File(settings.getCustomJarPath());
		makeDirectory(customJarPath);

		File resourcePath = new File(settings.getResourcePath());
		makeDirectory(resourcePath);

	}

	private static void makeDirectory(File dirPath) throws Exception {
		if (!dirPath.exists()) {
			boolean success = dirPath.mkdirs();
			if (!success) {
				throw new Exception("Failed to create " + dirPath.getAbsolutePath());
			}
		} else if (!dirPath.isDirectory()) {
			throw new Exception(dirPath.getAbsolutePath() + " is not a directory.");
		}
	}

	public static void main(String[] args) throws Exception {
		initialize();
		LOGGER.info("Topology-Generator Application Start");
		SpringApplication.run(TopologyGeneratorApplication.class, args);
	}
}
