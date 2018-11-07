package com.quark.datastream.runtime.engine.flink.task;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;

import com.quark.datastream.runtime.task.TaskNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeLoader.class);
  private URLClassLoader urlClassLoader = null;
  private ClassLoader classLoader = null;
  private String jarPath;

  /**
   * Class constructor specifying model jar path and classloader.
   *
   * @param jarPath Task model jar path in the job jar file
   * @param classLoader Classloader to use for task model
   */
  public NodeLoader(String jarPath, ClassLoader classLoader) {
    if (null == jarPath) {
      throw new NullPointerException("Jar path should be specified");
    }

    this.jarPath = jarPath;
    this.classLoader = classLoader;
  }

  /**
   * Returns TaskModel object that is loaded from a jar for task model.
   *
   * @param modelName Class name of task model to load
   * @return TaskModel object dynamically loaded from the given model
   * @throws Exception If loading the class named model name is failed.
   */
  public TaskNode newInstance(String modelName) throws Exception {
    loadJar(jarPath);
    Class<TaskNode> cls = getClassInstance(modelName);

    if (Modifier.isAbstract(cls.getModifiers())) {
      // if abstract class, we cannot instantiate
      return null;
    } else {
      Object obj = cls.newInstance();
      return (TaskNode) obj;
    }
  }

  private Class getClassInstance(String className)
      throws ClassNotFoundException, NoClassDefFoundError {
    return urlClassLoader.loadClass(className);
  }

  /*
    Jar file for Target TaskModel exists inside the root of Flink job jar file.
    In other words, jar:file://[TASK MODEL NAME].jar
   */
  private void loadJar(String jarPath) throws Exception {
    String inJarPath = "/" + jarPath;
    File targetJar = new File(jarPath);
    LOGGER.info("HH {}", targetJar.getAbsolutePath());
    if (!targetJar.exists()) {
      LOGGER.info("HH {}", getClass().getResource("/" + jarPath));
      try (InputStream jarStream = getClass().getResourceAsStream(inJarPath)) {
        Files.copy(jarStream, targetJar.getAbsoluteFile().toPath());
      }
    }

    this.urlClassLoader = new URLClassLoader(new URL[]{targetJar.toURI().toURL()},
        this.classLoader);

    Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    method.setAccessible(true);
    method.invoke(this.classLoader, new Object[]{targetJar.toURI().toURL()});

    LOGGER.info("URL ClassLoader loaded: " + jarPath);
  }

}
