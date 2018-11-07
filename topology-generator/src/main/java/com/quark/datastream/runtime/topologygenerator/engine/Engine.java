package com.quark.datastream.runtime.topologygenerator.engine;

import java.util.List;

import com.quark.datastream.runtime.common.job.Job;
import com.quark.datastream.runtime.common.job.RuntimeInst;

/**
 * 运行引擎接口
 */
public interface Engine {

	/**
	 * 创建作业
	 * 
	 * @param job
	 * @throws Exception
	 */
	void create(Job job) throws Exception;

	/**
	 * 运行作业
	 * 
	 * @param job
	 * @return
	 * @throws Exception
	 */
	String run(Job job) throws Exception;

	/**
	 * 停止作业
	 * 
	 * @param job
	 * @throws Exception
	 */
	void stop(Job job) throws Exception;

	/**
	 * 删除作业
	 * 
	 * @param job
	 * @throws Exception
	 */
	void delete(Job job) throws Exception;

	/**
	 * 作业详情
	 * 
	 * @param jon
	 * @return
	 * @throws Exception
	 */
	RuntimeInst detail(Job jon) throws Exception;

	/**
	 * 作业列表
	 * 
	 * @return
	 * @throws Exception
	 */

	List<RuntimeInst> jobList() throws Exception;

}
