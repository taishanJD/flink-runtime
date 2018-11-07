package com.quark.datastream.runtime.topologygenerator.engine.batch;

import java.util.List;

import com.quark.datastream.runtime.common.job.Job;
import com.quark.datastream.runtime.common.job.RuntimeInst;
import com.quark.datastream.runtime.topologygenerator.engine.AbstractEngine;

/**
 * 批处理引擎
 */
public class BatchEngine extends AbstractEngine {

    public BatchEngine(String host, int port) {
        super(host, port);
    }

    /**
     * 创建作业
     *
     * @param job
     * @throws Exception
     */
    @Override
    public void create(Job job) throws Exception {

    }

    /**
     * 运行作业
     *
     * @param job
     * @return
     * @throws Exception
     */
    @Override
    public String run(Job job) throws Exception {
        return null;
    }

    /**
     * 停止作业
     *
     * @param job
     * @throws Exception
     */
    @Override
    public void stop(Job job) throws Exception {

    }

    /**
     * 删除作业
     *
     * @param job
     * @throws Exception
     */
    @Override
    public void delete(Job job) throws Exception {

    }

    /**
     * 作业详情
     *
     * @param jon
     * @return
     * @throws Exception
     */
    @Override
    public RuntimeInst detail(Job jon) throws Exception {
        return null;
    }

    /**
     * 作业列表
     *
     * @return
     * @throws Exception
     */
    @Override
    public List<RuntimeInst> jobList() throws Exception {
        return null;
    }
}
