package com.quark.datastream.runtime.topologygenerator.engine.instant;

import java.util.List;

import com.quark.datastream.runtime.common.job.Job;
import com.quark.datastream.runtime.common.job.RuntimeInst;
import com.quark.datastream.runtime.topologygenerator.engine.AbstractEngine;

/**
 * in memory引擎
 */
public class InstantEngine extends AbstractEngine {

    public InstantEngine(String host, int port) {
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
