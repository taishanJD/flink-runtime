package com.quark.datastream.runtime.task;

import java.io.Serializable;
import java.util.List;

public interface TaskNode extends Serializable {

  /**
   * 任务类型
   */
  TaskType getType();

  /**
   * 任务名称
   * @return
   */
  String getName();

  /**
   * 设置任务节点参数
   * @param param
   */
  void setParam(TaskNodeParam param);

  /**
   * 设置输入记录key
   * @param inRecordKeys
   */
  void setInRecordKeys(List<String> inRecordKeys);

  /**
   * 设置输出记录key
   * @param outRecordKeys
   */
  void setOutRecordKeys(List<String> outRecordKeys);

  /**
   * 任务节点计算逻辑
   * @param in
   * @return
   */
  DataSet calculate(DataSet in);
}
