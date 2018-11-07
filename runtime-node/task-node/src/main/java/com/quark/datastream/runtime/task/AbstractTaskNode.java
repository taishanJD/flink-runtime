package com.quark.datastream.runtime.task;

import java.util.List;

/**
 * 任务节点抽象类
 */
public abstract class AbstractTaskNode implements TaskNode {

  @TaskParam(key = "inrecord", uiName = "In records", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "Enter in records")
  private List<String> inRecordKeys;
  @TaskParam(key = "outrecord", uiName = "Out records", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "Enter out records")
  private List<String> outRecordKeys;

  /**
   * This default constructor (with no argument) is required for dynamic instantiation from
   * TaskFactory.
   */
  public AbstractTaskNode() {
  }

  @Override
  public void setInRecordKeys(List<String> inRecordKeys) {
    this.inRecordKeys = inRecordKeys;
  }

  @Override
  public void setOutRecordKeys(List<String> outRecordKeys) {
    this.outRecordKeys = outRecordKeys;
  }

  @Override
  public DataSet calculate(DataSet in) {
    return calculate(in, this.inRecordKeys, this.outRecordKeys);

  }

  /**
   * 节点计算逻辑
   * @param in              输入数据
   * @param inRecordKeys    输入记录key
   * @param outRecordKeys   输出记录key
   * @return
   */
  public abstract DataSet calculate(DataSet in, List<String> inRecordKeys, List<String> outRecordKeys);
}
