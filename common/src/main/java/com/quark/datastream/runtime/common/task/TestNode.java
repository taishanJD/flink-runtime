package com.quark.datastream.runtime.common.task;

import com.quark.datastream.runtime.task.*;

import java.util.List;

/**
 * Class TestNode is provided only for unit tests.
 * Use this only when you want to ensure that your code works with arbitrary model.
 */
public final class TestNode implements TaskNode {

  @TaskParam(key = "age", uiType = TaskParam.UiFieldType.NUMBER, uiName = "Age")
  private int age;

  @TaskParam(key = "gender", uiType = TaskParam.UiFieldType.ENUMSTRING, uiName = "Gender")
  private Gender gender;

  @Override
  public TaskType getType() {
    return TaskType.INVALID;
  }

  @Override
  public String getName() {
    return TestNode.class.getSimpleName();
  }

  @Override
  public void setParam(TaskNodeParam param) {

  }

  @Override
  public void setInRecordKeys(List<String> inRecordKeys) {

  }

  @Override
  public void setOutRecordKeys(List<String> outRecordKeys) {

  }

  @Override
  public DataSet calculate(DataSet in) {
    return in;
  }

  public enum Gender {
    Male, Female
  }
}
