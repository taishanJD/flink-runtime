package com.quark.datastream.runtime.task;

import org.junit.Test;

public class TaskParamTest {

  @Test
  public void testUiFieldType() {
    for (TaskParam.UiFieldType uiFieldType : TaskParam.UiFieldType.values()) {
      System.out.println(uiFieldType.getUiFieldTypeText());
    }
  }

}
