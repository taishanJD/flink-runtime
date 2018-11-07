package com.quark.datastream.runtime.engine.flink.connectors.email_sink;

import com.google.gson.Gson;
import com.quark.datastream.runtime.task.DataSet;
import com.quark.datastream.runtime.task.util.EmailUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class EmailSinkNode extends RichSinkFunction<DataSet> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailSinkNode.class);

    // 邮箱服务器地址
    private final String server;
    // 邮箱服务器端口号
    private final String port;
    // 用户名
    private final String userName;
    // 密码
    private final String password;
    // 收件人
    private final String recipients;
    // 自定义收件人
    private final String custom;
    // 邮件主题
    private final String subject;
    //邮件内容key
    private final String key;

    // 邮件内容类型,纯文本格式
    private static final String CONTENTTYPE = "text/plain;charset=utf-8";

    public EmailSinkNode(String server, String port, String userName, String password, String recipients, String custom, String subject, String key) {
        this.server = server;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.recipients = recipients;
        this.custom = custom;
        this.subject = subject;
        this.key = key;
    }

    @Override
    public void invoke(DataSet in) {
        LOGGER.info("email sink in:{}", in);
        // 将收件人与自定义收件人合并
        String[] recipientPlus = mergeArray(recipients, custom);
        String[] tokens = this.key.split("\\.");
        Gson gson = new Gson();
        StringBuffer sb = new StringBuffer();
        sb.append("/records");
        for (String token : tokens) {
            sb.append("/" + token);
        }
        List records = in.getValue(sb.toString(), List.class);
        for (int i = 0; i < records.size(); i++) {
            // 发送邮件
            String content = gson.toJson(records.get(i));
            LOGGER.info("email sink out:{}", content);
            EmailUtil.sendEmail(server, port, userName, password, userName, recipientPlus, subject, content, CONTENTTYPE);
        }

    }

    private String[] mergeArray(String recipients, String custom) {
        if (null == recipients || recipients.equals("")) {
            String[] customArr = custom.split(",");
            return customArr;
        }
        if (null == custom || custom.equals("")) {
            String[] recipientsArr = recipients.split(",");
            return recipientsArr;
        }
        String[] recipientsArr = recipients.split(",");
        String[] customArr = custom.split(",");
        int firstLength = recipientsArr.length;
        recipientsArr = Arrays.copyOf(recipientsArr, firstLength + customArr.length);
        System.arraycopy(customArr, 0, recipientsArr, firstLength, customArr.length);
        return recipientsArr;
    }

}
