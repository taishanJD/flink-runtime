package com.quark.datastream.runtime.task.util;

import com.sun.mail.util.MailSSLSocketFactory;

import javax.mail.Authenticator;
import javax.mail.Message.RecipientType;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

/**
 * 发送邮件工具类
 *
 * @author  JiaLei
 */
public class EmailUtil {

    /**
     * 邮件内容类型：纯文本格式
     */
    public static String emailTextContentType = "text/plain;charset=utf-8";
    /**
     * 邮件内容类型：HTML格式
     */
    public static String emailHTMLContentType = "text/html;charset=utf-8";

    /**
     * 发送邮件工具类，采用的是smtp协议，具有SSL加密
     *
     * @param mailServer       
     *        邮件服务器的主机名:如 "smtp.xxx.com"
     * @param loginAccount
     *        邮件发送人的邮箱账号:如 "xxxxx@thundersoft.com"
     * @param loginPassword    
     *        邮件发送人的邮箱登录密码
     * @param sender           
     *        邮件发件人（发送人账号）
     * @param recipients       
     *        收件人，支持群发
     * @param emailSubject     
     *        邮件的主题
     * @param emailContent     
     *        邮件的内容
     * @param emailContentType 
     *        邮件内容的类型,
     *        支持纯文本格式的内容:"text/plain;charset=utf-8";
     *        支持HTML格式的内容:"text/html;charset=utf-8"
     *        
     * @return
     *        邮件发送是否成功 (1:成功、 0:失败)
     */
    public static int sendEmail(String mailServer, String port, final String loginAccount, final String loginPassword, String sender, String[] recipients,
                                String emailSubject, String emailContent, String emailContentType) {
        // 邮件是否发送成功
        int result;

        try {
            // 与smtp服务器建立一个连接
            Properties p = new Properties();
            // 设置邮件服务器主机名
            p.setProperty("mail.host", mailServer);
            // 设置邮件服务器端口号
            p.setProperty("mail.port", port);
            // 发送服务器需要身份验证,要采用指定用户名密码的方式去认证
            p.setProperty("mail.smtp.auth", "true");
            // 发送邮件协议名称
            p.setProperty("mail.transport.protocol", "smtp");

            // 开启SSL加密，否则会失败
            MailSSLSocketFactory sf = new MailSSLSocketFactory();
            sf.setTrustAllHosts(true);
            p.put("mail.smtp.ssl.enable", "true");
            p.put("mail.smtp.ssl.socketFactory", sf);

            // 创建session
            Session session = Session.getDefaultInstance(p, new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    //第一个参数为邮箱账号,第二个为邮箱登录密码
                    PasswordAuthentication passwordAuthentication = new PasswordAuthentication(loginAccount, loginPassword);
                    return passwordAuthentication;
                }
            });

            //设置打开调试状态
            session.setDebug(false);

            //声明一个Message对象(代表一封邮件),从session中创建，可以在这里for循环多次发送多封邮件
            MimeMessage msg = new MimeMessage(session);

            // 邮件信息封装
            // 定义发件人
            msg.setFrom(new InternetAddress(sender));

            // 定义收件人，可以多个
            InternetAddress[] receptientsEmail = new InternetAddress[recipients.length];
            for (int i = 0; i < recipients.length; i++) {
                receptientsEmail[i] = new InternetAddress(recipients[i]);
            }
            msg.setRecipients(RecipientType.TO, receptientsEmail);

            // 邮件内容:主题、内容
            msg.setSubject(emailSubject);
            msg.setContent(emailContent, emailContentType);//发html格式的文本

            // 执行发送
            Transport.send(msg);
            System.out.println("邮件发送成功! Time:" + new Date());
            result = 1;
        } catch (Exception e) {
            System.out.println("邮件发送失败! Time:" + new Date() + ", Exception: " + e.getMessage());
            result = 0;
        }
        return result;
    }
}