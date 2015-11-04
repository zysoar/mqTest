package cn.com.jms.activeMQ;

import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Sender {
	
	private void send(String url, String queue,Integer seconds) {
		// ConnectionFactory ：连接工厂，JMS 用它创建连接
		ConnectionFactory connectionFactory;
		// Connection ：JMS 客户端到JMS Provider 的连接
		Connection connection = null;
		// Session： 一个发送或接收消息的线程
		Session session;
		// Destination ：消息的目的地;消息发送给谁.
		Destination destination;
		// MessageProducer：消息发送者
		MessageProducer producer;
		// TextMessage message;
		// 构造ConnectionFactory实例对象，此处采用ActiveMq的实现jar
		connectionFactory = new ActiveMQConnectionFactory(
				ActiveMQConnection.DEFAULT_USER,
				ActiveMQConnection.DEFAULT_PASSWORD, url);
		try {
			// 构造从工厂得到连接对象
			connection = connectionFactory.createConnection();
			// 启动
			connection.start();
			// 获取操作连接
			session = connection.createSession(Boolean.TRUE,
					Session.AUTO_ACKNOWLEDGE);
			// 获取session注意参数值xingbo.xu-queue是一个服务器的queue，须在在ActiveMq的console配置
			destination = session.createQueue(queue);
			// 得到消息生成者【发送者】
			producer = session.createProducer(destination);
			// 设置不持久化，此处学习，实际根据项目决定
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			// 构造消息，此处写死，项目就是参数，或者方法获取
			sendMessage(session, producer,seconds);
			session.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != connection)
					connection.close();
			} catch (Throwable ignore) {
			}
		}
	}

	public static void sendMessage(Session session, MessageProducer producer,Integer seconds)
			throws Exception {
		while (true) {
			TextMessage message = session.createTextMessage("now : "+new Date().getTime());
			// 发送消息到目的地方
			producer.send(message);
			session.commit();
			Thread.sleep(seconds);
		}
	}

	public static void main(String[] args) {
		Integer seconds=1000;
		String url = "tcp://172.31.111.167:61616";
		String queue = "algrithm:172.31.30.155";
		if (args != null) {
			try {
				url = args[0];
			} catch (Exception e) {
			}
			try {
				queue = args[1];
			} catch (Exception e) {
			}
			try {
				seconds = Integer.parseInt(args[2]);
			} catch (Exception e) {
			}
		}
		System.out.println("url:[" + url + "],queue:[" + queue + "],seconds["+seconds+"]");
		new Sender().send(url, queue,seconds);
	}
}
