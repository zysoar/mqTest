package cn.com.jms.activeMQ;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Receiver {

	private void method1(Connection connection, Session session,
			MessageConsumer consumer, Integer num) throws JMSException {
		int i = 0;
		while (i < num) {
			i++;
			TextMessage message = (TextMessage) consumer.receive();
			session.commit();
			System.out.println("收到消息:[" + message.getText() + "]");
		}
		session.close();
		connection.close();
	}

	private void method2(MessageConsumer consumer) throws JMSException {
		consumer.setMessageListener(new MessageListener() {
			public void onMessage(Message arg0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				if (arg0 instanceof TextMessage) {
					try {
						System.out.println("收到消息:["
								+ ((TextMessage) arg0).getText() + "]");
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

	private void method3(MessageConsumer consumer) throws JMSException, InterruptedException {
		while (true) {
			Thread.sleep(1000);
			Message msg = consumer.receive(1000);
			TextMessage message = (TextMessage) msg;
			if (null != message) {
				System.out.println("收到消息:[" + message.getText() + "]");
			}
		}
	}

	private void receive(String url, String queue) {
		// ConnectionFactory ：连接工厂，JMS 用它创建连接
		ConnectionFactory connectionFactory;
		// Connection ：JMS 客户端到JMS Provider 的连接
		Connection connection = null;
		// Session： 一个发送或接收消息的线程
		Session session;
		// Destination ：消息的目的地;消息发送给谁.
		Destination destination;
		// 消费者，消息接收者
		MessageConsumer consumer;
		connectionFactory = new ActiveMQConnectionFactory(
				ActiveMQConnection.DEFAULT_USER,
				ActiveMQConnection.DEFAULT_PASSWORD, url);
		try {
			// 构造从工厂得到连接对象
			connection = connectionFactory.createConnection();
			// 启动
			connection.start();
			// 获取操作连接
			session = connection.createSession(Boolean.FALSE,
					Session.AUTO_ACKNOWLEDGE);
			// 获取session注意参数值xingbo.xu-queue是一个服务器的queue，须在在ActiveMq的console配置
			destination = session.createQueue(queue);
			consumer = session.createConsumer(destination);
			method3(consumer);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
	}

	public static void main(String[] args) {
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
		}
		System.out.println("url:[" + url + "],queue:[" + queue + "]");
		new Receiver().receive(url, queue);
	}
}