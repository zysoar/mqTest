package cn.com.jms.activeMQ;

import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.Iterator;  
  
import javax.jms.Connection;  
import javax.jms.DeliveryMode;  
import javax.jms.Destination;  
import javax.jms.JMSException;  
import javax.jms.Message;  
import javax.jms.MessageConsumer;  
import javax.jms.MessageListener;  
import javax.jms.MessageProducer;  
import javax.jms.Session;  
import javax.jms.TextMessage;  
import javax.jms.Topic;  
  
import org.apache.activemq.ActiveMQConnection;  
import org.apache.activemq.ActiveMQConnectionFactory;  
  
public class Consumer extends Thread implements MessageListener {  
  
      
    private Session session;
    private Session session2;
    private Session sessionMay;
    private Destination destination;  
    private MessageProducer replyProducer;
    /** 
     * @default 
     * @return org.apache.activemq.ActiveMQConnectionFactory 
     */  
    public ActiveMQConnectionFactory getConnectionFactory(){  
        // default null  
        String userMaster=ActiveMQConnection.DEFAULT_USER;
        // default null  
        String password=ActiveMQConnection.DEFAULT_PASSWORD;  
        // default failover://tcp://localhost:61616  
        String url=ActiveMQConnection.DEFAULT_BROKER_URL;  
        return this.getConnectionFactory(user, password, url);  
    }  
    /** 
     *  
     * @param user 
     *            java.lang.String 
     * @param password 
     *            java.lang.String 
     * @param url 
     *            java.lang.String 
     * @return org.apache.activemq.ActiveMQConnectionFactory 
     */  
    public ActiveMQConnectionFactory getConnectionFactory(String user,  
            String password, String url) {  
        // default null  
        // String user=ActiveMQConnection.DEFAULT_USER;  
        // default null  
        // String password=ActiveMQConnection.DEFAULT_PASSWORD;  
        // default failover://tcp://localhost:61616  
        // String url=ActiveMQConnection.DEFAULT_BROKER_URL;  
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(  
                user, password, url);  
        return connectionFactory;  
    }  
      
      
    /** 
     *  
     * @return javax.jms.Connection 
     */  
    public Connection getConnection(){  
        ActiveMQConnectionFactory connectionFactory = this.getConnectionFactory();  
        return this.getConnection(connectionFactory);  
    }  
  
    /** 
     *  
     * @param connectionFactory org.apache.activemq.ActiveMQConnectionFactory 
     * @return javax.jms.Connection 
     */  
    public Connection getConnection(ActiveMQConnectionFactory connectionFactory) {  
        Connection connection = null;  
        try {  
            if (connectionFactory != null) {  
                connection = connectionFactory.createConnection();  
                connection.start();  
            }  
        } catch (JMSException e) {  
            e.printStackTrace();  
            return null;  
        }  
        return connection;  
    }  
  
    /** 
     *  
     * @return javax.jms.Session 会话 
     */  
    public Session getSession() {  
        Connection connection = this.getConnection();  
        boolean transacted= false;  
        int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;  
        return this.getSession(connection, transacted, acknowledgeMode);  
    }  
      
    /** 
     *  
     * @param connection 
     *            javax.jms.Connection 
     * @param transacted 
     *            boolean 是否是一个事务 
     * @param acknowledgeMode 
     *            int acknowledge 标识 
     * @return javax.jms.Session 会话 
     */  
    public Session getSession(Connection connection, boolean transacted,  
            int acknowledgeMode) {  
        if (connection != null) {  
            if(session==null){  
                try {  
                    session = connection.createSession(transacted, acknowledgeMode);  
                } catch (JMSException e) {  
                    e.printStackTrace();  
                    return null;  
                }  
            }  
        }  
        return session;  
    }  
  
    /** 
     *  
     * @param subject java.lang.String 消息主题 
     * @return javax.jms.Destination 
     */  
    public Destination createDestination(String subject){  
        String mode="Point-to-Point";  
        return this.createDestination(mode, subject);  
    }  
    /** 
     *  
     * @param mode java.lang.String 消息传送模式 
     * @param subject java.lang.String 消息主题 
     * @return javax.jms.Destination 
     */  
    public Destination createDestination(String mode,String subject){  
        session = this.getSession();  
        return this.createDestination(mode, session, subject);  
    }  
      
    /** 
     *  
     * @param mode 
     *            java.lang.String 消息传送模式 
     * @param session 
     *            javax.jms.Session 会话 
     * @param subject 
     *            java.lang.String 消息主题 
     * @return javax.jms.Destination 
     */  
    public Destination createDestination(String mode, Session session,  
            String subject) {  
  
        if (session != null && mode != null && !mode.trim().equals("")) {  
            try {  
                if (mode.trim().equals("Publisher/Subscriber")) {  
                    destination = session.createTopic(subject);  
                } else if (mode.trim().equals("Point-to-Point")) {  
                    destination = session.createQueue(subject);  
                }  
            } catch (JMSException e) {  
                e.printStackTrace();  
                return null;  
            }  
        }  
        return destination;  
    }  
  
    /** 
     *  
     * @return 
     */  
    public MessageProducer createReplyer() {  
        Session session = this.getSession();  
        try {  
            replyProducer = session.createProducer(null);  
            replyProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);  
        } catch (JMSException e) {  
            e.printStackTrace();  
        }  
        return replyProducer;  
    }  
  
    /** 
     *  
     * @return javax.jms.MessageConsumer 
     */  
    public MessageConsumer createConsumer(){  
        Session session = this.getSession();  
        MessageConsumer consumer = null;  
        Destination destination = this.createDestination("TOOL.DEFAULT");//session.createQueue(subject);  
        try {  
            consumer = session.createConsumer(destination);  
        } catch (JMSException e) {  
            e.printStackTrace();  
        }  
        return consumer;  
    }  
      
    public void onMessage(Message message) {  
        try {  
  
            if (message instanceof TextMessage) {  
                TextMessage txtMsg = (TextMessage) message;  
  
                String msg = txtMsg.getText();  
                int length = msg.length();  
                System.out.println("[" + this.getName() + "] Received: '" + msg+ "' (length " + length + ")");  
            }  
  
            if (message.getJMSReplyTo() != null) {  
                Session session =this.getSession() ;  
                MessageProducer replyProducer = this.createReplyer();  
                replyProducer.send(message.getJMSReplyTo(), session.createTextMessage("Reply: "+ message.getJMSMessageID()));  
            }  
            message.acknowledge();  
        } catch (JMSException e) {  
            System.out.println("[" + this.getName() + "] Caught: " + e);  
            e.printStackTrace();  
        } finally {  
              
        }  
    }  
  
    @Override  
    public void run() {  
        //获取会话  
        session = this.getSession();  
        //创建Destination  
        destination = this.createDestination("TOOL.DEFAULT");  
        //创建replyProducer  
        replyProducer = this.createReplyer();  
        MessageConsumer consumer = this.createConsumer();  
        try {  
            consumer.setMessageListener(this);  
        } catch (JMSException e) {  
            e.printStackTrace();  
        }  
    }  
      
    public void testReceiveMessage(){  
        ArrayList<Consumer> threads = new ArrayList<Consumer>();  
        for (int threadCount = 1; threadCount <= 1; threadCount++) {  
            Consumer consumer = new Consumer();  
            consumer.start();  
            threads.add(consumer);  
        }  
        while (true) {  
            Iterator<Consumer> itr = threads.iterator();  
            int running = 0;  
            while (itr.hasNext()) {  
                Consumer thread = itr.next();  
                if (thread.isAlive()) {  
                    running++;  
                }  
            }  
            if (running <= 0) {  
                System.out.println("All threads completed their work");  
                break;  
            }  
        }  
    }  
}  