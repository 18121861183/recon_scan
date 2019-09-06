package com.colasoft.recon;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;


public class Start {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/recon?useUnicode=true&characterEncoding=utf-8&useSSL=false";

    //  Database credentials -- 数据库名和密码自己修改
    private static final String USER = "root";
    private static final String PASS = "123456";

    public static void main(String[] args) throws Exception {
        Class.forName(JDBC_DRIVER);

        System.out.println("Connecting to database...");
        Connection conn = DriverManager.getConnection(DB_URL,USER,PASS);
        Statement stmt = conn.createStatement();

        System.setProperty("java.security.auth.login.config", "/home/zyc/jass_kafka_client.conf");
        System.setProperty("java.security.krb5.conf", "/home/zyc/krb5.conf");
        // 环境变量添加，需要输入配置文件的路径System.out.println("===================配置文件地址"+fsPath+"\\conf\\cons_client_jaas.conf");
        Properties props = new Properties();
        props.put("bootstrap.servers", "dev242:9093");
        props.put("group.id", "group_run22");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka_server");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.subscribe(Arrays.asList("TOPSIGHT-AUTO-SCAN,TOPSIGHT-AUTO-SCAN-PRIOR".split(",")));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(5000);
            for (ConsumerRecord<String, String> record : records){
                String topic = record.topic();
                JSONObject object = JSONObject.parseObject(record.value());
                if (object == null) {
                    System.out.println(record.value());
                    continue;
                }
                String ip = object.getString("ip");
                if (ip==null) {
                    continue;
                }
                String query = "select * from recon_receivescans where ip='"+ip+"'";
                ResultSet execute = stmt.executeQuery(query);
                if (execute.next()) {
                    continue;
                }
                Integer flag;
                if (topic.equals("TOPSIGHT-AUTO-SCAN-PRIOR")) {
                    flag = 1;
                } else if (topic.equals("TOPSIGHT-AUTO-SCAN")) {
                    flag = 0;
                } else {
                    break;
                }
                String sql = "INSERT INTO recon_receivescans VALUES ('"+ip+"', 0, "+flag+")";
                System.out.println("插入任务队列: "+topic+" ; IP: "+ ip);
                stmt.execute(sql);
            }
        }

    }
}