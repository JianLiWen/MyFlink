import com.jd.bdp.jdq.JDQ_CLIENT_INFO_ENV;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * @author wenjianli
 * @date 2022/4/24 5:53 下午
 */
public class JdqProducerDemo {
    public static void main(String[] args) throws UnsupportedEncodingException {

        // 如果你的配置信息属于 jdq 线下测试环境，请配置如下信息
        JDQ_CLIENT_INFO_ENV.authClientInfoENV("t.bdp.jd.com",80);
        JDQ_CLIENT_INFO_ENV.setClusterAddress("t.bdp.jd.com");
        // JDQ_ENV.authClinetNV("OFFLINE_TEST", 80);//线下环境测试的时候如果鉴权的时候爆出：找不到CLIRNT_ID的时候加上这个试试
        String brokerlist = "jdqsuqiantest.jd.local:9008";
        String clientId ="P4d503dec";
        String topic = "jdq_topic_test_wjl";

        /**
         * 注意:
         * 1. clientId
         *      线上环境会验证clientId是否属于当前用户,所有这里的clientId一定要用用户申请下来的clientId
         *      另外这里的clientId也会授权提速(默认5MB/s),非授权的clientId速度为1KB/s
         * 2. brokerlist
         *      数据来源为JDQ3.0申请审批后的客户端信息
         *      brokerlist 对应页面中TOPIC信息标签下的 Brokerlist 数据
         */
        Properties props = getProperties(brokerlist, clientId);
        KafkaProducer<byte[],byte[]> producer = new KafkaProducer<byte[],byte[]>(props);

        long index = 0;
        try{
            while(!Thread.currentThread().isInterrupted() && index < 5){
                long begin = System.currentTimeMillis();
                byte[] key = ("testkey" + index).getBytes("UTF-8");
                byte[] val = ("testvalue" + index).getBytes("UTF-8");

                ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>(topic, key, val);

                /**
                 * 默认都是异步发送，如果想同步发送producer.send(xxx).get()单条发送，效率比较低
                 * 每条消息都会有callback，可以根据需要来执行发送成功或者失败后的操作
                 */
                producer.send(record, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception != null){
                            /**
                             * 当存在异常的时候,发送失败,大家可以根据自己需要设置失败逻辑
                             */
                            System.out.println("hhhh - " + exception.getMessage());
                        }else{
                            /**
                             * 没有异常,认为发送成功,也可设置成功之后的逻辑设置
                             * 注意:发送成功之后这里的RecordMetadata内包含此条消息写入到JDQ集群的分区和对应的位点信息等
                             */
                            System.out.println("OFFSET[" + metadata.offset() + "] - PARTITION[" + metadata.partition() + "] - TIME[" + new Timestamp(metadata.timestamp()) + "] is aready send ok");
                        }
                    }
                });

                /**
                 *  flush :  执行flush代表内存中的消息数据都会send到服务端，可根据callback来判断成功与否
                 *  自己控制flush
                 *  注意：这里可发送一批数据之后再掉flush，达到批量的效果
                 */
                producer.flush();

                try {
                    Thread.sleep(1000);//sleep不是必须的
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                index ++;
                System.out.println("------- Send OK ~ costtime[" + (System.currentTimeMillis() - begin) + "]");
            }
        }finally {
            /**
             * 注意在不用的时候释放资源
             */
            producer.close();
        }

    }

    /**
     * kafka配置列表，可参考（版本0.10.1.0）http://kafka.apache.org/0101/documentation.html#producerconfigs
     */
    private static Properties getProperties(String brokerlist, String clientId) {
        Properties props = new Properties();
        //注意: 线下测试环境和不认证集群访问必须显示指定PLAINTEXT
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");//lz4/gzip/snappy，建议lz4
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "262144");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "500");//sdk里面默认linger.ms是100,如果在单条同步发送条件下此值请用户设置成0

        return props;
    }
}
