import com.jd.bdp.jdq.JDQ_CLIENT_INFO_ENV;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * @author wenjianli
 * @date 2022/4/24 6:11 下午
 */
public class jdqConsumerDemo {
    public static  void main(String[] args) {

        // 如果你的配置信息属于 jdq 线下测试环境，请配置如下信息
        JDQ_CLIENT_INFO_ENV.authClientInfoENV("t.bdp.jd.com",80);
        JDQ_CLIENT_INFO_ENV.setClusterAddress("t.bdp.jd.com");

        // JDQ_ENV.authClinetNV("OFFLINE_TEST", 80);//线下环境测试的时候如果鉴权的时候爆出：找不到CLIRNT_ID的时候加上这个试试
        String brokerlist = "jdqsuqiantest.jd.local:9008";
        String clientId ="P4d503dec";
        String topic = "jdq_topic_test_wjl";
        String group = "wjl1650796613033";

        /**
         * 注意:
         * 1. clientId
         *      线上环境会验证clientId是否属于当前用户,所有这里的clientId一定要用用户申请下来的clientId
         *      另外这里的clientId也会授权提速(默认5MB/s),非授权的clientId速度为1KB/s
         */
        Properties props = getProperties(brokerlist, clientId, group);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        Collection<String> topics = Arrays.asList(topic);

        /**
         * 参考方法:
         *
         * consumer.partitionsFor(topic)
         *      查询topic的分区信息,当本地没有这个topic的元数据信息的时候会往服务端发送的远程请求
         *      注意: 没有权限的topic的时候会抛出异常(org.apache.kafka.common.errors.TopicAuthorizationException)
         *
         * consumer.position(new TopicPartition(topic, 0))
         *      获取下次拉取的数据的offset, 如果没有offset记录则会抛出异常
         *
         * consumer.committed(new TopicPartition(topic, 0))
         *      获取已提交的位点信息，如果没有查询到则返回null
         *
         * consumer.beginningOffsets(Arrays.asList(new TopicPartition(topic, 0)));
         * consumer.endOffsets(Arrays.asList(new TopicPartition(topic, 0)));
         * consumer.offsetsForTimes(timestampsToSearch);
         *      查询最小,最大,或者任意时间的位点信息
         *
         * consumer.seek(new TopicPartition(topic, 0), 10972);
         * consumer.seekToBeginning(Arrays.asList(new TopicPartition(topic, 0)));
         * consumer.seekToEnd(Arrays.asList(new TopicPartition(topic, 0)));
         *      设置offset给消费者拉取消息
         *
         * consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
         *      手动分配消费的topic和分区进行消费，这里不会出发group management操作,指定分区消费数据
         *      和subscribe方法互斥,如果assign 之后或者之后调用subscribe 则会报错，不允许再进行分配，2方法不能一起使用
         *
         * consumer.subscribe(topics);
         *      自动发布消费的topic进行消费,这里触发group management操作
         *      和assign方法互斥,如果subscribe 之后或者之后调用assign 则会报错，不允许再进行分配，2方法不能一起使用
         *
         * 注: group management
         *      根据group 进行topic分区内部的消费rebanlance
         *      例如消费的topic包含3个分区，启动了4个相同鉴权的客户端消费
         *              分区0 -- consumer1   分区1 -- consumer2   分区2 --- consumer3    consumer4则会空跑不消费数据
         *         当分区consumer1挂掉的时候则会出现rebalance，之后变为
         *              分区0 -- consumer2   分区1 -- consumer3   分区2 --- consumer4
         *
         */
        consumer.subscribe(topics);

        try{
            while(!Thread.currentThread().isInterrupted()){
                ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
                for(ConsumerRecord<byte[], byte[]> record : records){
                    System.out.println("[offset]:" + record.offset() +", [key]:" + new String(record.key()) +", [value]:" + new String(record.value()));
                }
                /**
                 * 提交位点:支持自动提交和手动提交
                 *
                 * 自动提交:
                 *      需要设置ConsumerConfig中  enable.auto.commit 为true
                 *      同时可设置提交时间间隔   auto.commit.interval.ms 默认为5000
                 *
                 * 手动提交:
                 * 1. 同步提交位点:
                 *      consumer.commitSync();
                 *      consumer.commitSync(offsets); // //如果使用的是subcribe()方式订阅的topic，那么在使用传参方式提交位点的时候注意先获取一下该consumer的assignment().
                 *   Set<TopicPartition> assignment = consumer.assignment();//这样是为了防止由于reblance或者其他原因导致该consumer提交不属于自己分配的topic分区的位点的时候报错。
                 * 2. 异步提交位点:
                 *      consumer.commitAsync();
                 *      consumer.commitAsync(offsetCommitCallback);// //如果使用的是subcribe()方式订阅的topic，那么在使用传参方式提交位点的时候注意先获取一下该consumer的assignment().
                 *      consumer.commitAsync(offsets,offsetCommitCallback);// //如果使用的是subcribe()方式订阅的topic，那么在使用传参方式提交位点的时候注意先获取一下该consumer的assignment().
                 *   Set<TopicPartition> assignment = consumer.assignment();//这样是为了防止由于reblance或者其他原因导致该consumer提交不属于自己分配的topic分区的位点的时候报错。
                 */
            }
        }finally {
            /**
             * 注意在不用的时候释放资源
             */
            consumer.close();
        }

    }

    private static Properties getProperties(String brokerlist, String clientId, String group) {

        Properties props = new Properties();

        /**
         * 注意: 线下测试环境和不认证集群需要显示指定PLAINTEXT
         */
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

        /**
         * kafka配置列表，可参考（版本0.10.1.0）http://kafka.apache.org/0101/documentation.html#newconsumerconfigs
         */
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//不设置默认值是latest（第一次消费或者越界从最新开始消费）；earliest：第一次消费或者越界从最小位点开始消费数据

        return props;
    }
}
