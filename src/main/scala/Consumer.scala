import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.{Collections, Properties}
import scala.sys.exit

object Consumer {
  private final val log = LoggerFactory.getLogger(this.getClass)
  private var server = "localhost:9092"

  private var topic: String = "default-topic"
  private var group: String = "default-group"

  private var username = "kafka"
  private var password = "kafka"

  private var loopFlag = true

  val latch = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {

    if (args.length % 2 != 0) {
      println(usage)
      exit(1)
    }

    args.sliding(2, 2).toList.collect {
      case Array("--server", server: String) => this.server = server
      case Array("--topic", topic: String) => this.topic = topic
      case Array("--group", group: String) => this.group = group
      case Array("--u", username: String) => this.username = username
      case Array("--pw", password: String) => this.password = password
      case _ => println(usage)
        exit(1)
    }

    val mainThread = Thread.currentThread()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        loopFlag = false
        try {
          mainThread.join()
        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
      }
    })

    consume()
    latch.await()
    log.info("Consumer shutdown")
  }

  def consume(): Unit = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

/*
    props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name)
    props.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN")
    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, s"org.apache.kafka.common.security.plain.PlainLoginModule required username='${username}' password='${password}';")
*/


    val consumer = new KafkaConsumer[String, String](props)

    try {
      consumer.subscribe(Collections.singletonList(topic))

      while (loopFlag) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

        records.forEach {
          record =>
            log.info(s"Key : ${record.key()}, Message : ${record.value()}" +
              s"\nPartition : ${record.partition()}, Offset : ${record.offset()}" +
              s"\nTimestamp : ${record.timestamp()}, Current : ${System.currentTimeMillis()}, Term : ${Math.abs(record.timestamp() - System.currentTimeMillis())}")
        }

        try {
          Thread.sleep(100)
        } catch {
          case e: InterruptedException => log.error(s"Inteerupted: ${e.getMessage}")
        }
      }
      consumer.wakeup()
    } catch {
      case e: Exception => log.error(s"Consume Error: ${e.getMessage}")
    } finally {
      consumer.close()
      latch.countDown()
    }
  }
  val usage: String =
    s"""* CONSUMER ARGUMENTS USAGE
       |  [ --server ]  kafka broker server IP:PORT (default : "$server")
       |  [ --topic  ]  topic name (default : "$topic")
       |  [ --group  ]  consumer group name (default : "$group")
       |  [ --u      ]  username for authentication (when server use SASL authentication) (default : "$username")
       |  [ --pw     ]  user password for authentication (when server use SASL authentication) (default : "$username")
       |""".stripMargin
}
