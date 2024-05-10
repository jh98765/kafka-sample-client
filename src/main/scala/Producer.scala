import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.util.{Properties, Timer, TimerTask}
import java.util.concurrent.{CountDownLatch, Executors}
import scala.sys.exit
import scala.util.Random

object Producer {
  private final val log = LoggerFactory.getLogger(this.getClass)
  private var server = "localhost:9092"

  private var topic: String = "default-topic"
  private var interval: Long = 5000

  private var msg: String = ""

  private var np: Int = 1

  private var username = "kafka"
  private var password = "kafka"

  private var cmsgFlag = false
  private var loopFlag = true

  val latch = new CountDownLatch(1)

  var count = 0

  def main(args: Array[String]): Unit = {

    if (args.length % 2 != 0) {
      println(usage)
      exit(1)
    }

    args.sliding(2, 2).toList.collect {
      case Array("--server", server: String) => this.server = server
      case Array("--topic", topic: String) => this.topic = topic
      case Array("--msg", msg: String) => this.msg = msg; cmsgFlag = true
      case Array("--num", multi: String) => if (np > 0) this.np = multi.toInt else { println(usage); exit(1) }
      case Array("--u", username: String) => this.username = username
      case Array("--pw", password: String) => this.password = password
      case Array("--i", interval: String) => try {
        this.interval = interval.toLong
      } catch {
        case _: Exception =>
          println(usage)
          exit(1)
      }
      case _ => println(usage)
        exit(1)
    }

    val executors = Executors.newFixedThreadPool(np)

    val mainThread = Thread.currentThread()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        loopFlag = false
        executors.shutdown()

        try {
          mainThread.join()
        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
      }
    })

    for (i <- 1 to np)
    executors.execute(() => {
      log.info(s"Producer start : $i")
      produce(i)
    })

    // count
    val cntScheduler = new Timer("kafka-throughput-count")
    cntScheduler.schedule(new TimerTask {
      override def run(): Unit = {
        log.info(s"produce message/s : ${count}")

        count = 0
      }
    }, 1000, 1000)

    latch.await()
    log.info("Producer shutdown")
  }


  def add(): Unit = this.synchronized {
    count += 1
  }

  def produce(i: Int): Unit = {
    var message = ""
    val clientId = s"producer-$i"
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    //props.setProperty(ProducerConfig.ACKS_CONFIG, "-1")
    props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "335544320")

/*
    props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name)
    props.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN")
    props.setProperty(SaslConfigs.DEFAULT_SASL_MECHANISM, "PLAIN")
    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, s"org.apache.kafka.common.security.plain.PlainLoginModule required username='${username}' password='${password}';")
*/

    val producer = new KafkaProducer[String, String](props)


    try {
      while (loopFlag) {

        if (!cmsgFlag) {
          val data = (Math.round(Random.nextFloat() * 50) / 10.0) + 17
          //val data = 10.0
          message = s"""{"test":"test"}"""
        }
        else {
          message = this.msg
        }

        val record = new ProducerRecord[String, String](topic, message)

        producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
          if (exception == null) {
            add()
            /*log.info(s"Metadata : $i" +
              s"\n* Topic : ${metadata.topic}, * Partition : ${metadata.partition()}, * Offset : ${metadata.offset()}" +
              s"\n* Message : $message" +
              s"\n* Timestamp : ${metadata.timestamp()}"
            )*/
          }
          else {
            log.error("* error : ", exception)
          }
        })

        producer.flush()

        if (interval > 0) {
          try {
            Thread.sleep(interval)
          } catch {
            case e: InterruptedException => log.error(s"Inteerupted: ${e.getMessage}")
          }
        }
      }
    } catch {
      case e: Exception => log.error(s"Produce Error: ${e.getMessage}")
    } finally {
      producer.close()
      latch.countDown()
    }
  }

  val usage: String =
    s"""* PRODUCER ARGUMENTS USAGE
       |  [ --server ]  kafka broker server IP:PORT,IP:PORT... (default : "$server")
       |  [ --topic  ]  topic name (default : "$topic")
       |  [ --i      ]  interval to produce message (Milliseconds) (default : $interval)
       |  [ --num    ]  number of kafka producer, must be num > 1 (default : $np)
       |  [ --msg    ]  message to produce (default : sample json object)
       |  [ --u      ]  username for authentication (when server use SASL authentication) (default : "$username")
       |  [ --pw     ]  user password for authentication (when server use SASL authentication) (default : "$username")
       |""".stripMargin
}
