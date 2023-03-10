use std::time::{Duration, SystemTime, UNIX_EPOCH};
use log::LevelFilter;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};

const BROKERS: &str = "127.0.0.1:9092";
const MESSAGE_HEADER: &str = "TestMessage";
const TOPIC: &str = "dev.test-messages";
const RECEIVE_TIMEOUT: i64 = Duration::from_secs(3).as_micros() as i64;
const PARTITION: i32 = 0;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder()
        .format_timestamp_millis()
        .filter_level(LevelFilter::Debug)
        .init();

    let mut cfg_producer = ClientConfig::new();
    configure_producer(&mut cfg_producer);

    let mut cfg_consumer = ClientConfig::new();
    configure_consumer(&mut cfg_consumer);

    let mut producer: ThreadedProducer<DefaultProducerContext> = cfg_producer
        .create_with_context(DefaultProducerContext {})
        .expect("Unable to create threaded producer");

    let mut consumer: StreamConsumer<DefaultConsumerContext> = cfg_consumer
        .create_with_context(DefaultConsumerContext {})
        .expect("Unable to create stream consumer");

    log::info!("Producer config:\n{:?}", cfg_producer);
    log::info!("Consumer config:\n{:?}", cfg_consumer);

    let topic_name = format!("{}/{}", TOPIC, PARTITION);
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(TOPIC, PARTITION);
    log::info!("Assigning consumer to {}", topic_name);
    consumer
        .assign(&partitions)
        .map_err(|e| format!("Cannot assign partition: {}. Error: {}", topic_name, e))?;

    log::info!("Fetching topic watermarks: {}", topic_name);
    let (_, offset) = consumer
        .fetch_watermarks(TOPIC, PARTITION, Duration::from_secs(1))
        .map_err(|e| format!("Cannot fetch watermarks of: {}. Error: {}", topic_name, e))?;

    log::info!("Seeking topic {} to offset: {}", topic_name, offset);
    consumer
        .seek(TOPIC, PARTITION, Offset::Offset(offset), Duration::from_secs(1))
        .map_err(|e| format!("Cannot seek {} to offset: {}. Error: {}", topic_name, offset, e))?;

    log::info!("------- finished init --------\n");

    for i in 1..10+1 {
        let (sent_at, received_at) = send_and_receive(&mut producer, &mut consumer).await?;

        log::info!(
            "Test message {}/10 round trip: {}ms",
            i,
            (received_at - sent_at) as f32 / 1_000.0
        );
    }

    Ok(())
}


fn configure_producer(config: &mut ClientConfig) {
    config.set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "10000")
        .set("queue.buffering.max.ms", "1")
        .set("message.send.max.retries", "100000")
        .set("topic.metadata.refresh.interval.ms", "4")
        .set_log_level(RDKafkaLogLevel::Info);
}

fn configure_consumer(config: &mut ClientConfig) {
    config.set("bootstrap.servers", BROKERS)
        .set("group.id", "some-group-id")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .set("socket.keepalive.enable", "true")
        .set("fetch.wait.max.ms", "100")
        .set_log_level(RDKafkaLogLevel::Info);
}

async fn send_and_receive(producer: &mut ThreadedProducer<DefaultProducerContext>, consumer: &mut StreamConsumer) -> Result<(i64, i64), String> {
    let id = uuid::Uuid::new_v4().to_string();
    let sent_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
    let payload = format!("test message {}", id);
    let message = BaseRecord::to(TOPIC)
        .key("test-key")
        .headers(OwnedHeaders::new().insert(Header { key: MESSAGE_HEADER, value: Some(id.as_str()) }))
        .timestamp(sent_at)
        .payload(payload.as_str());

    producer.send(message).map_err(|(e, _)| format!("Cannot send test message: {:?}", e))?;

    loop {
        let raw_message = consumer.recv().await.expect("Unable to receive test message");
        if raw_message.headers().is_none() {
            continue;
        }

        let mut is_our_message = false;
        for h in raw_message.headers().unwrap().iter() {
            if h.value.is_none() {
                continue;
            }

            if h.key == MESSAGE_HEADER && String::from_utf8_lossy(h.value.unwrap()).to_string() == id {
                is_our_message = true;
                break;
            }
        };

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        if is_our_message {
            break Ok((sent_at, now));
        }

        if now > RECEIVE_TIMEOUT + sent_at {
            break Err(format!(
                "Timeout. No test message with id: {} received within {} ms",
                id,
                (now - sent_at) as f32 / 1_000.0
            ));
        }
    }
}
