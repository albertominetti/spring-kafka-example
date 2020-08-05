package it.minetti.kafkaexample;

import it.minetti.kafkaexample.controller.Sender;
import it.minetti.kafkaexample.service.VeryImportantService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@Slf4j
@SpringBootTest
@EmbeddedKafka(topics = {"myTopic", "myTopic2"})
@TestPropertySource(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class KafkaTest {

    @Autowired
    public KafkaTemplate<String, String> template;

    @Autowired
    public EmbeddedKafkaBroker kafkaEmbedded;

    @MockBean
    public VeryImportantService veryImportantService;

    @Autowired
    public Sender sender;

    @Autowired
    public ConsumerFactory<String, String> consumerFactory;

    @Test
    void when_receiving_a_valid_message_then_is_processed() {
        // given
        Message<String> message = MessageBuilder.withPayload("ciao ciao")
                .setHeader(TOPIC, "myTopic")
                .setHeader("event_type", "VALIDATED")
                .build();

        // when
        template.send(message);

        // then
        Mockito.verify(veryImportantService, timeout(10_000L)).doSomething("ciao ciao");
    }

    @Test
    void then_receiving_an_invalid_message_then_is_ignored() {
        // given
        Message<String> message = MessageBuilder.withPayload("ciao ciao")
                .setHeader(TOPIC, "myTopic")
                .setHeader("event_type", "SUBMITTED")
                .build();

        // when
        template.send(message);

        // then
        Mockito.verify(veryImportantService, timeout(10_000L).times(0)).doSomething(any());
    }

    @Test
    void sender_works() throws InterruptedException, ExecutionException {
        // given
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton("myTopic2"));
        consumer.poll(Duration.ZERO);

        // when
        sender.send("myTopic2", "ciao ciao");
        template.flush();

        // then
        ConsumerRecord<String, String> message = KafkaTestUtils.getSingleRecord(consumer, "myTopic2", 10_000);
        assertThat(message, is(notNullValue()));
        assertThat(message.value(), is(notNullValue()));
        assertThat(message.value(), is("ciao ciao"));
    }

    @Slf4j
    @TestConfiguration
    public static class KafkaTestConfig {

//        @Bean
//        public ConsumerFactory<String, String> consumerFactory(EmbeddedKafkaBroker embeddedKafkaBroker) {
//            log.info("I was here!");
//            return new DefaultKafkaConsumerFactory<>(
//                    KafkaTestUtils.consumerProps("group-id", "false", embeddedKafkaBroker),
//                    new StringDeserializer(),
//                    new StringDeserializer());
//        }

//        @Bean
//        public ProducerFactory<String, String> producerFactory(EmbeddedKafkaBroker embeddedKafkaBroker) {
//            DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(
//                    KafkaTestUtils.producerProps(embeddedKafkaBroker),
//                    new StringSerializer(), new StringSerializer());
//            return pf;
//        }

//        @Bean
//        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
//                ConsumerFactory<String, String> kafkaConsumerFactory) {
//            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//            factory.setConsumerFactory(kafkaConsumerFactory);
//            return factory;
//        }

//        @Bean
//        public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
//            KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
//            return kafkaTemplate;
//        }

//        @Bean
//        public KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
//            KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry = new KafkaListenerEndpointRegistry();
//            return kafkaListenerEndpointRegistry;
//        }
    }
}
