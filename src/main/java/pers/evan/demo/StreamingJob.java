/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pers.evan.demo;

import com.google.protobuf.StringValue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.RequestReplyFunctionBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamingJob.class);

	private static final String NAMESPACE = "pers.evan.demo";
	private static final FunctionType GREET = new FunctionType(NAMESPACE, "greet");
	private static final FunctionType REMOTE_GREET = new FunctionType(NAMESPACE, "greeter");
	private static final EgressIdentifier<TypedValue> GREETINGS =
			new EgressIdentifier<>(NAMESPACE, "greetings", TypedValue.class);
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String host = "127.0.0.1:9092";

		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setTopics("names")
				.setGroupId("name-consumer")
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setStartingOffsets(OffsetsInitializer.earliest())
				.build();

		DataStream<String> names = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka");

		DataStream<RoutableMessage> namesIngress =
				names.map(name ->
				{
					LOGGER.info("{} is coming", name);
					return RoutableMessageBuilder.builder()
							.withTargetAddress(REMOTE_GREET, name)
							.withSourceAddress(GREET, name)
							.withMessageBody(TypedValue.newBuilder()
									.setValue(StringValue.of(name).getValueBytes())
									.setTypename("pers.evan.demo/greeter")
									.setHasValue(true)
									.build())
							.build();
				});

		StatefulFunctionEgressStreams egresses =
				StatefulFunctionDataStreamBuilder.builder(NAMESPACE)
						.withDataStreamAsIngress(namesIngress)
						.withRequestReplyRemoteFunction(
								RequestReplyFunctionBuilder.requestReplyFunctionBuilder(
										REMOTE_GREET, URI.create("http://localhost:8000/statefun"))
										.withMaxRequestDuration(Duration.ofSeconds(15))
										.withMaxNumBatchRequests(500))
						.withEgressId(GREETINGS)
						.build(env);

		FlinkKafkaProducer<String> producer =
				new FlinkKafkaProducer<>(host, "greetings", new SimpleStringSchema());

		egresses.getDataStreamForEgressId(GREETINGS).map(m -> new String(m.toByteArray())).addSink(producer);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
