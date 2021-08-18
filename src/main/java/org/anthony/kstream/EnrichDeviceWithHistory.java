package org.anthony.kstream;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnrichDeviceWithHistory {
	
	//RAW_TOPIC -> ENRICHED_TOPIC JOIN LAST_SEEN_KTABLE -> LAST_SEEN_KTABLE 
	
	private static final String STATE_STORE_DEVICE_LAST_SEEN = "stateStore-device-last-seen";
	private static final String STATE_STORE_DEVICE_BLOCK_SEEN_COUNT_IN_WINDOW = "stateStore-device-block-seen-count-in-window";
	private static final String STATE_STORE_DEVICE_SEEN_COUNT_IN_WINDOW = "stateStore-device-seen-count-in-window";
	static final String RAW_TOPIC = "device-log";
	static final String ENRICHED_TOPIC = "device-log-enriched";
//	static final String LAST_SEEN_KTABLE = "device-last-seen";  //kafka-topics --create --topic device-last-seen --partitions 4 --config "cleanup.policy=compact"
	//static final String OUTPUT_TOPIC = "device-action-count-in-window";
	
	private static String TRANSFORM_STATE_STORE_ADD_TIMESTAMP_NAME = "transform-stateStore-add-timestamp";
	
	@JsonIgnoreProperties(ignoreUnknown = true) 
	public static class Device {
		
		@JsonIgnoreProperties(ignoreUnknown = true) 
		public static class History {
			private int deviceCountInWindow;
			private int deviceBlockCountInWindow;
			private long lastSeenTimestamp;
			private long windowStartTime;
			
			public int getDeviceCountInWindow() {
				return deviceCountInWindow;
			}
			public void setDeviceCountInWindow(int deviceCountInWindow) {
				this.deviceCountInWindow = deviceCountInWindow;
			}
			public int getDeviceBlockCountInWindow() {
				return deviceBlockCountInWindow;
			}
			public void setDeviceBlockCountInWindow(int deviceBlockCountInWindow) {
				this.deviceBlockCountInWindow = deviceBlockCountInWindow;
			}
			public long getLastSeenTimestamp() {
				return lastSeenTimestamp;
			}
			public void setLastSeenTimestamp(long lastSeenTimestamp) {
				this.lastSeenTimestamp = lastSeenTimestamp;
			}
			public long getWindowStartTime() {
				return windowStartTime;
			}
			public void setWindowStartTime(long windowStartTime) {
				this.windowStartTime = windowStartTime;
			}
			@Override
			public String toString() {
				return "History [deviceCountInWindow=" + deviceCountInWindow
						+ ", deviceBlockCountInWindow=" + deviceBlockCountInWindow + ", lastSeenTimestamp="
						+ lastSeenTimestamp + ", windowStartTime=" + windowStartTime + "]";
			}

			
			
		}
		
		private String id;
		private String action;
		private long count;
		private long timestamp;
		private History history;
		
		private Map<String, String> meta;
		
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getAction() {
			return action;
		}
		public void setAction(String action) {
			this.action = action;
		}
		public long getCount() {
			return count;
		}
		public void setCount(long count) {
			this.count = count;
		}
		public long getTimestamp() {
			return timestamp;
		}
		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}
		public Map<String, String> getMeta() {
			return meta;
		}
		public void setMeta(Map<String, String> meta) {
			this.meta = meta;
		}
		public History getHistory() {
			return history;
		}
		public void setHistory(History history) {
			this.history = history;
		}
		@Override
		public String toString() {
			return "Device [id=" + id + ", action=" + action + ", count=" + count + ", timestamp=" + timestamp
					+ ", history=" + history + ", meta=" + meta + "]";
		}
	}
	
	public static class DeviceSerde implements Serde<Device> {
		private static DeviceSerde INSTANCE = new DeviceSerde();
		
		private final ObjectMapper objectMapper = new ObjectMapper();

		
		@Override
		public Serializer<Device> serializer() {
			return new Serializer<Device>(){
				public byte[] serialize(String topic, Device data) {
			        if (data == null)
			            return null;

			        try {
			            return objectMapper.writeValueAsBytes(data);
			        } catch (Exception e) {
			            throw new SerializationException("Error serializing JSON message", e);
			        }
				}				
			};
		}

		@Override
		public Deserializer<Device> deserializer() {
			return new Deserializer<EnrichDeviceWithHistory.Device>() {
				public Device deserialize(String topic, byte[] bytes) {
			        if (bytes == null)
			            return null;

			        Device data;
			        try {
			            data = objectMapper.readValue(bytes, Device.class);
			        } catch (Exception e) {
			            throw new SerializationException(e);
			        }

			        return data;
				}
			};
		}
		
		public static DeviceSerde get() {
			return INSTANCE;
		}
	}
	
	public static class DeviceContextEnrichTransformer implements 
//		Transformer<String, DevicesSeenInWindow.Device, KeyValue<String,Device>>
		ValueTransformerWithKey<String, Device, Device>
	{
			
		private ProcessorContext context;
		private TimestampedWindowStore<String, Integer> stateDeviceCount;
		private TimestampedWindowStore<String, Integer> stateDeviceBlockCount;
		private KeyValueStore<String, Device> stateLastSeenDevice;

		
		@Override
		public Device transform(String readOnlyKey, Device device) {
			System.out.println("transform() readOnlyKey="+readOnlyKey);
//			KeyValueIterator<Windowed<String>, ValueAndTimestamp<Device>> iter = state.all();
//			int i = 0;
//			for ( ; iter.hasNext() ; ++i ) {
//				KeyValue<Windowed<String>, ValueAndTimestamp<Device>> kv = iter.next();
//				System.out.println("  - kv: " + kv);
//			}
//			System.out.println("  count in window:"+i);

			if(device.getHistory() == null) {
				device.setHistory(new Device.History());
			}
			
			//set device's current timestamp
			device.setTimestamp(context.timestamp());

			long windowStart = Instant.ofEpochMilli(device.getTimestamp()).truncatedTo(ChronoUnit.MINUTES).toEpochMilli();
			device.getHistory().setWindowStartTime(windowStart);
			
			//setDeviceCountInWindow then increment state
			ValueAndTimestamp<Integer> devCountVT = stateDeviceCount.fetch(device.getId(), windowStart);
			int devCount = (devCountVT == null)?0:(devCountVT.value());
			device.getHistory().setDeviceCountInWindow(devCount);
			stateDeviceCount.put(device.getId(), ValueAndTimestamp.make(devCount+1, device.getTimestamp()), windowStart);
			System.out.println("  stateDeviceCount.put("+device.getId()+", ValueAndTimestamp.make("+(devCount+1)+", "+device.getTimestamp()+"), "+windowStart+");");
			
			
			//setDeviceBlockCountInWindow then increment state
			ValueAndTimestamp<Integer> devBlkCountVT = stateDeviceBlockCount.fetch(readOnlyKey, windowStart);
			int devBlkCount = (devBlkCountVT == null)?0:(devBlkCountVT.value());
			device.getHistory().setDeviceBlockCountInWindow(devBlkCount);  
			stateDeviceBlockCount.put(readOnlyKey, ValueAndTimestamp.make(devBlkCount+1, device.getTimestamp()), windowStart);
			
			//set last seen then update state
			Device lastSeenDevice = stateLastSeenDevice.get(device.getId());
			if(lastSeenDevice != null) {
				device.getHistory().setLastSeenTimestamp(lastSeenDevice.getTimestamp());
			}
			stateLastSeenDevice.put(device.getId(), device);
			


			//set topic, partition and offset of the original message
			HashMap<String,String> meta = new HashMap<>();
//				meta.put("applicationId", ctx.applicationId());
			meta.put("topic", context.topic());
			meta.put("partition", Integer.toString(context.partition()));
			meta.put("offset", Long.toString(context.offset()));
			
			device.setMeta(meta);
			
			return device;
		}
		
//		public KeyValue<String, Device> transform(String key, Device value) {
//			value.setTimestamp(context.timestamp());
//			
//			HashMap<String,String> meta = new HashMap<>();
////				meta.put("applicationId", ctx.applicationId());
////				meta.put("topic", ctx.topic());
//			meta.put("partition", Integer.toString(context.partition()));
//			meta.put("offset", Long.toString(context.offset()));
//			
//			value.setMeta(meta);
//			
//			//re-key using devie id
//			return KeyValue.pair(value.getId(), value);
//		}
		
//		public Device transform(Device value) {
//
//		}
//		
		public void init(ProcessorContext context) {
			this.context = context;
			this.stateDeviceCount      = (TimestampedWindowStore<String, Integer>)context.getStateStore(STATE_STORE_DEVICE_BLOCK_SEEN_COUNT_IN_WINDOW);
			this.stateDeviceBlockCount = (TimestampedWindowStore<String, Integer>)context.getStateStore(STATE_STORE_DEVICE_SEEN_COUNT_IN_WINDOW);
			this.stateLastSeenDevice   = (KeyValueStore<String, Device>)context.getStateStore(STATE_STORE_DEVICE_LAST_SEEN);
		}
		
		public void close() {
		}




	}
	
//	public static class DeviceContextEnrichTransformerSupplier implements TransformerSupplier<String, DevicesSeenInWindow.Device, KeyValue<String,Device>> {
//
//		@Override
//		public Transformer<String, Device, KeyValue<String, Device>> get() {
//			return new DeviceContextEnrichTransformer();
//		}
//		
//	     // provide store(s) that will be added and connected to the associated transformer
//	     // the store name from the builder ("myTransformState") is used to access the store later via the ProcessorContext
//		//Set<StoreBuilder<?>>
//		public Set<StoreBuilder<?>> stores() {
//			StoreBuilder<KeyValueStore<String, Device>> keyValueStoreBuilder =
//	                   Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("DeviceContextEnrichTransformerStateStore"),
//	                   Serdes.String(),
//	                   DeviceSerde.get());
//	        return Collections.singleton(keyValueStoreBuilder);
//	    }
//	}

	public static void main(String[] args) {
		final Properties streamsConfiguration = getStreamsConfiguration();

		final Topology topology = getTopologyLastSeen();
//		final Topology topology = getTopologyCountInWindow();
		
	    System.out.println(topology.describe());
	    
	    final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
	    
	    streams.cleanUp();
	    streams.start();

	    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private static Topology getTopologyLastSeen() {
	    final StreamsBuilder builder = new StreamsBuilder();

//	    StoreBuilder<KeyValueStore<String,Device>> transformStateStoreAddTimestamp =
//	            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(TRANSFORM_STATE_STORE_ADD_TIMESTAMP_NAME),
//	                    Serdes.String(),
//	                    DeviceSerde.get());
//	    builder.addStateStore(transformStateStoreAddTimestamp);
	    
	    //State Store to count number of times a device is seen in the time window
	    builder.addStateStore(Stores.timestampedWindowStoreBuilder(Stores.inMemoryWindowStore(STATE_STORE_DEVICE_SEEN_COUNT_IN_WINDOW, Duration.ofMinutes(1), Duration.ofMinutes(1), false), Serdes.String(), Serdes.Integer()));
	    
	    //State Store to count number of times a device block is seen in the time window
	    builder.addStateStore(Stores.timestampedWindowStoreBuilder(Stores.inMemoryWindowStore(STATE_STORE_DEVICE_BLOCK_SEEN_COUNT_IN_WINDOW, Duration.ofMinutes(1), Duration.ofMinutes(1), false), Serdes.String(), Serdes.Integer()));
	    
	    //State Store to store when the device is last seen
	    builder.addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STATE_STORE_DEVICE_LAST_SEEN), Serdes.String(), DeviceSerde.get()));
	    
	    
	    final KStream<String, Device> deviceLogStream = builder.stream(RAW_TOPIC, Consumed.with(Serdes.String(), DeviceSerde.get()));
	    
	    final KStream<String, Device> deviceLogEnrichedStream = deviceLogStream
//	    			   .transform(new TransformerSupplier<String, Device, KeyValue<String, Device>>() {
//
//							public Transformer<String, Device, KeyValue<String, Device>> get() {
//								return new DeviceContextEnrichTransformer();
//							}
//	    			   })
//	    			   .transform(DeviceContextEnrichTransformer::new)
//	    			   .transform(new DeviceContextEnrichTransformerSupplier())
	    		       .transformValues(DeviceContextEnrichTransformer::new, STATE_STORE_DEVICE_SEEN_COUNT_IN_WINDOW, STATE_STORE_DEVICE_BLOCK_SEEN_COUNT_IN_WINDOW, STATE_STORE_DEVICE_LAST_SEEN)
//	    			   .leftJoin(lastSeenTable, (deviceLeft, deviceRight) -> {
//	    				   		if(deviceRight != null) {
//	    				   			deviceLeft.setLastSeenTimestamp(deviceRight.getTimestamp());
//	    				   		}
//	    				   		return deviceLeft;
//	    			   		})
	    			   .peek((key, device) -> System.out.println(Thread.currentThread().getName() + ":" + device))
	    			   ;
	    
	    deviceLogEnrichedStream.to(ENRICHED_TOPIC, Produced.with(Serdes.String(), new DeviceSerde()));
//	    deviceLogEnrichedStream.to(LAST_SEEN_KTABLE, Produced.with(Serdes.String(), new DeviceSerde()));
	    
	    //deviceLogStream.to(ENRICHED_TOPIC, Produced.with(Serdes.String(), new DeviceSerde()));
	    //deviceLogEnrichedStream.foreach((key, device) -> System.out.println(device));

	    
	    

//	    final KTable<String, Device> lastSeenTable = deviceLogStream.toTable(m);
//	    
//	    final ReadOnlyKeyValueStore<String, Device> readonlyStore = streams.
//	    
//	    deviceLogStream.foreach( (key, device) -> { 
//	    	System.out.println(device.toString()); 
//	    });
	    
//	    Materialized<String, Device, KeyValueStore<Bytes, byte[]>> m = Materialized.<String, Device>as(Stores.persistentKeyValueStore("last-seen"));
//	    Materialized<String, Device, KeyValueStore<Bytes, byte[]>> m = Materialized.<String, Device>as(Stores.inMemoryKeyValueStore("last-seen"));
//	    final KTable<String, Device> deviceLogLastSeenTableUpdated = 
//	    		builder.stream(ENRICHED_TOPIC, Consumed.with(Serdes.String(), new DeviceSerde()))
//	    			   .toTable(Materialized.<String, Device, KeyValueStore<Bytes, byte[]>>as(LAST_SEEN_KTABLE).withKeySerde(Serdes.String()).withValueSerde(new DeviceSerde()));
//	    	
	    return builder.build();
	}
	
//	private static Topology getTopologyCountInWindow() {
//	    final StreamsBuilder builder = new StreamsBuilder();
//	    final KStream<String, Device> deviceLogStream = builder.stream(RAW_TOPIC, Consumed.with(Serdes.String(), DeviceSerde.get()));
//	    
//	    
//
//
//
//	    
//	    
//	    
////	    KTable<Windowed<String>, Long> countByIdInWindow = deviceLogStream
////	            .groupByKey()
////	            .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
////	            .count(
////	            		Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("id-count")
////	            		//.withValueSerde(Serdes.Long())
////	           	 )
////	           	.toStream().toTable();
//	    
////	    final KStream<String, Device> joined = deviceLogStream.join(countByIdInWindow, (key, tuple) -> {
////	    	
////	    	device.getMeta().put("countInTimeWindow", deviceCount.toString());
////	    	return device;
////	    });
//	    
//	    //countByIdInWindow.foreach((key, val) -> System.out.println(key + " " + val));
//
//	    
//	    return builder.build();
//	}

	private static Properties getStreamsConfiguration() {
	    final Properties streamsConfiguration = new Properties();

	    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "EnrichDeviceWithHistory");
	    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "EnrichDeviceWithHistory-client");
	    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
//	    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//	    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, DeviceSerde.get().getClass().getName());
	    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
	    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
	    return streamsConfiguration;
	}
}
