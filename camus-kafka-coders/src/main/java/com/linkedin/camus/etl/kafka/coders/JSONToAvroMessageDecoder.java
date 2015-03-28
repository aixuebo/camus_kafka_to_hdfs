package com.linkedin.camus.etl.kafka.coders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;

public class JSONToAvroMessageDecoder extends MessageDecoder<byte[], GenericData.Record> {
  private static final Logger      log                            = Logger.getLogger(JsonStringMessageDecoder.class);
  public static final String       CAMUS_SCHEMA_ID_FIELD          = "camus.message.schema.id.field";
  public static final String       DEFAULT_SCHEMA_ID_FIELD        = "schemaID";
  JsonParser                       jsonParser;
  private String                   schemaIDField;
  protected DecoderFactory         decoderFactory;
  protected SchemaRegistry<Schema> registry;
  private Schema                   latestSchema;

  public JSONToAvroMessageDecoder() {
    this.jsonParser = new JsonParser();
  }

  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    this.props = props;
    this.topicName = topicName;

    this.schemaIDField = props.getProperty(CAMUS_SCHEMA_ID_FIELD, DEFAULT_SCHEMA_ID_FIELD);
    try {
      SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class.forName(props.getProperty(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS)).newInstance();
      log.info("Prop " + KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS + " is: " + props.getProperty(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS));
      log.info("Underlying schema registry for topic: " + topicName + " is: " + registry);
      registry.init(props);

      this.registry = new CachedSchemaRegistry<Schema>(registry, props);
      this.latestSchema = ((Schema) registry.getLatestSchemaByTopic(topicName).getSchema());
    } catch (Exception e) {
      throw new MessageDecoderException(e);
    }

    this.decoderFactory = DecoderFactory.get();
  }

  public CamusWrapper<GenericData.Record> decode(byte[] payload) {
    String payloadString = new String(payload);
    JsonObject jsonObject;
    try {
      jsonObject = this.jsonParser.parse(payloadString.trim()).getAsJsonObject();
      String templateID = jsonObject.get(schemaIDField).getAsString();
      MessageDecoderHelper helper = new MessageDecoderHelper(this.registry, this.topicName).invoke(templateID);
      GenericRecord datum = null;
      DatumReader reader = helper.getTargetSchema() == null ? new GenericDatumReader(helper.getSchema()) : new GenericDatumReader(helper.getSchema(), helper.getTargetSchema());
      InputStream inStream = new ByteArrayInputStream(payload);
      JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(helper.getSchema(), inStream);
      datum = (GenericRecord) reader.read(datum, jsonDecoder);
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      GenericDatumWriter writer = helper.getTargetSchema() == null ? new GenericDatumWriter(helper.getSchema()) : new GenericDatumWriter(helper.getTargetSchema());
      Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
      writer.write(datum, encoder);
      encoder.flush();
      output.close();
      DatumReader avroReader = helper.getTargetSchema() == null ? new GenericDatumReader(helper.getSchema()) : new GenericDatumReader(helper.getTargetSchema());
      return new KafkaAvroMessageDecoder.CamusAvroWrapper((GenericData.Record) avroReader.read(null, this.decoderFactory.binaryDecoder(output.toByteArray(), 0, output.toByteArray().length, null)));
    } catch (RuntimeException e) {
      log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new MessageDecoderException(e);
    }
  }

  public class MessageDecoderHelper {
    private Schema                       schema;
    private Schema                       targetSchema;
    private final SchemaRegistry<Schema> registry;
    private final String                 topicName;

    public MessageDecoderHelper(SchemaRegistry<Schema> registry, String topicName) {
      this.registry = registry;
      this.topicName = topicName;
    }

    public Schema getSchema() {
      return this.schema;
    }

    public Schema getTargetSchema() {
      return this.targetSchema;
    }

    public MessageDecoderHelper invoke(String id) {
      this.schema = (this.registry.getSchemaByID(this.topicName, id));
      if (this.schema == null)
        throw new IllegalStateException("Unknown schema id: " + id);
      this.targetSchema = JSONToAvroMessageDecoder.this.latestSchema;
      return this;
    }
  }
}
