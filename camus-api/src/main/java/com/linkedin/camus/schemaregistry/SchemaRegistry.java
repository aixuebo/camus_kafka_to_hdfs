package com.linkedin.camus.schemaregistry;

import java.util.Properties;

/**
 * The schema registry is used to read and write schemas for Kafka topics. This
 * is useful because it means you no longer have to store your schema with your
 * message payload. Instead, you can store a schema id with the message, and use
 * a schema registry to look up the message's schema when you wish to decode it.
 * In essence, a schema registry is just a client-side interface for a versioned
 * key-value store that's meant to store schemas.
 * 
 * @param <S>
 *            A schema type.
 * 为每一个topic注册一个schema，泛型S是具体的schema对象
 */
public interface SchemaRegistry<S> {
    
    /**
     * Initializer for SchemaRegistry;
     * 
     * @param props
     *            Java properties
     */
    public void init(Properties props);

	/**
	 * Store a schema in the registry. If a schema already exists for this
	 * topic, the old schema will not be over-written. Instead, the new schema
	 * will be stored with a different id.
	 * 
	 * @param topic
	 *            A topic name.
	 * @param schema
	 *            A schema.
	 * @return A schema id. This id is implementation-specific. If the write
	 *         fails, this method will throw an unchecked
	 *         SchemaRegistryException.
	 * 注册一个topic与schema映射
	 */
	public String register(String topic, S schema);

	/**
	 * Get a schema for a given topic/id pair, regardless of whether the schema
	 * was the last one written for this topic.
	 * 
	 * @param topic
	 * @param id
	 * @return A schema. If not schema exists, an unchecked
	 *         SchemaNotFoundException will be thrown.
	 * 还原某一个版本的topic的schema信息
	 */
	public S getSchemaByID(String topic, String id);

	/**
	 * Get the last schema that was written for a specific topic.
	 * 
	 * @param topic
	 *            A topic name.
	 * @return A class that contains the topic name, schema id, and schema. If
	 *         not schema exists, an unchecked SchemaNotFoundException will be
	 *         thrown.
	 * 获取最新的schema信息
	 */
	public SchemaDetails<S> getLatestSchemaByTopic(String topic);
}