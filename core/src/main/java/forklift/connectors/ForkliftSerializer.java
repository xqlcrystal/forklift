package forklift.connectors;

import forklift.source.SourceI;

/**
 * An interface that encapsulates the logic for serializing and deserializing objects,
 * potentially making a distinction between serializing to different sources.
 */
public interface ForkliftSerializer {
    /**
     * Serialize the given object to bytes.
     *
     * @param source the source for which the serialized bytes are destined
     * @param object the object to serialize
     * @return the byte serialization of the given object
     */
    public byte[] serializeForSource(SourceI source, Object object);

    /**
     * Deserialize the given object from bytes.
     *
     * @param <T> the type expected from the deserialized object
     * @param source the source for which the bytes were destined
     * @param bytes the bytes to deserialize
     * @return the bytes deserialized into an object
     */
    public <T> T deserializeForSource(SourceI source, byte[] bytes);
}
