package com.dataplatform.processor.consumers.tranformers;

import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.utils.JsonUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class StoredDataJsonSerdeTest {

  private final JsonSerde<StoredData> serde = new JsonSerde<>(StoredData.class);

  @Test
  void roundTripWithChildItems() throws Exception {
    var original = new StoredData(
        "global", "api_events", "s3://bucket/data/file.parquet", 42, 8192L,
        List.of("s3://bucket/data/a.parquet", "s3://bucket/data/b.parquet"));
    byte[] bytes = serde.serializer().serialize("dp-merge-data-events", original);
    StoredData restored = serde.deserializer().deserialize("dp-merge-data-events", bytes);
    assertEquals(original, restored);
  }

  @Test
  void deserializeFromJsonWithNullChildItems() throws Exception {
    String json = """
        {"namespace":"global","type":"api_events","location":"s3://bucket/data/x.parquet","totalRecords":10,"length":100,"childItems":null}
        """;
    StoredData restored = JsonUtils.getMapper().readValue(json, StoredData.class);
    assertNotNull(restored);
    assertEquals("global", restored.namespace());
    assertEquals(10, restored.totalRecords());
    assertNull(restored.childItems());
  }

  @Test
  void deserializeTombstoneAsNull() {
    assertNull(serde.deserializer().deserialize("dp-merge-data-events", null));
    assertNull(serde.deserializer().deserialize("dp-merge-data-events", new byte[0]));
  }
}
