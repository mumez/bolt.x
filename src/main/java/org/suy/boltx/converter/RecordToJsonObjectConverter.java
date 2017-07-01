package org.suy.boltx.converter;

import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.Record;

import java.util.Map;


/**
 * Created by Masashi on 2017/06/12.
 */
public class RecordToJsonObjectConverter {
  public JsonObject convert(Record record){
    ValueToJsonSerializableConverter valueConverter = new ValueToJsonSerializableConverter();
    Map<String, Object> map = record.asMap(valueConverter::convert);
    return new JsonObject(map);
  }

}
