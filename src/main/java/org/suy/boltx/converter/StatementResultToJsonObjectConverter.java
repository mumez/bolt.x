package org.suy.boltx.converter;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import java.util.List;


/**
 * Created by Masashi on 2017/06/12.
 */
public class StatementResultToJsonObjectConverter {
  public JsonObject convert(StatementResult result){
    List<Record> records = result.list();
    JsonArray retArr = new JsonArray();
    RecordToJsonObjectConverter converter = new RecordToJsonObjectConverter();
    records.forEach(rec -> retArr.add(converter.convert(rec)));
    JsonObject ret = new JsonObject();
    ret.put("data", retArr);
    return ret;
  }

}
