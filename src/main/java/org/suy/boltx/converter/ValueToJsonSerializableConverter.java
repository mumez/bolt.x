package org.suy.boltx.converter;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Masashi on 2017/06/12.
 */
public class ValueToJsonSerializableConverter {
  public Object convert(Value value){
    Object valObj = value.asObject();
    return convertObject(valObj);
  }

  @SuppressWarnings("unchecked")
  public Object convertObject(Object orig){
    if(orig == null){return null;}
    if(orig instanceof Node){return convertNode((Node)orig);}
    if(orig instanceof Relationship){return convertRelationship((Relationship)orig);}
    if(orig instanceof Path){return convertPath((Path)orig);}
    if(orig instanceof Map){return convertMap((Map<String,Object>)orig);}
    if(orig instanceof List){return convertList((List)orig);}
    return orig;
  }

  public Map<String, Object> convertNode(Node node){
    Map<String, Object> map = new HashMap<>();
    map.put("__id", node.id());
    map.put("__labels", node.labels());
    Map<String, Object> props = new HashMap<>();
    for(String key: node.keys()){
      props.put(key, convert(node.get(key)));
    }
    map.put("__props", props);
    return map;
  }

  public Map<String, Object> convertRelationship(Relationship rel){
    Map<String, Object> map = new HashMap<>();
    map.put("__id", rel.id());
    map.put("__type", rel.type());
    Map<String, Object> props = new HashMap<>();
    for(String key: rel.keys()){
      props.put(key, convert(rel.get(key)));
    }
    map.put("__props", props);
    return map;
  }

  public List<Object> convertPath(Path path){
    List<Object> list = new ArrayList<>();
    path.forEach(segment -> list.add(convertSegment(segment)));
    return list;
  }

  public Map<String, Object> convertSegment(Path.Segment segment){
    Map<String, Object> map = new HashMap<>();
    map.put("__start", convertNode(segment.start()));
    map.put("__end", convertNode(segment.end()));
    map.put("__relationship", convertRelationship(segment.relationship()));
    return map;
  }

  public List<Object> convertList(List list){
    List<Object> converted = new ArrayList<>();
    for(Object elem : list){
      converted.add(convertObject(elem));
    }
    return converted;
  }

  public Map<String, Object> convertMap(Map<String, Object> map){
    Map<String, Object> converted = new HashMap<>();
    map.forEach((key, value)-> converted.put(key, convertObject(value)));
    return converted;
  }
}
