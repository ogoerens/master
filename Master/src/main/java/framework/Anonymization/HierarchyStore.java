package framework.Anonymization;

import org.deidentifier.arx.AttributeType;
import org.deidentifier.arx.aggregates.HierarchyBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class HierarchyStore {
  public HashMap<String, AttributeType.Hierarchy> hierarchies;
  public HashMap<String, HierarchyBuilder> hierarchyBuilders;
  private HashMap<String, Integer> map;

  public HierarchyStore(){
    this.hierarchies = new HashMap<String, AttributeType.Hierarchy>();
    this.hierarchyBuilders = new HashMap<String, HierarchyBuilder>();
    map = new HashMap<>();
  }

  public HierarchyStore(HashMap<String, AttributeType.Hierarchy> hierarchies,HashMap<String, HierarchyBuilder> hierarchyBuilders){
    this.hierarchies = hierarchies;
    this.hierarchyBuilders = hierarchyBuilders;
    map = new HashMap<>();
    for (Map.Entry<String, AttributeType.Hierarchy> entry: hierarchies.entrySet()){
      map.put(entry.getKey(), 0);
    }
    for (Map.Entry<String, HierarchyBuilder> entry: hierarchyBuilders.entrySet()){
      map.put(entry.getKey(), 1);
    }
  }

  public Integer getIndexForColumnName (String key){
    return map.get(key);
  }

  public ArrayList<String> getColumnNames(){
    ArrayList<String> result = new ArrayList<>();
    for(Map.Entry e : map.entrySet()){
      result.add(e.getKey().toString());
    }
    return result;
  }
}
