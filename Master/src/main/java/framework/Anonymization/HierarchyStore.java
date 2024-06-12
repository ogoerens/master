package framework.Anonymization;

import org.deidentifier.arx.AttributeType;
import org.deidentifier.arx.aggregates.HierarchyBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class HierarchyStore {
  private HashMap<String, AttributeType.Hierarchy> hierarchies;
  private HashMap<String, HierarchyBuilder> hierarchyBuilders;
  private HashMap<String, Integer> map;

  public HierarchyStore() {
    this.hierarchies = new HashMap<String, AttributeType.Hierarchy>();
    this.hierarchyBuilders = new HashMap<String, HierarchyBuilder>();
    map = new HashMap<>();
  }

  public HierarchyStore(
      HashMap<String, AttributeType.Hierarchy> hierarchies,
      HashMap<String, HierarchyBuilder> hierarchyBuilders) {
    this.hierarchies = hierarchies;
    this.hierarchyBuilders = hierarchyBuilders;
    map = new HashMap<>();
    for (Map.Entry<String, AttributeType.Hierarchy> entry : hierarchies.entrySet()) {
      map.put(entry.getKey(), 0);
    }
    for (Map.Entry<String, HierarchyBuilder> entry : hierarchyBuilders.entrySet()) {
      map.put(entry.getKey(), 1);
    }
  }

  /**
   * Returns true if the HierarchyStore contains either a hierarchy or hierarchyBuilder for the
   * given name.
   *
   * @param name
   * @return
   */
  public Boolean contains(String name) {
    return map.containsKey(name);
  }

  public HashMap<String, AttributeType.Hierarchy> getHierarchies() {
    return hierarchies;
  }

  public HashMap<String, HierarchyBuilder> getHierarchyBuilders() {
    return hierarchyBuilders;
  }

  /**
   * Returns an index indicating whether there is a Hierarchy or a HierarchyBuilder stored for the
   * given key.
   *
   * @param key
   * @return 0 stands for Hierarchy. 1 stands for HierarchyBuilder.
   */
  public Integer getIndexForColumnName(String key) {
    return map.get(key);
  }

  public ArrayList<String> getColumnNames() {
    ArrayList<String> result = new ArrayList<>();
    for (Map.Entry e : map.entrySet()) {
      result.add(e.getKey().toString());
    }
    return result;
  }
}
