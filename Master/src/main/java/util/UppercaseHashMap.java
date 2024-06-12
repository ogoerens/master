package util;

import java.util.HashMap;

public class UppercaseHashMap<T> extends HashMap<String,T> {
    @Override
    public T put(String key, T value){
        return super.put(key.toUpperCase(),value);
    }
}
