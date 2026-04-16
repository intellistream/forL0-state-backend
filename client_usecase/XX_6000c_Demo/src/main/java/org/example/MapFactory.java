package org.example;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import java.lang.reflect.Type;
import java.util.Map;

public class MapFactory<K, V> extends TypeInfoFactory<Map<K, V>> {

    @Override
    public TypeInformation<Map<K, V>> createTypeInfo(Type type, Map<String, TypeInformation<?>> map) {
        TypeInformation<?> keyType = map.get("K");
        TypeInformation<?> valueType = map.get("V");
        return new MapTypeInfo<>((TypeInformation<K>)keyType, (TypeInformation<V>) valueType);
    }
}