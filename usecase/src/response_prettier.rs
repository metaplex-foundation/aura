use serde_json::Value;

pub fn filter_non_null_fields(value: Option<&Value>) -> Option<Value> {
    match value {
        Some(Value::Null) => None,
        Some(Value::Object(map)) => {
            if map.values().all(|v| matches!(v, Value::Null)) {
                None
            } else {
                let filtered_map: serde_json::Map<String, Value> = map
                    .into_iter()
                    .filter(|(_k, v)| !matches!(v, Value::Null))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if filtered_map.is_empty() {
                    None
                } else {
                    Some(Value::Object(filtered_map))
                }
            }
        },
        _ => value.cloned(),
    }
}
