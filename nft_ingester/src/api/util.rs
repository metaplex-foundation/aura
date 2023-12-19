/// Macro which takes struct and returns a string with names of all the fields which are Some.
/// For now it is using only for SearchAssets struct.
#[macro_export]
macro_rules! extract_some_field_names {
    ($struct_name:ident { $($field:ident),* $(,)? }) => {
        pub fn extract_some_fields(instance: &$struct_name) -> String {
            let mut result = String::new();

            $(
                if instance.$field.is_some() {
                    result = format!("{}{}_", result, stringify!($field));
                }
            )*

            result[..result.len()-1].to_string()
        }
    };
}
