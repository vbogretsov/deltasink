use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::Name;
use arrow::datatypes::*;
use pretty_assertions::assert_eq;

use avroarrow;
use registry;

macro_rules! tz_offset {
    () => {
        std::sync::Arc::from(format!("{:?}", chrono::Local::now().offset()))
    };
}

macro_rules! array_of {
    ($ty:expr, $nullable:expr) => {
        DataType::List(Arc::new(Field::new("item", $ty, $nullable)))
    };
}

macro_rules! map_of {
    ($ty:expr) => {
        DataType::Dictionary(Box::new(DataType::Utf8), Box::new($ty))
    };
}

#[test]
fn test_convert_schema_flat() {
    let avro_schema_str = std::fs::read_to_string("./tests/data/example.avsc")
        .unwrap();
    let avro_schema = AvroSchema::parse_str(&avro_schema_str)
        .unwrap();

    let expected = Schema::new(vec![
        Field::new("f_string", DataType::Utf8, false),
        Field::new("f_opt_string", DataType::Utf8, true),
        Field::new("f_bool", DataType::Boolean, false),
        Field::new("f_int", DataType::Int32, false),
        Field::new("f_long", DataType::Int64, false),
        Field::new("f_float", DataType::Float32, false),
        Field::new("f_double", DataType::Float64, false),
        Field::new("f_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("f_dec128", DataType::Decimal128(38, 8), false),
        Field::new("f_opt_date", DataType::Date32, true),
        Field::new("f_duration", DataType::Duration(TimeUnit::Millisecond), false),
        Field::new("f_bytes", DataType::Binary, false),
        Field::new("f_opt_fixed", DataType::FixedSizeBinary(32), true),
        Field::new("f_time_ms", DataType::Time32(TimeUnit::Millisecond), false),
        Field::new("f_time_mc", DataType::Time64(TimeUnit::Microsecond), false),
        Field::new("f_timestamp_ms", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("f_timestamp_mc", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("f_loc_timestamp_ms", DataType::Timestamp(TimeUnit::Millisecond, Some(tz_offset!())), false),
        Field::new("f_loc_timestamp_mc", DataType::Timestamp(TimeUnit::Microsecond, Some(tz_offset!())), true),
        Field::new("f_array", array_of!(DataType::Utf8, true), false),
        Field::new("f_map", map_of!(DataType::Int64), false),
    ]);

    let actual = avroarrow::convert_schema(&avro_schema);

    assert_eq!(expected, actual);
}

#[test]
fn test_convert_schema_nested() {
    let avro_schemas_str = vec![
        std::fs::read_to_string("./tests/data/contact.avsc").unwrap(),
        std::fs::read_to_string("./tests/data/address.avsc").unwrap(),
        std::fs::read_to_string("./tests/data/person.avsc").unwrap(),
    ];

    let avro_schemas_ref: Vec<&str> = avro_schemas_str
        .iter()
        .map(|s| s.as_str())
        .collect();

    let mut schemas: HashMap<Name, AvroSchema> = HashMap::new();

    let avro_schemas = AvroSchema::parse_list(&avro_schemas_ref).unwrap();
    for avro_schema in avro_schemas.iter() {
        registry::register_schema(&avro_schema, &mut schemas).unwrap();
    }

    let person_schema = registry::expand_schema(&avro_schemas[2], &schemas).unwrap();

    let arrow_contact_schema = DataType::Struct(vec![
        Field::new("email", DataType::Utf8, false),
        Field::new("phone", DataType::Utf8, true),
    ].into());

    let arrow_address_schema = DataType::Struct(vec![
        Field::new("country", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("street", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, true),
        Field::new("zipcode", DataType::Utf8, true),
    ].into());

    let arrow_job_schema = DataType::Struct(vec![
        Field::new("company", DataType::Utf8, false),
        Field::new("position", DataType::Utf8, false),
    ].into());

    let expected = Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("birthdate", DataType::Date32, true),
        Field::new("contacts", array_of!(arrow_contact_schema, true), false),
        Field::new("addresses", map_of!(arrow_address_schema), false),
        Field::new("job", arrow_job_schema, false),
    ]);

    let actual = avroarrow::convert_schema(&person_schema);

    assert_eq!(expected, actual);
}
