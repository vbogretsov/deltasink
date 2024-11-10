use std::sync::Arc;

use apache_avro::schema::{ArraySchema, DecimalSchema, EnumSchema, FixedSchema, MapSchema, Schema};
use apache_avro::Decimal;
use apache_avro::AvroSchema;
use apache_avro::types::Value;
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use chrono::Local;
use pretty_assertions::assert_eq;
use serde::Serialize;
use maplit::hashmap;
use num_bigint::ToBigInt;
use uuid;

use avroarrow;

fn as_sorted_string(array: ArrayRef) -> Vec<Option<String>> {
    let mut slice = array
        .as_ref()
        .as_string::<i32>()
        .clone()
        .iter()
        .map(|i| {
            match i {
                Some(s) => Some(s.to_string()),
                None => None,
            }
        })
        .collect::<Vec<_>>();
    slice.sort();

    slice
}

fn tz_offset() -> Option<Arc<str>> {
    Some(Arc::from(format!("{:?}", Local::now().offset())))
}

fn to_array<T, B: Array + From<Vec<T>> + 'static>(row: Vec<T>) -> ArrayRef {
    Arc::from(B::from(row))
}

fn to_arrow<T: ArrayBuilder>(
    schema: &Schema,
    values: &Vec<Value>,
) -> ArrayRef {
    let mut builder = avroarrow::create_builder(&schema, 32).unwrap();
    for v in values {
        avroarrow::append_record(&mut builder, &schema, &v).unwrap();
    }

    builder
        .as_any_mut()
        .downcast_mut::<T>()
        .unwrap()
        .finish()
}

#[test]
fn test_append_bool() {
    let row = vec![true, false, true];

    let schema = Schema::Boolean;
    let values: Vec<_> = row.iter().map(|v| Value::Boolean(*v)).collect();

    let expected = to_array::<bool, BooleanArray>(row);
    let actual = to_arrow::<BooleanBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_int() {
    let row = vec![3, 4, 5];

    let schema = Schema::Int;
    let values: Vec<_> = row.iter().map(|v| Value::Int(*v)).collect();

    let expected = to_array::<i32, Int32Array>(row);
    let actual = to_arrow::<Int32Builder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_long() {
    let row: Vec<i64> = vec![3, 4, 5];

    let schema = Schema::Long;
    let values: Vec<_> = row.iter().map(|v| Value::Long(*v)).collect();

    let expected = to_array::<i64, Int64Array>(row);
    let actual = to_arrow::<Int64Builder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_float() {
    let row: Vec<f32> = vec![3.1, 4.2, 5.3];

    let schema = Schema::Float;
    let values: Vec<_> = row.iter().map(|v| Value::Float(*v)).collect();

    let expected = to_array::<f32, Float32Array>(row);
    let actual = to_arrow::<Float32Builder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_double() {
    let row: Vec<f64> = vec![3.1, 4.2, 5.3];

    let schema = Schema::Double;
    let values: Vec<_> = row.iter().map(|v| Value::Double(*v)).collect();

    let expected = to_array::<f64, Float64Array>(row);
    let actual = to_arrow::<Float64Builder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_decimal() {
    let raw: Vec<i128> = vec![
        1024,
        -2048,
        4096,
    ];

    let schema = Schema::Decimal(DecimalSchema{
        precision: 10,
        scale: 4,
        inner: Box::new(Schema::Bytes),
    });

    let values: Vec<_> = raw
        .iter()
        .map(|v| {
            let b = v.to_bigint().unwrap().to_signed_bytes_be();
            Value::Decimal(Decimal::from(b))
        })
        .collect();

    let expected: ArrayRef = Arc::new(
        Decimal128Array::from(raw).with_data_type(DataType::Decimal128(10, 4)),
    );

    let actual = to_arrow::<Decimal128Builder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_date() {
    let raw: Vec<i32> = vec![
        19938, // 10.11.2024
        19937, // 09.11.2024
        19936, // 08.11.2024
    ];

    let schema = Schema::Date;
    let values: Vec<_> = raw.iter().map(|v| Value::Date(*v)).collect();

    let expected = to_array::<i32, Date32Array>(raw);
    let actual = to_arrow::<Date32Builder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_time_ms() {
    let raw: Vec<i32> = vec![42000000, 52000000, 62000000];

    let schema = Schema::TimeMillis;
    let values: Vec<_> = raw.iter().map(|v| Value::TimeMillis(*v)).collect();

    let expected = to_array::<i32, Time32MillisecondArray>(raw);
    let actual = to_arrow::<Time32MillisecondBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_time_mc() {
    let raw: Vec<i64> = vec![42000000, 52000000, 62000000];

    let schema = Schema::TimeMicros;
    let values: Vec<_> = raw.iter().map(|v| Value::TimeMicros(*v)).collect();

    let expected = to_array::<i64, Time64MicrosecondArray>(raw);
    let actual = to_arrow::<Time64MicrosecondBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_timestamp_ms() {
    let raw: Vec<i64> = vec![
        1709821534123,
        1709821533123,
        1709821532123,
    ];

    let schema = Schema::TimestampMillis;
    let values: Vec<_> = raw.iter().map(|v| Value::TimestampMillis(*v)).collect();

    let expected = to_array::<i64, TimestampMillisecondArray>(raw);
    let actual = to_arrow::<TimestampMillisecondBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_timestamp_mc() {
    let raw: Vec<i64> = vec![
        1709821534123123,
        1709821533123456,
        1709821532123789,
    ];

    let schema = Schema::TimestampMicros;
    let values: Vec<_> = raw.iter().map(|v| Value::TimestampMicros(*v)).collect();

    let expected = to_array::<i64, TimestampMicrosecondArray>(raw);
    let actual = to_arrow::<TimestampMicrosecondBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_timestamp_ns() {
    let row: Vec<i64> = vec![
        1699539650112345678,
        1699539650112335678,
        1699539650112325678,
    ];

    let schema = Schema::TimestampNanos;
    let values: Vec<_> = row.iter().map(|v| Value::TimestampNanos(*v)).collect();

    let expected = to_array::<i64, TimestampNanosecondArray>(row);
    let actual = to_arrow::<TimestampNanosecondBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_local_timestamp_ms() {
    let raw: Vec<i64> = vec![
        1709821534123,
        1709821533123,
        1709821532123,
    ];

    let schema = Schema::LocalTimestampMillis;
    let values: Vec<_> = raw.iter().map(|v| Value::LocalTimestampMillis(*v)).collect();

    let expected: ArrayRef = Arc::new(
        TimestampMillisecondArray::from(raw).with_data_type(
            DataType::Timestamp(TimeUnit::Millisecond, tz_offset()),
        ),
    );
    let actual = to_arrow::<TimestampMillisecondBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_local_timestamp_mc() {
    let raw: Vec<i64> = vec![
        1709821534123123,
        1709821533123456,
        1709821532123789,
    ];

    let schema = Schema::LocalTimestampMicros;
    let values: Vec<_> = raw.iter().map(|v| Value::LocalTimestampMicros(*v)).collect();

    let expected: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(raw).with_data_type(
            DataType::Timestamp(TimeUnit::Microsecond, tz_offset()),
        ),
    );
    let actual = to_arrow::<TimestampMicrosecondBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_local_timestamp_ns() {
    let raw: Vec<i64> = vec![
        1699539650112345678,
        1699539650112335678,
        1699539650112325678,
    ];

    let schema = Schema::LocalTimestampNanos;
    let values: Vec<_> = raw.iter().map(|v| Value::LocalTimestampNanos(*v)).collect();

    let expected: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(raw).with_data_type(
            DataType::Timestamp(TimeUnit::Nanosecond, tz_offset()),
        ),
    );
    let actual = to_arrow::<TimestampNanosecondBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_string() {
    let row: Vec<String> = vec![
        "a-3.1".to_string(),
        "b-4.2".to_string(),
        "c-5.3".to_string(),
    ];

    let schema = Schema::String;
    let values: Vec<_> = row.iter().map(|v| Value::String(v.clone())).collect();

    let expected = to_array::<String, StringArray>(row);
    let actual = to_arrow::<StringBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_enum() {
    let raw = vec![
        "X1".to_string(),
        "X2".to_string(),
        "X3".to_string(),
    ];
    let schema = Schema::Enum(EnumSchema{
        name: apache_avro::schema::Name {
            name: "e1".to_string(),
            namespace: None,
        },
        doc: None,
        aliases: None,
        symbols: raw.clone(),
        default: Some(raw[0].clone()),
        attributes: Default::default(),
    });
    let values: Vec<_> = raw
        .iter()
        .enumerate()
        .map(|(i, v)| Value::Enum(i as u32, v.clone()))
        .collect();

    let expected = to_array::<String, StringArray>(raw);
    let actual = to_arrow::<StringBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_uuid() {
    let raw: Vec<_> = vec![
        "cbb297c0-14a9-46bc-ad91-1d0ef9b42df9",
        "465a78ad-93cc-432e-a836-9824d49506d6",
        "0cd4a0d3-2e41-4b51-945a-eb06adbe8d1e",
    ];

    let schema = Schema::Uuid;
    let values: Vec<_> = raw.iter().map(|v| {
        Value::Uuid(uuid::Uuid::parse_str(*v).unwrap())
    }).collect();

    let bin = raw
        .iter()
        .map(|v| {
            Some(uuid::Uuid::parse_str(v).unwrap().as_bytes().clone())
        })
        .collect::<Vec<Option<_>>>();

    let expected: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            bin.into_iter(),
            16,
        ).unwrap()
    );
    let actual = to_arrow::<FixedSizeBinaryBuilder>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_bytes() {
    let raw: Vec<&[u8]> = vec![
        b"one",
        b"tow",
        b"",
        b"three",
    ];

    let schema = Schema::Bytes;
    let values: Vec<Value> = raw
        .iter()
        .map(|v| Value::Bytes(v.to_vec()))
        .collect();

    let expected: ArrayRef = Arc::new(
        BinaryArray::from_vec(raw)
    );
    let actual = to_arrow::<BinaryBuilder>(&schema, &values);

    assert_eq!(*expected, *actual)
}

#[test]
fn test_append_fixed() {
    let raw: Vec<&[u8; 2]> = vec![
        b"12",
        b"23",
        b"56",
    ];

    let schema = Schema::Fixed(FixedSchema{
        name: apache_avro::schema::Name {
            name: "test-1".to_string(),
            namespace: None,
        },
        doc: None,
        size: 2,
        aliases: None,
        default: None,
        attributes: Default::default(),
    });

    let values: Vec<Value> = raw
        .iter()
        .map(|v| Value::Fixed(2, v.to_vec()))
        .collect();

    let bin = raw
        .iter()
        .map(|v| Some(v))
        .collect::<Vec<Option<_>>>();

    let expected: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            bin.into_iter(),
            2,
        ).unwrap()
    );
    let actual = to_arrow::<FixedSizeBinaryBuilder>(&schema, &values);

    assert_eq!(*expected, *actual)
}

#[test]
fn test_append_string_array() {
    let raw = vec![
        vec!["x1", "x2"],
        vec!["y1", "y2", "y3"],
        vec!["z1", "z2", "z3", "z4"],
    ];

    let schema = Schema::Array(ArraySchema{
        items: Box::new(Schema::String),
        attributes: Default::default(),
    });

    let values: Vec<Value> = raw
        .iter()
        .map(|v| Value::Array(v.iter().map(|i| Value::String(i.to_string())).collect()))
        .collect();

    let mut expected_builder = ListBuilder::new(StringBuilder::with_capacity(32, 32));
    for i in raw {
        for j in i {
            expected_builder.values().append_value(j);
        }
        expected_builder.append(true);
    }

    let expected: ArrayRef = Arc::from(expected_builder.finish());
    let actual = to_arrow::<ListBuilder<Box<dyn ArrayBuilder>>>(&schema, &values);

    assert_eq!(*expected, *actual);
}

#[test]
fn test_append_string_map() {
    let raw = vec![
        hashmap! {
            "x1" => "a",
            "x2" => "b",
        },
        hashmap! {
            "y1" => "A",
            "y2" => "B",
            "y3" => "C",
        },
        hashmap! {
            "z1" => "1",
        },
    ];

    let schema = Schema::Map(MapSchema{
        types: Box::new(Schema::String),
        attributes: Default::default(),
    });

    let values: Vec<Value> = raw
        .iter()
        .map(|i| Value::Map(
            i
                .iter()
                .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
                .collect()
            )
        )
        .collect();

    let actual = to_arrow::<MapBuilder<StringBuilder, Box<dyn ArrayBuilder>>>(
        &schema,
        &values,
    );

    let actual_map = actual.as_map();

    assert_eq!(actual_map.value_offsets(), &[0, 2, 5, 6]);
    assert_eq!(
        as_sorted_string(actual_map.keys().slice(0, 2)),
        vec![Some("x1".to_string()), Some("x2".to_string())],
    );
    assert_eq!(
        as_sorted_string(actual_map.values().slice(0, 2)),
        vec![Some("a".to_string()), Some("b".to_string())],
    );
    assert_eq!(
        as_sorted_string(actual_map.keys().slice(2, 3)),
        vec![Some("y1".to_string()), Some("y2".to_string()), Some("y3".to_string())],
    );
    assert_eq!(
        as_sorted_string(actual_map.values().slice(2, 3)),
        vec![Some("A".to_string()), Some("B".to_string()), Some("C".to_string())],
    );
    assert_eq!(
        as_sorted_string(actual_map.keys().slice(5, 1)),
        vec![Some("z1".to_string())],
    );
    assert_eq!(
        as_sorted_string(actual_map.values().slice(5, 1)),
        vec![Some("1".to_string())],
    );
}

#[derive(Serialize, AvroSchema)]
struct Person {
    id: String,
    name: String,
    age: Option<i32>,
}

#[test]
fn test_append_struct() {
    let schema = Person::get_schema();
    dbg!(&schema);

    let records = vec![
        Person {
            id: "p-1".to_string(),
            name: "Job".to_string(),
            age: Some(33),
        },
        Person {
            id: "p-2".to_string(),
            name: "Don".to_string(),
            age: Some(43),
        },
        Person {
            id: "p-3".to_string(),
            name: "Rob".to_string(),
            age: None,
        },
    ];

    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for record in records {
        writer.append_ser(record).unwrap();
    }

    let stream = writer.into_inner().unwrap();
    let reader = apache_avro::Reader::with_schema(&schema, &stream[..]).unwrap();
    let values: Vec<Value> = reader.into_iter().map(|v| v.unwrap()).collect();

    let mut builder = avroarrow::create_builder(&schema, 32).unwrap();
    for value in &values {
        avroarrow::append_record(&mut builder, &schema, value).unwrap();
    }

    let actual = builder.finish();
    dbg!(actual);

    /* match schema {
        Schema::Record(record_schema) => {
            match &record_schema.fields[2].schema {
                Schema::Union(union_schema) => {
                    assert!(matches!(union_schema.variants()[0], Schema::Null));
                }
                _ => {  }
            }
        },
        _ => {  }
    } */
}
