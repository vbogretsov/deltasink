use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use arrow::datatypes::{Schema as ArrowSchema, *};
use chrono::Local;

macro_rules! tz_offset {
    () => {
        Arc::from(format!("{:?}", Local::now().offset()))
    };
}


pub fn convert_schema(src: &AvroSchema) -> ArrowSchema {
    match src {
        AvroSchema::Record(record_schema) => {
            let arrow_fields: Vec<Field> = record_schema
                .fields
                .iter()
                .map(|field| Field::new(
                    field.name.clone(),
                    convert_to_datatype(&field.schema),
                    is_nullable(&field.schema),
                ))
                .collect();
            Schema::new(arrow_fields)
        }
        _ => panic!("unsupported avro schema root type: expected a record"),
    }
}

fn convert_to_datatype(src: &AvroSchema) -> DataType {
    match src {
        AvroSchema::Null => DataType::Null,
        AvroSchema::Boolean => DataType::Boolean,
        AvroSchema::Int => DataType::Int32,
        AvroSchema::Long => DataType::Int64,
        AvroSchema::Float => DataType::Float32,
        AvroSchema::Double => DataType::Float64,
        AvroSchema::Bytes => DataType::Binary,
        AvroSchema::String => DataType::Utf8,
        AvroSchema::Uuid => DataType::FixedSizeBinary(16),
        AvroSchema::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
        AvroSchema::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        AvroSchema::TimestampMillis => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        AvroSchema::TimestampMicros => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        AvroSchema::TimestampNanos => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        AvroSchema::LocalTimestampMillis => {
            DataType::Timestamp(TimeUnit::Millisecond, Some(tz_offset!()))
        }
        AvroSchema::LocalTimestampMicros => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz_offset!()))
        }
        AvroSchema::LocalTimestampNanos => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz_offset!()))
        }
        AvroSchema::Date => DataType::Date32,
        AvroSchema::Duration => DataType::Duration(TimeUnit::Millisecond),
        AvroSchema::Enum { .. } => DataType::Utf8,
        AvroSchema::Fixed(schema) => DataType::FixedSizeBinary(schema.size as i32),
        AvroSchema::Decimal(schema) => {
            DataType::Decimal128(schema.precision as u8, schema.scale as i8)
        }
        AvroSchema::BigDecimal => DataType::Binary,
        AvroSchema::Array(schema) => {
            DataType::List(Arc::new(Field::new(
                "item",
                convert_to_datatype(&schema.items),
                is_nullable(&schema.items),
            )))
        }
        AvroSchema::Union(schema) => {
            let type_ids: Vec<i8> = schema
                .variants()
                .iter()
                .enumerate()
                .map(|(index, _)| index as i8)
                .collect();

            let fields: Vec<FieldRef> = schema
                .variants()
                .iter()
                .enumerate()
                .filter(|(_, variant)| **variant != AvroSchema::Null)
                .map(|(index, variant)| {
                    Arc::new(Field::new(
                        index.to_string(),
                        convert_to_datatype(variant),
                        is_nullable(&variant),
                    ))
                })
                .collect();

            if fields.len() == 1 {
                return fields[0].data_type().clone();
            }

            DataType::Union(
                UnionFields::new(type_ids, fields),
                UnionMode::Dense,
            )
        }
        AvroSchema::Map(schema) => {
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(convert_to_datatype(&schema.types)),
            )
        }
        AvroSchema::Record(schema) => {
            let arrow_fields: Fields = schema
                .fields
                .iter()
                .map(|field| Arc::new(Field::new(
                    field.name.clone(),
                    convert_to_datatype(&field.schema),
                    is_nullable(&field.schema),
                )))
                .collect();

            DataType::Struct(arrow_fields)
        }
        _ => panic!("unsupported avro schema {:?}", src),
    }

}

fn is_nullable(src: &AvroSchema) -> bool {
    if let AvroSchema::Union(schemas) = src {
        schemas
            .variants()
            .iter()
            .any(|s| matches!(s, AvroSchema::Null))
    } else {
        false
    }
}
