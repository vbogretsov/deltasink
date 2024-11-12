use std::error::Error;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{ArraySchema, DecimalSchema, MapSchema, RecordSchema, UnionSchema};
use arrow::datatypes::{Schema as ArrowSchema, *};
use arrow::array::builder::*;
use chrono::Local;

macro_rules! tz_offset {
    () => {
        Arc::from(format!("{:?}", Local::now().offset()))
    };
}

pub fn convert_schema(src: &AvroSchema) -> Result<ArrowSchema, Box<dyn Error>> {
    match src {
        AvroSchema::Record(record_schema) => {
            let arrow_fields: Result<Vec<Field>, Box<dyn Error>> = record_schema
                .fields
                .iter()
                .map(|field| Ok(Field::new(
                    field.name.clone(),
                    convert_to_datatype(&field.schema)?,
                    is_nullable(&field.schema),
                )))
                .collect();
            Ok(Schema::new(arrow_fields?))
        }
        _ => panic!("unsupported avro schema root type: expected a record"),
    }
}

fn convert_to_datatype(src: &AvroSchema) -> Result<DataType, Box<dyn Error>> {
    match src {
        AvroSchema::Null => Ok(DataType::Null),
        AvroSchema::Boolean => Ok(DataType::Boolean),
        AvroSchema::Int => Ok(DataType::Int32),
        AvroSchema::Long => Ok(DataType::Int64),
        AvroSchema::Float => Ok(DataType::Float32),
        AvroSchema::Double => Ok(DataType::Float64),
        AvroSchema::Bytes => Ok(DataType::Binary),
        AvroSchema::String => Ok(DataType::Utf8),
        AvroSchema::Uuid => Ok(DataType::FixedSizeBinary(16)),
        AvroSchema::TimeMillis => Ok(DataType::Time32(TimeUnit::Millisecond)),
        AvroSchema::TimeMicros => Ok(DataType::Time64(TimeUnit::Microsecond)),
        AvroSchema::TimestampMillis => {
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        AvroSchema::TimestampMicros => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        AvroSchema::TimestampNanos => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        AvroSchema::LocalTimestampMillis => {
            Ok(DataType::Timestamp(TimeUnit::Millisecond, Some(tz_offset!())))
        }
        AvroSchema::LocalTimestampMicros => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, Some(tz_offset!())))
        }
        AvroSchema::LocalTimestampNanos => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, Some(tz_offset!())))
        }
        AvroSchema::Date => Ok(DataType::Date32),
        AvroSchema::Enum { .. } => Ok(DataType::Utf8),
        AvroSchema::Decimal(schema) => {
            Ok(DataType::Decimal128(schema.precision as u8, schema.scale as i8))
        }
        AvroSchema::Fixed(schema) => Ok(DataType::FixedSizeBinary(schema.size as i32)),
        AvroSchema::BigDecimal => Ok(DataType::Binary),
        AvroSchema::Array(schema) => {
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                convert_to_datatype(&schema.items)?,
                is_nullable(&schema.items),
            ))))
        }
        AvroSchema::Map(schema) => {
            let entries = Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new(
                        "values",
                        convert_to_datatype(&schema.types)?,
                        is_nullable(&schema.types),
                    ),
                ].into()),
                false,
            );
            Ok(DataType::Map(entries.into(), false))
        }
        AvroSchema::Union(schema) => {
            if is_optional(schema) {
                convert_to_datatype(&schema.variants()[1])
            } else {
                Err("only unions of king [null, TYPE] are supported".into())
            }
        }
        AvroSchema::Record(schema) => {
            let arrow_fields: Result<Fields, Box<dyn Error>> = schema
                .fields
                .iter()
                .map(|field| Ok(Arc::new(Field::new(
                    field.name.clone(),
                    convert_to_datatype(&field.schema)?,
                    is_nullable(&field.schema),
                ))))
                .collect();

            Ok(DataType::Struct(arrow_fields?))
        }
        _ => Err(format!("unsupported avro schema {:?}", src).into()),
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

fn is_optional(src: &UnionSchema) -> bool {
    src.variants().len() == 2 && matches!(src.variants()[0], AvroSchema::Null)
}

pub fn create_builder(
    avro: &AvroSchema,
    cap: usize,
) -> Result<Box<dyn ArrayBuilder>, Box<dyn Error>> {
    match avro {
        AvroSchema::Null => Ok(Box::new(NullBuilder::new())),
        AvroSchema::Boolean => Ok(Box::new(BooleanBuilder::with_capacity(cap))),
        AvroSchema::Int => Ok(Box::new(Int32Builder::with_capacity(cap))),
        AvroSchema::Long => Ok(Box::new(Int64Builder::with_capacity(cap))),
        AvroSchema::Float => Ok(Box::new(Float32Builder::with_capacity(cap))),
        AvroSchema::Double => Ok(Box::new(Float64Builder::with_capacity(cap))),
        AvroSchema::Decimal(schema) => Ok(Box::new(decimal_builder(schema, cap))),
        AvroSchema::Date => Ok(Box::new(Date32Builder::with_capacity(cap))),
        AvroSchema::TimeMillis => Ok(Box::new(Time32MillisecondBuilder::with_capacity(cap))),
        AvroSchema::TimeMicros => Ok(Box::new(Time64MicrosecondBuilder::with_capacity(cap))),
        AvroSchema::TimestampMillis => Ok(Box::new(TimestampMillisecondBuilder::with_capacity(cap))),
        AvroSchema::TimestampMicros => Ok(Box::new(TimestampMicrosecondBuilder::with_capacity(cap))),
        AvroSchema::TimestampNanos => Ok(Box::new(TimestampNanosecondBuilder::with_capacity(cap))),
        AvroSchema::LocalTimestampMillis => Ok(Box::new(local_timestamp_ms_builder(cap))),
        AvroSchema::LocalTimestampMicros => Ok(Box::new(local_timestamp_mc_builder(cap))),
        AvroSchema::LocalTimestampNanos => Ok(Box::new(local_timestamp_ns_builder(cap))),
        AvroSchema::Bytes => Ok(Box::new(BinaryBuilder::with_capacity(cap, 2048))),
        AvroSchema::Fixed(schema) => Ok(Box::new(FixedSizeBinaryBuilder::with_capacity(cap, schema.size as i32))),
        AvroSchema::String => Ok(Box::new(StringBuilder::with_capacity(cap, 2048))),
        AvroSchema::Enum(_) => Ok(Box::new(StringBuilder::with_capacity(cap, 2048))),
        AvroSchema::Uuid => Ok(Box::new(FixedSizeBinaryBuilder::with_capacity(cap, 16))),
        AvroSchema::Array(schema) => array_builder(schema, cap),
        AvroSchema::Map(schema) => map_builder(schema, cap),
        AvroSchema::Record(schema) => struct_builder(schema, cap),
        AvroSchema::Union(schema) => {
            if is_optional(schema) {
                create_builder(&schema.variants()[1], cap)
            } else {
                Err("only unions of king [null, TYPE] are supported".into())
            }
        }
        _ => Err(format!("cannot create builder for {:?}", avro).into()),
    }
}

#[inline]
fn arrow_decimal_type(schema: &DecimalSchema) -> DataType {
    DataType::Decimal128(schema.precision as u8, schema.scale as i8)
}

#[inline]
fn decimal_builder(schema: &DecimalSchema, cap: usize) -> Decimal128Builder {
    Decimal128Builder::with_capacity(cap).with_data_type(arrow_decimal_type(schema))
}

#[inline]
fn arrow_timestamp_type(time_unit: TimeUnit) -> DataType {
    DataType::Timestamp(time_unit, Some(tz_offset!()))
}

#[inline]
fn local_timestamp_ms_builder(cap: usize) -> TimestampMillisecondBuilder {
    TimestampMillisecondBuilder::with_capacity(cap)
        .with_data_type(arrow_timestamp_type(TimeUnit::Millisecond))
}

#[inline]
fn local_timestamp_mc_builder(cap: usize) -> TimestampMicrosecondBuilder {
    TimestampMicrosecondBuilder::with_capacity(cap)
        .with_data_type(arrow_timestamp_type(TimeUnit::Microsecond))
}

#[inline]
fn local_timestamp_ns_builder(cap: usize) -> TimestampNanosecondBuilder {
    TimestampNanosecondBuilder::with_capacity(cap)
        .with_data_type(arrow_timestamp_type(TimeUnit::Nanosecond))
}

fn array_builder(
    schema: &ArraySchema,
    cap: usize,
) -> Result<Box<dyn ArrayBuilder>, Box<dyn Error>>  {
    Ok(Box::new(ListBuilder::with_capacity(create_builder(&schema.items, cap)?, cap)))
}

fn map_builder(
    schema: &MapSchema,
    cap: usize,
) -> Result<Box<dyn ArrayBuilder>, Box<dyn Error>> {
    let keys_builder = StringBuilder::with_capacity(cap, 2048);
    let values_builder = create_builder(schema.types.as_ref(), cap)?;
    Ok(Box::new(MapBuilder::new(None, keys_builder, values_builder)))
}

fn struct_builder(
    schema: &RecordSchema,
    cap: usize,
) -> Result<Box<dyn ArrayBuilder>, Box<dyn Error>> {
    let mut fields: Vec<Field> = Vec::new();
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    for f in &schema.fields {
        let t = convert_to_datatype(&f.schema)?;
        fields.push(Field::new(f.name.clone(), t, is_nullable(&f.schema)));
        builders.push(create_builder(&f.schema, cap)?);
    }

    Ok(Box::new(StructBuilder::new(fields, builders)))
}
