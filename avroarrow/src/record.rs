use std::any::Any;
use std::error::Error;

use apache_avro::{Schema, Decimal, Uuid};
use apache_avro::types::Value;
use arrow::array::builder::*;
use arrow::datatypes::Fields;
use num_bigint::BigInt;
use num_traits::ToPrimitive;

macro_rules! unexpected_type {
    ($name:expr, $value:expr) => {
        panic!("expected {} but got {:?}", $name, $value)
    };
}

macro_rules! cast {
    ($builder:expr, $type:ty) => {
        cast_unchecked::<$type>($builder.as_any_mut())
    };
}

macro_rules! convert_ex {
    ($builder:expr, $avro:ident, $arrow:ty, $value:expr, $match_arm:pat => $result:expr) => {
        match $value {
            Value::Null => Ok(cast!($builder, $arrow).append_null()),
            $match_arm => Ok(cast!($builder, $arrow).append_value($result)),
            _ => unexpected_type!(stringify!($avro), $value),
        }
    };
}

macro_rules! convert {
    ($builder:expr, $avro:ident, $arrow:ty, $value:expr, $func:expr) => {
        convert_ex!($builder, $avro, $arrow, $value, Value::$avro(v) => $func(v))
    };
}

pub fn append_record(
    builder: &mut dyn ArrayBuilder,
    schema: &Schema,
    record: &Value,
) -> Result<(), Box<dyn Error>>{
    match schema {
        Schema::Boolean => {
            convert!(builder, Boolean, BooleanBuilder, record, deref)
        }

        Schema::Int => {
            convert!(builder, Int, Int32Builder, record, deref)
        }

        Schema::Long => {
            convert!(builder, Long, Int64Builder, record, deref)
        }

        Schema::Float => {
            convert!(builder, Float, Float32Builder, record, deref)
        }

        Schema::Double => {
            convert!(builder, Double, Float64Builder, record, deref)
        }

        Schema::Decimal(_) => convert!(
            builder,
            Decimal,
            Decimal128Builder,
            record,
            from_decimal128
        ),

        Schema::Date => convert!(
            builder,
            Date,
            Date32Builder,
            record,
            deref
        ),

        Schema::TimeMillis => convert!(
            builder,
            TimeMillis,
            Time32MillisecondBuilder,
            record,
            deref
        ),

        Schema::TimeMicros => convert!(
            builder,
            TimeMicros,
            Time64MicrosecondBuilder,
            record,
            deref
        ),

        Schema::TimestampMillis => convert!(
            builder,
            TimestampMillis,
            TimestampMillisecondBuilder,
            record,
            deref
        ),

        Schema::TimestampMicros => convert!(
            builder,
            TimestampMicros,
            TimestampMicrosecondBuilder,
            record,
            deref
        ),

        Schema::TimestampNanos => convert!(
            builder,
            TimestampNanos,
            TimestampNanosecondBuilder,
            record,
            deref
        ),

        Schema::LocalTimestampMillis => convert!(
            builder,
            LocalTimestampMillis,
            TimestampMillisecondBuilder,
            record,
            deref
        ),

        Schema::LocalTimestampMicros => convert!(
            builder,
            LocalTimestampMicros,
            TimestampMicrosecondBuilder,
            record,
            deref
        ),

        Schema::LocalTimestampNanos => convert!(
            builder,
            LocalTimestampNanos,
            TimestampNanosecondBuilder,
            record,
            deref
        ),

        Schema::String => convert!(
            builder,
            String,
            StringBuilder,
            record,
            asis
        ),

        Schema::Enum(_) => convert_ex!(
            builder,
            Enum,
            StringBuilder,
            record,
            Value::Enum(_, v) => v
        ),

        Schema::Bytes => convert!(
            builder,
            Bytes,
            BinaryBuilder,
            record,
            asis
        ),

        Schema::Uuid => {
            let typed = cast!(builder, FixedSizeBinaryBuilder);
            match record {
                Value::Null => Ok(typed.append_null()),
                Value::Uuid(v) => Ok(typed.append_value(from_uuid(v))?),
                _ => unexpected_type!("Uuid", record)
            }
        }

        Schema::Fixed(_) => {
            let typed = cast!(builder, FixedSizeBinaryBuilder);
            match record {
                Value::Null => Ok(typed.append_null()),
                Value::Fixed(_, v) => Ok(typed.append_value(v)?),
                _ => unexpected_type!("Fixed", record)
            }
        }

        Schema::Array(inner) => {
            let typed = cast!(builder, ListBuilder<Box<dyn ArrayBuilder>>);
            match record {
                Value::Null => Ok(typed.append_null()),
                Value::Array(items) => {
                    for item in items {
                        append_record(typed.values(), &inner.items, item)?;
                    }
                    Ok(typed.append(true))
                }
                _ => unexpected_type!("Array", record),
            }
        }

        Schema::Map(inner) => {
            let typed = cast!(builder, MapBuilder<StringBuilder, Box<dyn ArrayBuilder>>);
            match record {
                Value::Null => Ok(typed.append(false)?),
                Value::Map(m) => {
                    for (k, v) in m {
                        typed.keys().append_value(k);
                        append_record(typed.values(), &inner.types, v)?;
                    }
                    Ok(typed.append(true)?)
                },
                _ => unexpected_type!("Map", record),
            }
        }

        Schema::Record(inner) => {
            let typed = cast!(builder, StructBuilder);
            match record {
                Value::Null => Ok(typed.append_null()),
                Value::Record(fields) => {
                    for (i, (_, f_value)) in fields.iter().enumerate() {
                        let f_schema = &inner.fields[i].schema;
                        append_record(struct_field(typed, i), f_schema, f_value)?;
                    }
                    Ok(typed.append(true))
                },
                _ => unexpected_type!("Record", record),
            }
        }

        Schema::Union(inner) => {
            let type_schema = &inner.variants()[1];
            match record {
                Value::Union(_, value) => {
                    append_record(builder, type_schema, value)
                }
                _ => unexpected_type!("Union", record)
            }
        }

        _ => Err(format!("unsupported schema {:?}", schema).into()),
    }
}

#[inline(always)]
pub fn cast_unchecked<T>(any: &mut dyn Any) -> &mut T {
    unsafe { &mut *(any as *mut dyn Any as *mut T) }
}

#[allow(unused)]
struct StructBuilderLayout {
    fields: Fields,
    field_builders: Vec<Box<dyn ArrayBuilder>>,
}

#[inline(always)]
fn struct_field(builder: &mut StructBuilder, i: usize) -> &mut dyn ArrayBuilder {
    unsafe {
        let shadow = builder as *mut dyn Any as *mut StructBuilderLayout;
        &mut (*shadow).field_builders[i]
    }
}

#[allow(unused)]
struct DecimalLayout {
    value: BigInt,
    len: usize,
}

#[inline(always)]
fn decimal_to_bigint(d: &Decimal) -> &BigInt {
    unsafe {
        let shadow = d as *const dyn Any as *const DecimalLayout;
        &(*shadow).value
    }
}

#[inline(always)]
fn asis<T>(v: &T) -> &T {
    v
}

#[inline(always)]
fn deref<T>(v: &'_ T) -> T
where
    T: Copy,
{
    *v
}

#[inline(always)]
fn from_decimal128(d: &Decimal) -> i128 {
    match decimal_to_bigint(d).to_i128() {
        Some(v) => v,
        None => {
            // TODO: Log warn
            0
        }
    }
}

#[inline(always)]
fn from_uuid(u: &Uuid) -> &[u8; 16] {
    u.as_bytes()
}
