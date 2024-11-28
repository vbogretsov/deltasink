use std::collections::HashMap;

use apache_avro::schema::*;

#[derive(Debug)]
pub enum RegistryError {
    ExpectedRecord,
    ResolutionFailed(String),
}

pub fn register_schema(
    schema: &Schema,
    schemas: &mut HashMap<Name, Schema>,
) -> Result<(), RegistryError> {
    match schema {
        Schema::Record(record_schema) => {
            schemas.insert(record_schema.name.clone(), schema.clone());
            Ok(())
        }
        _ => Err(RegistryError::ExpectedRecord)
    }
}

pub fn expand_schema(
    src: &Schema,
    schemas: &HashMap<Name, Schema>,
) -> Result<Schema, RegistryError> {
    match src {
        Schema::Record(record_schema) => {
            let fields: Result<Vec<RecordField>, RegistryError> = record_schema
                .fields
                .iter()
                .map(|field| {
                    Ok(RecordField{
                        name: field.name.clone(),
                        doc: field.doc.clone(),
                        aliases: field.aliases.clone(),
                        default: field.default.clone(),
                        custom_attributes: field.custom_attributes.clone(),
                        order: field.order.clone(),
                        position: field.position.clone(),
                        schema: expand_schema(&field.schema, schemas)?,
                    })
                })
                .collect();
            Ok(Schema::Record(RecordSchema{
                name: record_schema.name.clone(),
                aliases: record_schema.aliases.clone(),
                doc: record_schema.doc.clone(),
                lookup: record_schema.lookup.clone(),
                attributes: record_schema.attributes.clone(),
                fields: fields?,
            }))
        }
        Schema::Map(inner) => {
            Ok(Schema::Map(MapSchema{
                types: Box::new(expand_schema(inner.types.as_ref(), schemas)?),
                attributes: inner.attributes.clone(),
            }))
        }
        Schema::Array(inner) => {
            Ok(Schema::Array(ArraySchema{
                items: Box::new(expand_schema(inner.items.as_ref(), schemas)?),
                attributes: inner.attributes.clone(),
            }))
        }
        Schema::Union(inner) => {
            let union_schemas: Result<Vec<Schema>, RegistryError> = inner
                .variants()
                .iter()
                .map(|s| expand_schema(s, schemas))
                .collect();
            match UnionSchema::new(union_schemas?) {
                Ok(union_schema) => Ok(Schema::Union(union_schema)),
                Err(err) => Err(RegistryError::ResolutionFailed(err.to_string()))
            }
        }
        Schema::Ref { name } => {
            if let Some(resolved) = schemas.get(name) {
                expand_schema(resolved, schemas)
            } else {
                Err(RegistryError::ResolutionFailed(name_to_str(name)))
            }
        }
        _ => Ok(src.clone())
    }
}

fn name_to_str(name: &Name) -> String {
    if let Some(ns) = &name.namespace {
        format!("{}.{}", ns, name.name)
    } else {
        format!(".{}", name.name)
    }
}
