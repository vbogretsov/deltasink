use std::collections::HashMap;
use std::borrow::{Borrow, Cow};
use std::rc::Rc;

use apache_avro::schema::*;

use crate::RegistryError;
use crate::client::Client;

#[derive(Hash, PartialEq, Eq)]
struct SchemaKey<'a> {
    subject: Cow<'a, String>,
    version: i32,
}

/* impl Borrow<SchemaKey> for SchemaKey {
    fn borrow(&self) -> &SchemaKey {
        k
    }
} */

// TODO: Minimize usage of keys .clone during hash map lookups.
pub struct AvroRegistry<'a> {
    client: Client,
    cache: HashMap<(String, i32), Rc<Schema>>,
    cache2: HashMap<SchemaKey<'a>, Rc<Schema>>,
    cache_raw: HashMap<(String, i32), String>,
}

macro_rules! err_cycle {
    ($subject:expr, $version:expr) => {
        Err(RegistryError::ResolutionFailed(format!("cycle detected at {}:{}", $subject, $version)))
    };
}

impl<'a> AvroRegistry<'a> {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            cache: HashMap::new(),
            cache2: HashMap::new(),
            cache_raw: HashMap::new(),
        }
    }

    pub fn get<'b: 'a>(
        &mut self,
        subject: &'b str,
        version: i32,
    ) -> Result<Rc<Schema>, RegistryError> {
        let key = (subject.to_string(), version);
        match self.cache.get(&key) {
            Some(schema) => Ok(schema.clone()),
            None => {
                let mut schemas: Vec<(String, i32, String)> = Vec::new();
                let mut seen: HashMap<(String, i32), i32> = HashMap::new();

                self.resolve(subject, version, &mut schemas, &mut seen)?;

                let schemas_raw: Vec<&str> = schemas
                    .iter()
                    .map(|(_, _, value)| value.as_ref())
                    .collect();

                let avro_schemas = match Schema::parse_list(&schemas_raw) {
                    Ok(result) => Ok(result),
                    Err(e) => Err(RegistryError::DeserializationFailed(e.to_string())),
                }?;

                dbg!(&avro_schemas);

                for i in 0..avro_schemas.len() {
                    let key = &schemas[i];
                    let value = &avro_schemas[i];
                    self.cache.insert((key.0.clone(), key.1), Rc::from(value.clone()));
                }

                Ok(self.cache[&key].clone())
            }
        }
    }

    fn resolve<'b: 'a>(
        &mut self,
        subject: &'a str,
        version: i32,
        schemas: &mut Vec<(String, i32, String)>,
        seen: &mut HashMap<(String, i32), i32>,
    ) -> Result<(), RegistryError> {
        let key = (subject.to_string(), version);
        match seen.get(&key) {
            Some(status) => {
                match status {
                    0 => {
                        err_cycle!(subject, version)
                    }
                    1 => {
                        Ok(())
                    }
                    _ => unreachable!()
                }
            }
            None => {
                seen.insert(key.clone(), 0);
                match self.cache_raw.get(&key) {
                    Some(value) => {
                        schemas.push((subject.to_string(), version, value.clone()));
                        *seen.get_mut(&key).unwrap() = 1;
                        Ok(())
                    }
                    None => {
                        let response = match self.client.get_subject(subject, version) {
                            Ok(result) => Ok(result),
                            Err(e) => Err(RegistryError::ClientError(e.to_string())),
                        }?;
                        self.cache_raw.insert(key.clone(), response.schema.clone());

                        schemas.push((subject.to_string(), version, response.schema));
                        if let Some(refs) = response.references {
                            for dep in refs {
                                let name = match dep.subject.strip_suffix("-value") {
                                    Some(value) => value,
                                    None => &dep.subject,
                                };
                                self.resolve(name, dep.version, schemas, seen)?;
                            }
                        };

                        *seen.get_mut(&key).unwrap() = 1;
                        Ok(())
                    }
                }
            }
        }
    }
}

// TODO: Remove these functions.
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
