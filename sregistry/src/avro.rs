use std::borrow::Borrow;
use std::collections::HashMap;
use std::rc::Rc;

use apache_avro::schema::*;
use ostr::Str;

use crate::RegistryError;
use crate::client::Client;

#[derive(Clone, Hash, Eq, PartialEq)]
struct SchemaKey {
    subject: Str,
    version: i32,
}

#[derive(Hash, Eq, PartialEq)]
struct SchemaKeyRef<'a> {
    subject: &'a str,
    version: i32,
}

impl<'a> Borrow<SchemaKeyRef<'a>> for SchemaKey {
    fn borrow(&self) -> &SchemaKeyRef<'a> {
        unsafe {
            &*(self as *const SchemaKey as *const SchemaKeyRef)
        }
    }
}

impl <'a> ToOwned for SchemaKeyRef<'a> {
    type Owned = SchemaKey;

    fn to_owned(&self) -> Self::Owned {
        SchemaKey{subject: Str::new(self.subject.as_ref()), version: self.version}
    }
}

pub struct AvroRegistry {
    client: Client,
    cache: HashMap<SchemaKey, Rc<Schema>>,
    cache_raw: HashMap<SchemaKey, String>,
}

impl AvroRegistry {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            cache: HashMap::new(),
            cache_raw: HashMap::new(),
        }
    }

    pub fn get(
        &mut self,
        subject: &str,
        version: i32,
    ) -> Result<Rc<Schema>, RegistryError> {
        let key = SchemaKeyRef{subject, version};
        
        if let Some(schema) = self.cache.get(&key) {
            return Ok(schema.clone());
        }

        let mut schemas: Vec<(SchemaKey, String)> = Vec::new();

        self.resolve(subject, version, &mut schemas)?;

        let schemas_raw: Vec<&str> = schemas
            .iter()
            .map(|(_, value)| value.as_ref())
            .collect();

        let avro_schemas = Schema::parse_list(&schemas_raw)
            .map_err(|e| RegistryError::DeserializationFailed(e.to_string()))?;

        let mut tmp_cache: HashMap<Name, Schema> = HashMap::new();
        for i in 0..avro_schemas.len() {
            register_schema(&avro_schemas[i], &mut tmp_cache)?;
        }

        for i in 0..avro_schemas.len() {
            let key = &schemas[i].0;
            let value = expand_schema(&avro_schemas[i], &tmp_cache)?;
            self.cache.insert(key.clone(), Rc::from(value.clone()));
        }

        Ok(self.cache.get(&key).unwrap().clone())
    }

    fn resolve(
        &mut self,
        subject: &str,
        version: i32,
        schemas: &mut Vec<(SchemaKey, String)>,
    ) -> Result<(), RegistryError> {
        let mut stack = vec![
            SchemaKey{subject: subject.into(), version},
        ];

        while let Some(key) = stack.pop() {
            if let Some(value) = self.cache_raw.get(&key) {
                schemas.push((key.to_owned(), value.clone()));
                continue;
            }

            let response = self.client.get_subject(key.subject.as_ref(), key.version)
                .map_err(|e| RegistryError::ClientError(e.to_string()))?;

            self.cache_raw.insert(key.to_owned(), response.schema.clone());
            schemas.push((key.to_owned(), response.schema));

            if let Some(refs) = response.references {
                for dep in refs {
                    stack.push(SchemaKey{
                        subject: Str::new(dep.subject.as_ref()),
                        version: dep.version,
                    });
                }
            }
        }
        
        Ok(())
    }
}

fn register_schema(
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

fn expand_schema(
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
