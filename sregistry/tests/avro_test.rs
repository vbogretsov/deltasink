#[test]
fn test_avro_registry_get_with_references() {
    let location_schema = r#"
        {
            "type": "record",
            "name": "Location",
            "namespace": "com.test",
            "fields": [
                {
                    "name": "latitude",
                    "type": "float"
                },
                {
                    "name": "longtitude",
                    "type": "float"
                }
            ]
        }
    "#;

    let address_schema = r#"
        {
            "type": "record",
            "name": "Address",
            "namespace": "com.test",
            "fields": [
                {
                    "name": "id",
                    "type": "long"
                },
                {
                    "name": "street",
                    "type": "string"
                },
                {
                    "name": "location",
                    "type": "Location"
                }
            ]
        }
    "#;

    let user_schema = r#"
        {
            "type": "record",
            "name": "User",
            "namespace": "com.test",
            "fields": [
                {
                  "name": "id",
                  "type": "long"
                },
                {
                  "name": "email",
                  "type": ["null", "string"],
                  "default": null
                },
                {
                    "name": "address",
                    "type": "Address"
                }
            ]
        }
    "#;

    let mut server = mockito::Server::new();

    let _ = server
        .mock("GET", "/subjects/Location-value/versions/3")
        .with_status(200)
        .with_body(
            serde_json::json!({
                "id": 303,
                "schemaType": "AVRO",
                "schema": location_schema,
                "version": 3,
            })
            .to_string(),
        )
        .create();

    let _ = server
        .mock("GET", "/subjects/Address-value/versions/1")
        .with_status(200)
        .with_body(
            serde_json::json!({
                "id": 101,
                "schemaType": "AVRO",
                "schema": address_schema,
                "version": 1,
                "references": [
                    {
                        "name": "Location",
                        "subject": "Location-value",
                        "version": 3,
                    }
                ]
            })
            .to_string(),
        )
        .create();

    let _ = server
        .mock("GET", "/subjects/User-value/versions/2")
        .with_status(200)
        .with_body(
            serde_json::json!({
                "id": 202,
                "schemaType": "AVRO",
                "schema": user_schema,
                "version": 2,
                "references": [
                    {
                        "name": "Address",
                        "subject": "Address-value",
                        "version": 1,
                    }
                ]
            })
            .to_string(),
        )
        .create();

    let client = sregistry::Client::new(
        reqwest::blocking::Client::new(),
        server.url(),
    );

    let mut instance = sregistry::avro::AvroRegistry::new(client);

    let _actual = instance.get("User", 2).unwrap();
    dbg!(_actual);
}
