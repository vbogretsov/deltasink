use pretty_assertions::assert_eq;

#[test]
fn test_get_versions_ok() {
    let mut server = mockito::Server::new();
    let _m_versions = server
        .mock("GET", "/subjects/test-value/versions")
        .with_status(200)
        .with_body(r#"[1, 2]"#)
        .create();

    let client = reqwest::blocking::Client::new();
    let registry = sregistry::Client::new(client, server.url());

    let versions = registry.get_versions("test-value").unwrap();
    assert_eq!(versions, vec![1, 2]);
}

#[test]
fn test_get_versions_404() {
    let mut server = mockito::Server::new();
    let _m_versions = server
        .mock("GET", "/subjects/test-value/versions")
        .with_status(404)
        .with_body(serde_json::json!({
            "detail": "not found"
        }).to_string())
        .create();

    let client = reqwest::blocking::Client::new();
    let registry = sregistry::Client::new(client, server.url());

    let versions = registry.get_versions("test-value");
    assert!(versions.is_err(), "expected error but got ok");
}

#[test]
fn test_get_schema_ok() {
    let v1 = r#"
        {
            "type": "record",
            "name": "User",
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
                  "name": "first_name",
                  "type": ["null", "string"],
                  "default": null
                },
                {
                  "name": "last_name",
                  "type": ["null", "string"],
                  "default": null
                }
            ]
        }
    "#;
    let v2 = r#"
        {
            "type": "record",
            "name": "User",
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
                  "name": "first_name",
                  "type": ["null", "string"],
                  "default": null
                },
                {
                  "name": "last_name",
                  "type": ["null", "string"],
                  "default": null
                },
                {
                  "name": "birthday",
                  "type": ["null", {
                    "type": "int",
                    "logicalType": "date"
                  }],
                  "default": null
                },
                {
                  "name": "raiting",
                  "type": ["null", "float"],
                  "default": null
                },
                {
                  "name": "address",
                  "type": ["null", "Address"]
                }
            ]
        }
    "#;

    let mut server = mockito::Server::new();

    let _m_version_1 = server
        .mock("GET", "/subjects/user-value/versions/1")
        .with_status(200)
        .with_body(
            serde_json::json!({
                "id": 1,
                "schemaType": "AVRO",
                "schema": v1,
                "version": 1,
            })
            .to_string(),
        )
        .create();

    let _m_version_2 = server
        .mock("GET", "/subjects/user-value/versions/2")
        .with_status(200)
        .with_body(
            serde_json::json!({
                "id": 2,
                "schemaType": "AVRO",
                "schema": v2,
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

    let client = reqwest::blocking::Client::new();
    let instance = sregistry::Client::new(client, server.url());

    assert_eq!(
        sregistry::Subject{
            id: 1,
            schema_type: "AVRO".to_string(),
            schema: v1.to_string(),
            references: None,
        },
        instance.get_subject("user-value", 1).unwrap(),
    );
    assert_eq!(
        sregistry::Subject{
            id: 2,
            schema_type: "AVRO".to_string(),
            schema: v2.to_string(),
            references: vec![
                sregistry::Reference{
                    name: "Address".to_string(),
                    subject: "Address-value".to_string(),
                    version: 1,
                },
            ].into(),
        },
        instance.get_subject("user-value", 2).unwrap(),
    );
}

#[test]
fn test_get_schema_404() {
    let mut server = mockito::Server::new();

    let _m_version_1 = server
        .mock("GET", "/subjects/user-value/versions/1")
        .with_status(200)
        .with_body(serde_json::json!({
            "detail": "not found",
        }).to_string())
        .create();

    let client = reqwest::blocking::Client::new();
    let registry = sregistry::Client::new(client, server.url());

    let res = registry.get_subject("user-value", 1);
    assert!(res.is_err(), "expected error but got ok");
}
