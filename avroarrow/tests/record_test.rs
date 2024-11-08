use fake::Dummy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Dummy)]
struct Example {
    f_string: String,
    f_opt_string: Option<String>,
}

