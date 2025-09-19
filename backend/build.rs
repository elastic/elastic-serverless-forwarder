// build.rs

use std::{
    env,
    fs::{self, File,},
    io::{BufWriter, Write},
    path::Path,
};

use serde::{Serialize, Deserialize};

use convert_case::{Case};
use ron;

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("fields.rs");
    dump_map(&dest_path);
    for f in ["build.rs", "src/lib.rs", "src/error.rs", "src/convert.rs"].into_iter() {
        println!("cargo::rerun-if-changed={f}");
        println!("warning::rerun-if-changed={f}");
    }
}

#[derive(Deserialize, Serialize)]
struct Field {
    id: u32,
    name: String,
    typeT: String,
}

fn dump_map(pth: &Path) {

    // open the ron file
    let ron_path = Path::new("fields.ron");
    let fields : Vec<Field> = ron::from_str(&fs::read_to_string(ron_path)
        .expect("Should be able to read the Fields"))
        .expect("Failed to parse ron string");

    let file = File::create(pth).unwrap();
    let mut writer = BufWriter::new(file);

    let header = vec![
        "extern crate phf;",
        "use phf::phf_map;",
        "use std::collections::HashMap;",
        "pub static RFC_5102_INFO_ELEMENT : phf::Map<u32, &Element> = phf_map! {",
    ];

    let footer = vec![
        "};",
    ];

    for line in header {
        writeln!(writer, "{}", line).unwrap();
    }

    let mut last_id = 0;
    for field in fields {
        while field.id > last_id + 1 {
            last_id += 1;
            writeln!(writer, "{0}  => &Element{{name: \"RESERVED_{0}\", typeT: \"octetarray\"}},",
                last_id);
        }
        writeln!(writer, "{0}  => &Element{{name: \"{1}\", typeT: \"{2}\"}},",
            field.id, field.name, field.typeT);
        last_id += 1;
    }

    for line in footer {
        writeln!(writer, "{}", line).unwrap();
    }

    let _ = r#"
};
        "#;
}

