use anyhow::{Context, Result};
use log::info;
use minijinja::State;
use num_format::{Locale, ToFormattedString};
use serde::Deserialize;
use serde_json::Value;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Instant;
use libsqlitesite::c14n_url_w_slash;

pub(crate) fn path2(name: &str, min_nid: u64) -> String {
    if name == "(unnamed)" {
        format!("{}-{:012}", name, min_nid)
    } else {
        format!("{}.{:012}", name_hash2(name), min_nid)
    }
}

pub(crate) fn name_hash2(name: &str) -> String {
    let hash = calculate_hash(&name);
    let name = slugify(name);
    format!("{}-{:03}", name, hash % 1000)
}

pub(crate) fn slugify(s: &str) -> String {
    let replace_with_hypen = [' ', '/'];
    let deletes = ['(', ')', '\'', '\"', '.', '&', '#', '*', ','];

    let mut s = s.to_lowercase();
    for c in replace_with_hypen {
        s = s.replace(c, "-");
    }
    for c in deletes {
        s = s.replace(c, "");
    }

    while s.contains("--") {
        s = s.replace("--", "-")
    }

    s
}

pub(crate) fn fmt_length(l: String) -> String {
    let l: f64 = match l.parse() {
        Err(_) => {
            return l;
        }
        Ok(x) => x,
    };
    if l < 1000. {
        format!("{} m", (l.round() as u64).to_formatted_string(&Locale::en))
    } else {
        let l_km_int = (l / 1000.).round() as u64;
        format!("{} km", l_km_int.to_formatted_string(&Locale::en))
    }
}

/// Simple hash value
pub(crate) fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// Parse this json string into the json object
pub(crate) fn parse_inner_json_value(val: &mut serde_json::Value) -> Result<()> {
    let new_val = serde_json::from_str(
        val.as_str()
            .with_context(|| format!("inner json value not a str, it's {:?}", val))?,
    )?;
    let _ = std::mem::replace(val, new_val);
    Ok(())
}

/// Round this float to this many places after the decimal point.
/// Used to reduce size of output geojson file
pub(crate) fn round(f: f64, places: u8) -> f64 {
    let places: f64 = 10_u64.pow(places as u32) as f64;
    (f * places).round() / places
}

pub(crate) fn opt_link_path(state: &State, text: String, path: String) -> String {
    format!(
        r#"<a href="{url_prefix}{path}">{text}</a>"#,
        url_prefix = c14n_url_w_slash(state.lookup("url_prefix").unwrap().as_str().unwrap()),
        path = path,
        text = text
    )
}

pub(crate) fn sexagesimal(degrees: f64) -> String {
    let mut deg = degrees.trunc() as i32;
    let min_f = (degrees.abs() - deg.abs() as f64) * 60.0;
    let mut min = min_f.trunc() as i32;
    let mut sec = ((min_f - min as f64) * 60.0).round() as i32;

    // Handle rounding up to 60 seconds
    if sec == 60 {
        if min == 59 {
            deg = deg + deg.signum();
            min = 0;
        }
        min += 1;
        sec = 0;
    }

    format!("{}°\u{00a0}{}′\u{00a0}{}″", deg.abs(), min, sec)
}

pub(crate) fn fmt_latlng(val: &minijinja::Value) -> String {
    let val: serde_json::Value = serde_json::Value::deserialize(val).unwrap();
    let lat = val["lat"].as_f64().unwrap();
    let lng = val["lon"].as_f64().unwrap();
    format!(
        "{}\u{00a0}{}, {}\u{00a0}{}",
        if lat > 0. { "N" } else { "S" },
        sexagesimal(lat),
        if lng > 0. { "E" } else { "W" },
        sexagesimal(lng),
    )
}

pub fn format_duration_human(duration: &std::time::Duration) -> String {
    let sec_f = duration.as_secs_f32();
    if sec_f < 60. {
        let msec = (sec_f * 1000.).round() as u64;
        if sec_f > 0. && msec == 0 {
            "<1ms".to_string()
        } else if msec > 0 && duration.as_secs_f32() < 1. {
            format!("{}ms", msec)
        } else {
            format!("{:>3.1}s", sec_f)
        }
    } else {
        let sec = sec_f.round() as u64;
        let (min, sec) = (sec / 60, sec % 60);
        if min < 60 {
            format!("{}m{:02}s", min, sec)
        } else {
            let (hr, min) = (min / 60, min % 60);
            if hr < 24 {
                format!("{}h{:02}m{:02}s", hr, min, sec)
            } else {
                let (day, hr) = (hr / 24, hr % 24);
                format!("{}d{:02}h{:02}m{:02}s", day, hr, min, sec)
            }
        }
    }
}

pub(crate) fn apply_to_all_floats(val: &mut Value, func: &impl Fn(f64) -> f64) {
    match val {
        Value::Null | Value::Bool(_) | Value::String(_) => {}
        Value::Array(arr) => {
            for el in arr.iter_mut() {
                apply_to_all_floats(el, func);
            }
        }
        Value::Object(o) => {
            for (_key, val) in o.iter_mut() {
                apply_to_all_floats(val, func);
            }
        }
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                *n = serde_json::Number::from_f64(func(f)).unwrap();
            }
        }
    }
}

pub(crate) fn xml_encode(s: String) -> String {
    let s = s.replace("&", "&amp;");
    let s = s.replace("<", "&lt;");
    let s = s.replace(">", "&gt;");
    let s = s.replace("'", "&apos;");

    s.replace("\"", "&quot;")
}
//
//pub(crate) opt_compress(
//        if let Some(ref mut html_zstd_dict_comp) = html_zstd_dict_comp {
//            let new_content = html_zstd_dict_comp.compress(&content)?;
//            let _ = std::mem::replace(&mut content, new_content);
//        }

pub struct ElapsedPrinter {
    started: Instant,
    task_name: String,
}

impl ElapsedPrinter {
    pub fn start(task_name: impl Into<String>) -> Self {
        Self {
            task_name: task_name.into(),
            started: Instant::now(),
        }
    }
}

impl Drop for ElapsedPrinter {
    fn drop(&mut self) {
        info!(
            "Finished {} in {}",
            self.task_name,
            format_duration_human(&self.started.elapsed())
        );
    }
}
