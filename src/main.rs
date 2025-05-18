#![allow(dead_code)]
use anyhow::Result;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use libsqlitesite::c14n_url_w_slash;
use libsqlitesite::SqliteSite;
use log::{info, warn};
use minijinja::{context, Environment};
use num_format::{Locale, ToFormattedString};
use ordered_float::OrderedFloat;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{row::Row, Client, NoTls};
use rayon::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use zstd::bulk::Compressor;

mod utils;
use utils::*;

const FILEEXT_HTTP_RESP_HEADERS: &[(&str, &[(&str, &str)])] = &[
    ("css", &[("content-type", "text/css")]),
    ("html", &[("content-type", "text/html; charset=utf-8")]),
    ("xml", &[("content-type", "text/xml; charset=utf-8")]),
    ("js", &[("content-type", "application/javascript")]),
    ("png", &[("content-type", "image/png")]),
    ("svg", &[("content-type", "image/svg+xml")]),
    ("woff", &[("content-type", "font/woff")]),
    ("woff2", &[("content-type", "font/woff2")]),
];

#[derive(Parser, Debug)]
struct Args {
    /// All the static files to add
    #[arg(long = "static", value_name = "STATIC_DIR/")]
    static_dir: PathBuf,

    /// The templates are hosted here.
    #[arg(long = "templates", value_name = "TEMPLATES_DIR/")]
    template_dir: PathBuf,

    /// Save the pages to this sqlitesite file
    #[arg(short, long = "output", value_name = "OUTPUT.sqlitesite")]
    output_site_db: PathBuf,

    /// Everything is hosted under this URL
    #[arg(long = "prefix", value_name = "/URL/PREFIX")]
    url_prefix: PathBuf,
}

fn main() -> Result<()> {
    let _global = ElapsedPrinter::start("generating everything");
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = Args::parse();

    let global_http_response_headers = vec![
        ("X-Clacks-Overhead", "GNU Terry Pratchett"),
        ("Cache-Control", "max-age=3600; public"),
    ];

    let mut env = setup_jinja_env(&args)?;

    let mut output_site_db = SqliteSite::create(&args.output_site_db)?;

    let mut zstd_dictionaries = HashMap::new();
    get_or_create_zstd_dictionaries(&mut zstd_dictionaries, &mut output_site_db);

    add_static_files(
        &args.url_prefix,
        &args.static_dir,
        &mut output_site_db,
        global_http_response_headers.as_slice(),
        &zstd_dictionaries,
    )?;

    index_page(
        &args,
        &mut env,
        &mut output_site_db,
        global_http_response_headers.as_slice(),
        &zstd_dictionaries,
    )?;

    name_index_pages(
        &args,
        &mut env,
        &mut output_site_db,
        global_http_response_headers.as_slice(),
        &zstd_dictionaries,
    )?;

    individual_river_pages(
        &args,
        &mut env,
        &mut output_site_db,
        global_http_response_headers.as_slice(),
        &zstd_dictionaries,
    )?;

    individual_region_pages(
        &args,
        &mut env,
        &mut output_site_db,
        global_http_response_headers.as_slice(),
        &zstd_dictionaries,
    )?;

    Ok(())
}

fn row_to_json(row: Row) -> Result<Value> {
    let columns = row.columns();
    let mut obj = serde_json::Map::new();

    for (i, col) in columns.iter().enumerate() {
        let column_name = col.name();
        let value: Value = match *col.type_() {
            postgres::types::Type::BOOL => json!(row.get::<_, bool>(i)),
            postgres::types::Type::CHAR => json!(row.get::<_, String>(i)),
            postgres::types::Type::FLOAT8 => json!(row.get::<_, f64>(i)),
            postgres::types::Type::INT4 => json!(row.get::<_, i32>(i)),
            postgres::types::Type::INT8 => json!(row.get::<_, i64>(i)),
            postgres::types::Type::JSON => json!(row.get::<_, serde_json::Value>(i)),
            postgres::types::Type::VARCHAR => json!(row.get::<_, Option<String>>(i)),
            postgres::types::Type::TEXT => json!(row.get::<_, Option<String>>(i)),
            _ => unimplemented!("Unknown type {:?}", col.type_()),
        };
        obj.insert(column_name.to_string(), value);
    }

    Ok(Value::Object(obj))
}

fn do_query(
    conn: &mut Client,
    stmt: &(impl postgres::ToStatement + ?Sized),
    args: &[&(dyn postgres::types::ToSql + Sync)],
) -> Result<Vec<serde_json::Value>> {
    conn.query(stmt, args)?
        .into_iter()
        .map(row_to_json)
        .collect::<Result<Vec<serde_json::Value>>>()
}

fn index_page(
    args: &Args,
    env: &mut minijinja::Environment,
    output_site_db: &mut SqliteSite,
    global_http_response_headers: &[(&str, &str)],
    _zstd_dictionaries: &HashMap<String, (u32, Box<[u8]>)>,
) -> Result<()> {
    let mut conn = connect_to_db()?;
    let url_prefix = &args.url_prefix;
    let rows: Vec<serde_json::Value> = do_query(&mut conn,
        "select
            tag_group_value as name, url_path,
            min_nid, length_m,
            stream_level, stream_level_code,
            coalesce((select json_agg(json_build_object('name', a_name, 'url_path', a_url_path) order by a_name) from ww_a where ww_ogc_fid = ogc_fid and ww_a.a_level = 0), '[]'::json) as countries
        from
            planet_grouped_waterways
        where tag_group_value IS NOT NULL AND length_m > 20000
        order by length_m desc limit 500;", &[])?;


    let index_page = env
        .get_template("index.j2")?
        .render(context!(stream_level0s => rows))?;
    let hdr_idx = output_site_db.get_or_create_http_response_headers_id(
        http_headers_for_fileext("html", global_http_response_headers),
    )?;
    output_site_db.set_url(url_prefix.to_str().unwrap(), None, hdr_idx, &index_page)?;

    Ok(())
}


fn name_index_pages(
    args: &Args,
    env: &mut minijinja::Environment,
    output_site_db: &mut SqliteSite,
    global_http_response_headers: &[(&str, &str)],
    _zstd_dictionaries: &HashMap<String, (u32, Box<[u8]>)>,
) -> Result<()> {
    let _name_index_pages = ElapsedPrinter::start("generating name_index_pages");

    let mut conn = connect_to_db()?;
    let url_prefix = &args.url_prefix;
    let index_max = 1000;
    let total_names: i64 = conn.query_one(r#"select count(distinct tag_group_value) as total from planet_grouped_waterways where tag_group_value IS NOT NULL;"#, &[])?.get(0);
    let num_index_pages = (total_names as f64 / index_max as f64).ceil() as u64;
    info!(
        "There are {} unique names, resulting in {} index pages.",
        total_names.to_formatted_string(&Locale::en),
        num_index_pages
    );

    let bar = ProgressBar::new(num_index_pages);
    bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {human_pos:>4}/{human_len:4} eta: {eta}. Index pages {msg}",
        )
        .unwrap(),
    );

    let name_index_url = url_prefix.join("name-index");
    let mut urls_all_sitemaps: Vec<String> = Vec::new();

    let html_hdr_idx = output_site_db.get_or_create_http_response_headers_id(
        http_headers_for_fileext("html", global_http_response_headers),
    )?;
    let xml_hdr_idx = output_site_db.get_or_create_http_response_headers_id(
        http_headers_for_fileext("xml", global_http_response_headers),
    )?;

    let query = format!(
        r#"
	  WITH Sorted AS (
		  SELECT tag_group_value, COUNT(*) AS count
		  FROM planet_grouped_waterways
		  WHERE tag_group_value IS NOT NULL
		  GROUP BY tag_group_value
		ORDER BY tag_group_value
	  ),
	  Ranked AS (
		  SELECT 
			  tag_group_value, 
			  NTILE({num_index_pages}) OVER (ORDER BY tag_group_value) AS bin_number
		  FROM Sorted
	  )
	  SELECT 
		  bin_number, 
		  MIN(tag_group_value) AS bin_start, 
		  MAX(tag_group_value) AS bin_end
	  FROM Ranked
	  GROUP BY bin_number
	  ORDER BY bin_number;
	  "#,
        num_index_pages = num_index_pages
    );
    let stmt = conn.prepare(&query)?;
    let index_pages: Vec<(i32, String, String)> = conn
        .query(&stmt, &[])?
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect::<Vec<_>>();

    output_site_db.set_url(
        c14n_url_w_slash(name_index_url.to_str().unwrap()),
        None,
        html_hdr_idx,
        env.get_template("name_index_index.j2")?
            .render(context!(index_pages => index_pages))?,
    )?;

    let mut output_site_db_bulk_adder = output_site_db.start_bulk()?;

    let stmt = conn.prepare(r#"select tag_group_value as name, url_path, min_nid, length_m from planet_grouped_waterways where tag_group_value IS NOT NULL AND tag_group_value >= $1 and tag_group_value <= $2 order by tag_group_value;"#)?;

    let template = env.get_template("name_index.j2")?;
    let sitemap_template = env.get_template("sitemap.j2")?;
    let mut urls_for_sitemap: Vec<String> = vec![];

    for (bin_index, bin_start, bin_end) in index_pages {
        bar.inc(1);
        urls_for_sitemap.truncate(0);
        let this_index_page_url = c14n_url_w_slash(
            name_index_url
                .join(bin_index.to_string().as_str())
                .to_str()
                .unwrap(),
        )
        .to_string();
        urls_for_sitemap.push(this_index_page_url.clone());

        let mut rivers: Vec<serde_json::Value> =
            do_query(&mut conn, &stmt, &[&bin_start, &bin_end])?;

        rivers.par_iter_mut().for_each(|row| {
            row["url_path"] = c14n_url_w_slash(url_prefix.join(row["url_path"].as_str().unwrap()).to_str().unwrap()).into();
        });

        urls_for_sitemap.extend(
            rivers
                .iter()
                .map(|r| r["url_path"].as_str().unwrap().to_owned()),
        );

        let data = serde_json::json!({
            "rivers": rivers,
            "from": bin_start,
            "to": bin_end,
        });

        output_site_db_bulk_adder.add_unique_url(
            &this_index_page_url,
            None,
            html_hdr_idx,
            template.render(data)?,
        )?;

        // sitemap for this index page
        let this_sitemap_url = c14n_url_w_slash(
            name_index_url
                .join(bin_index.to_string().as_str())
                .join("sitemap.xml")
                .to_str()
                .unwrap(),
        )
        .to_string();

        if urls_for_sitemap.len() > 40_000 {
            warn!(
                "There are {} URLs in this site map (max is 50k): {}",
                urls_for_sitemap.len(),
                &this_sitemap_url
            );
        }
        output_site_db_bulk_adder.add_unique_url(
            &this_sitemap_url,
            None,
            xml_hdr_idx,
            sitemap_template.render(context!(url_paths => urls_for_sitemap))?,
        )?;
        urls_all_sitemaps.push(this_sitemap_url);
    }

    output_site_db_bulk_adder.finish()?;

    output_site_db.set_url(
        url_prefix.join("sitemap_index.xml").to_str().unwrap(),
        None,
        xml_hdr_idx,
        env.get_template("sitemap_index.j2")?
            .render(context!(url_paths => urls_all_sitemaps))?,
    )?;
    info!(
        "Created global sitemap index at: {}",
        url_prefix.join("sitemap_index.xml").to_str().unwrap()
    );

    //		  'to': index_page['bin_end'],
    //		  'entries': entries,
    //		  'num_index_pages': len(index_pages),
    //		}
    //		i = index_page['bin_number'] - 1
    //		if i > 0:
    //			data_to_render['prev'] = {'i': i+1-1, 'from': index_pages[i-1]['bin_start'], 'to': index_pages[i-1]['bin_end']}
    //		if i < len(index_pages)-1:
    //			data_to_render['next'] = {'i': i+1+1, 'from': index_pages[i+1]['bin_start'], 'to': index_pages[i+1]['bin_end']}
    //		html = name_index_template.render(data_to_render)
    //
    //		with open(output_dir / "name-index" / str(index_page['bin_number']) / "index.html", "wb") as output_fp:
    //			output_fp.write(html.encode("utf8"))

    Ok(())
}

fn individual_river_pages(
    args: &Args,
    env: &mut minijinja::Environment,
    output_site_db: &mut SqliteSite,
    global_http_response_headers: &[(&str, &str)],
    zstd_dictionaries: &HashMap<String, (u32, Box<[u8]>)>,
) -> Result<()> {
    let _elapsed = ElapsedPrinter::start("all individual_river_pages");
    let mut conn1 = connect_to_db()?;
    let mut conn2 = connect_to_db()?;
    let url_prefix = &args.url_prefix;
    let total_rivers: u64 = conn1
        .query_one(
            r#"select count(*) as total from planet_grouped_waterways;"#,
            &[],
        )?
        .get::<_, i64>(0)
        .try_into()
        .unwrap();
    info!(
        "Need to create {} individual river pages.",
        total_rivers.to_formatted_string(&Locale::en)
    );
    let template = env.get_template("river.j2")?;

    let html_hdr_idx = output_site_db.get_or_create_http_response_headers_id(
        http_headers_for_fileext("html", global_http_response_headers),
    )?;
    let html_zstd_dict = zstd_dictionaries.get("html");
    let html_zstd_dict_id = html_zstd_dict.map(|x| x.0);
    let mut html_zstd_dict_comp = html_zstd_dict
        .map(|x| Compressor::with_dictionary(3, &x.1))
        .transpose()?;

    let geojson_hdr_idx = output_site_db.get_or_create_http_response_headers_id(
        http_headers_for_fileext("geojson", global_http_response_headers),
    )?;
    let geojson_zstd_dict = zstd_dictionaries.get("geojson");
    let geojson_zstd_dict_id = geojson_zstd_dict.map(|x| x.0);
    let mut geojson_zstd_dict_comp = geojson_zstd_dict
        .map(|x| Compressor::with_dictionary(3, &x.1))
        .transpose()?;

    let mut output_site_db_bulk_adder = output_site_db.start_bulk()?;

    let all_rivers_sql = conn1.prepare(
        r#"
	    select
            ogc_fid,
            tag_group_value as name,
            (tag_group_value IS NULL) as is_unnamed,
            url_path,
            min_nid, length_m,
            stream_level, stream_level_code,
            branching_distributaries, terminal_distributaries, distributaries_sea,
            side_channels, tributaries,
            ST_AsGeoJSON(ST_Multi(coalesce(ST_Simplify(geom,0.00001), geom))) as geom,
            ST_AsGeoJSON(ST_Expand(geom, 0.001)) as bbox
            from planet_grouped_waterways
            where length_m >= 100000
            ;
	  "#,
    )?;

    let river_in_admins_stmt = conn2.prepare(
        r#"select
        a_name as name, a_iso, a_url_path as url_path
        from ww_a
        where ww_a.ww_ogc_fid = $1 and a_level = 0
		order by a_name
    "#,
    )?;
    let river_in_subregions_stmt = conn2.prepare(
        r#"select
        a_name as name, a_url_path as url_path
        from ww_a
        where ww_a.ww_ogc_fid = $1 and a_level = 1 and a_parent_iso  = $2
        order by a_name
        "#,
    )?;

    let mut rivers_iter = conn1.query_raw(&all_rivers_sql, &[] as &[bool; 0])?; // [bool;0] is just
                                                                                // a hack
    let rivers_iter = std::iter::from_fn(|| {
        rivers_iter
            .next()
            .ok()
            .flatten()
            .and_then(|pgrow| row_to_json(pgrow).ok())
    });

    let bar = ProgressBar::new(total_rivers);
    bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {human_pos:>7}/{human_len:7} {per_sec:>10}. eta: {eta} {msg}",
        )
        .unwrap(),
    );
    bar.set_message("Genering River Pages");
    for mut river in rivers_iter.into_iter() {
        bar.inc(1);
        if river["name"].is_null() {
            river["name"] = "(unnamed)".into();
        }
        parse_inner_json_value(&mut river["geom"])?;

        river["num_tributaries"] = river["tributaries"].as_array().unwrap().len().into();
        river["num_distributaries"] = (river["terminal_distributaries"].as_array().unwrap().len()
            + river["branching_distributaries"].as_array().unwrap().len())
        .into();

        parse_inner_json_value(&mut river["bbox"])?;
        let bbox = river["bbox"]["coordinates"][0].as_array().unwrap();
        let mut bbox = [
            bbox[0][0].as_f64().unwrap(),
            bbox[0][1].as_f64().unwrap(),
            bbox[2][0].as_f64().unwrap(),
            bbox[2][1].as_f64().unwrap(),
        ];
        let width = bbox[2] - bbox[0];
        let height = bbox[3] - bbox[1];
        bbox[0] = round(bbox[0] - width * 0.1, 6);
        bbox[1] = round(bbox[1] - height * 0.1, 6);
        bbox[2] = round(bbox[2] + width * 0.1, 6);
        bbox[3] = round(bbox[3] + height * 0.1, 6);
        river["bbox"] = serde_json::to_string(&bbox).unwrap().into();

        // needed for Overpass BBOX query. yes it's in this order
        river["bbox_bracket"] = format!("({},{},{},{})", bbox[1], bbox[0], bbox[3], bbox[2]).into();

        river["max_upstream_m"] = (river["distributaries_sea"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["upstream_m"].as_f64().unwrap())
            .sum::<f64>()
            + river["terminal_distributaries"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|x| x["confluences"].as_array().unwrap().iter())
                .map(|x| x["upstream_m"].as_f64().unwrap())
                .sum::<f64>())
        .into();

        for key in [
            "tributaries",
            "branching_distributaries",
            "terminal_distributaries",
        ] {
            river[key]
                .as_array_mut()
                .unwrap()
                .par_iter_mut()
                .for_each(|ww| {
                    ww["name"] = ww["tag_group_value"].clone();
                    if ww["name"].is_null() {
                        ww["name"] = "(unnamed)".into();
                    }
                    ww["url_path"] = format!("{} {:012}", ww["name"].as_str().unwrap(), ww["min_nid"].as_i64().unwrap()).into();
                });
        }

        river["url"] = c14n_url_w_slash(url_prefix.join(river["url_path"].as_str().unwrap()).to_string_lossy()).into();

        let mut admin0s = do_query(
            &mut conn2,
            &river_in_admins_stmt,
            &[&(river["ogc_fid"].as_i64().unwrap() as i32)],
        )?;
        for region in admin0s.iter_mut() {
            region["subregions"] = do_query(
                &mut conn2,
                &river_in_subregions_stmt,
                &[
                    &(river["ogc_fid"].as_i64().unwrap() as i32),
                    &region["a_iso"].as_str().unwrap(),
                ],
            )?
            .into();
        }
        river["is_in_regions"] = admin0s.into();

        // Render the template!
        let content = template.render(&river)?;
        let mut content = content.into_bytes();
        anyhow::ensure!(!content.is_empty());

        if let Some(ref mut html_zstd_dict_comp) = html_zstd_dict_comp {
            let new_content = html_zstd_dict_comp.compress(&content)?;
            let _ = std::mem::replace(&mut content, new_content);
        }

        output_site_db_bulk_adder.add_unique_url(
            river["url"].as_str().unwrap(),
            html_zstd_dict_id,
            html_hdr_idx,
            content,
        )?;

        apply_to_all_floats(&mut river["geom"], &|x| round(x, 7));
        let mut content = serde_json::to_string(&river["geom"])?.into_bytes();
        if let Some(ref mut geojson_zstd_dict_comp) = geojson_zstd_dict_comp {
            let new_content = geojson_zstd_dict_comp.compress(&content)?;
            let _ = std::mem::replace(&mut content, new_content);
        }

        output_site_db_bulk_adder.add_unique_url(
            c14n_url_w_slash(url_prefix.join(river["url_path"].as_str().unwrap()).join("geometry.geojson").to_string_lossy()),
            geojson_zstd_dict_id,
            geojson_hdr_idx,
            content,
        )?;
    }

    output_site_db_bulk_adder.finish()?;

    Ok(())
}

fn add_static_files(
    url_prefix: &Path,
    dir: &Path,
    output_site_db: &mut SqliteSite,
    global_http_response_headers: &[(&str, &str)],
    _zstd_dictionaries: &HashMap<String, (u32, Box<[u8]>)>,
) -> Result<()> {
    anyhow::ensure!(dir.exists(), "Static directory {:?} not found", dir);
    anyhow::ensure!(dir.is_dir(), "Static path {:?} is not a directory", dir);

    let http_response_headers: HashMap<String, u32> = FILEEXT_HTTP_RESP_HEADERS
        .iter()
        .map(|(fileext, _hdrs)| {
            let new_headers = http_headers_for_fileext(fileext, global_http_response_headers);
            let hdr_idx = output_site_db
                .get_or_create_http_response_headers_id(new_headers)
                .unwrap();
            (fileext.to_string(), hdr_idx)
        })
        .collect();

    assert!(!global_http_response_headers.is_empty());

    let mut output_site_db_bulk_adder = output_site_db.start_bulk()?;
    let mut contents = Vec::new();
    let mut added_count = 0;
    let mut added_bytes = 0;

    for entry in WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
    {
        let path = entry
            .path()
            .strip_prefix(dir)
            .unwrap()
            .display()
            .to_string();
        let mut file = File::open(entry.path()).unwrap();

        let extension = entry.path().extension().and_then(|s| s.to_str());
        if !FILEEXT_HTTP_RESP_HEADERS
            .iter()
            .any(|(e, _)| *e == extension.unwrap())
        {
            warn!("File extension that is not known {:?}", extension);
        }
        let http_headers_idx = extension
            .and_then(|fileext| http_response_headers.get(fileext))
            .copied();

        contents.truncate(0);
        file.read_to_end(&mut contents).unwrap();
        added_bytes += contents.len();
        added_count += 1;

        output_site_db_bulk_adder.add_unique_url(
            url_prefix.join(path).to_str().unwrap(),
            None,
            http_headers_idx,
            &contents,
        )?;
    }

    output_site_db_bulk_adder.finish()?;
    info!(
        "Added {} static files from {}, totaling {} B",
        added_count,
        dir.display(),
        added_bytes
    );

    Ok(())
}

fn individual_region_pages(
    args: &Args,
    env: &mut minijinja::Environment,
    output_site_db: &mut SqliteSite,
    global_http_response_headers: &[(&str, &str)],
    zstd_dictionaries: &HashMap<String, (u32, Box<[u8]>)>,
) -> Result<()> {
    let mut conn1 = connect_to_db()?;
    let mut conn2 = connect_to_db()?;
    let url_prefix = &args.url_prefix;
    let num_regions: i64 = conn1
        .query_one(r#"select count(*) as admin from admins;"#, &[])?
        .get::<_, i64>(0);
    info!(
        "Need to create {} individual admin region pages.",
        num_regions.to_formatted_string(&Locale::en)
    );
    let region_url = url_prefix.join("region");
    let mut admin0s = HashMap::with_capacity(200);

    let html_hdr_idx = output_site_db.get_or_create_http_response_headers_id(
        http_headers_for_fileext("html", global_http_response_headers),
    )?;
    let html_zstd_dict = zstd_dictionaries.get("html");
    let html_zstd_dict_id = html_zstd_dict.map(|x| x.0);
    let mut html_zstd_dict_comp = html_zstd_dict
        .map(|x| Compressor::with_dictionary(3, &x.1))
        .transpose()?;

    let bar = ProgressBar::new(num_regions as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {human_pos:>7}/{human_len:7} {per_sec:>10}. eta: {eta} Doing {msg}",
        )
        .unwrap()
    );

    let rivers_in_admin_sql = conn2.prepare(
        r#"select
        ww_tag_group_value as name, ww_length_m as length_m, ww_min_nid as min_nid, ww_url_path as url_path
        from ww_a
        WHERE a_ogc_fid = $1
        AND ww_length_m >= 1000
        ORDER BY ww_rank_in_a ASC
        LIMIT 20000
        "#,
    )?;

    let subregions_sql = conn2
        .prepare(r"select
            name, iso, url_path,
            (select count(*) from ww_a where a_ogc_fid = admins.ogc_fid AND ww_tag_group_value IS NOT NULL and ww_length_m >= 1000) as num_rivers,
            coalesce((select json_agg(json_build_object('name', ww_tag_group_value, 'url_path', ww_url_path)) from ww_a where a_ogc_fid = admins.ogc_fid and ww_tag_group_value IS NOT NULL AND ww_rank_in_a <= 5), '[]'::json) as top_rivers
        from admins WHERE parent_iso = $1 order by name")?;

    let admins = r#"select ogc_fid, url_path, name, iso, parent_iso, level
        from admins
        WHERE name IS NOT NULL
        order by level, iso, name
        "#;

    let mut rivers_in_admin = conn1.query_raw(admins, &[] as &[bool; 0])?;
    let rivers_in_admin_iter = std::iter::from_fn(|| {
        rivers_in_admin
            .next()
            .ok()
            .flatten()
            .and_then(|pgrow| row_to_json(pgrow).ok())
    });

    //let mut rivers_in_admin_iter = rivers_in_admin_iter_raw.chunk_by(|row| row.get("admin_ogc_fid").unwrap().clone());
    let set_url_path = |admin: &mut Value| {
        let mut admin_url = region_url.clone();
        admin_url.push(admin["url_path"].as_str().unwrap());
        admin["url_path"] = c14n_url_w_slash(admin_url.display().to_string()).into();
    };

    let mut chunk = Vec::new();
    for mut admin in rivers_in_admin_iter {
        bar.inc(1);
        bar.set_message(admin["url_path"].as_str().unwrap().to_string());
        chunk.truncate(0);
        let mut rows = conn2.query_raw(
            &rivers_in_admin_sql,
            [admin.get("ogc_fid").unwrap().as_i64().unwrap() as i32],
        )?;
        while let Some(row) = rows.next()? {
            chunk.push(row_to_json(row)?);
        }
        if chunk.is_empty() {
            continue;
        }
        drop(rows);
        chunk.par_sort_by_key(|r| {
            OrderedFloat::from(-r.get("length_m").and_then(|v| v.as_f64()).unwrap())
        });

        chunk.par_iter_mut().for_each(|river| {
            if river["name"].is_null() {
                river["name"] = "(unnamed)".into();
            }
            let url = url_prefix.join(river["url_path"].as_str().unwrap());
            river["url_path"] = url.to_string_lossy().into();
        });

        set_url_path(&mut admin);
        admin["num_rivers"] = chunk.len().into();
        admin["num_subregions"] = 0.into();
        //admin["long_rivers"] = chunk.iter().take(5).collect::<Vec<_>>().into();

        let mut subregions = conn2
            .query(&subregions_sql, &[&admin["iso"].as_str().unwrap()])?
            .into_iter()
            .map(row_to_json)
            .collect::<Result<Vec<_>>>()?;
        subregions.par_iter_mut().for_each(set_url_path);
        admin["num_subregions"] = subregions.len().into();
        admin["subregions"] = subregions.into();

        if admin.get("level").unwrap().as_i64() == 0.into() {
            admin0s.insert(admin["iso"].as_str().unwrap().to_string(), admin.clone());
        }

        admin["parent_region"] = admin
            .get("parent_iso")
            .and_then(Value::as_str)
            .and_then(|parent_iso| admin0s.get(parent_iso))
            .cloned()
            .into();

        let mut content = env
            .get_template("admin_region.j2")?
            .render(context!(region => admin, rivers=> chunk))?
            .into_bytes();

        if let Some(ref mut html_zstd_dict_comp) = html_zstd_dict_comp {
            let new_content = html_zstd_dict_comp.compress(&content)?;
            let _ = std::mem::replace(&mut content, new_content);
        }

        output_site_db.set_url(
            admin["url_path"].as_str().unwrap(),
            html_zstd_dict_id,
            html_hdr_idx,
            content,
        )?;
    }

    let mut admin0s = admin0s.into_values().collect::<Vec<_>>();
    admin0s.par_sort_by(|a, b| {
        a.get("name")
            .and_then(Value::as_str)
            .cmp(&b.get("name").and_then(Value::as_str))
    });

    let mut content = env
        .get_template("admin_region_index.j2")?
        .render(context!(regions => admin0s))?
        .into_bytes();

    if let Some(ref mut html_zstd_dict_comp) = html_zstd_dict_comp {
        let new_content = html_zstd_dict_comp.compress(&content)?;
        let _ = std::mem::replace(&mut content, new_content);
    }

    output_site_db.set_url(
        c14n_url_w_slash(region_url.to_str().unwrap()),
        html_zstd_dict_id,
        html_hdr_idx,
        content,
    )?;

    Ok(())
}

fn setup_jinja_env<'b>(args: &Args) -> Result<minijinja::Environment<'b>> {
    let mut env = Environment::new();
    env.set_loader(minijinja::path_loader(&args.template_dir));

    let url_prefix: String = args.url_prefix.to_str().map(String::from).unwrap();
    env.add_global("url_prefix", url_prefix.clone());

    env.add_filter("fmt_length", fmt_length);
    env.add_filter("fmt_latlng", fmt_latlng);
    env.add_filter("fmt_nid", |nid: String| {
        format!(
            r#"<a href="https://www.openstreetmap.org/node/{nid}/"><code>n{nid}</code></a>"#,
            nid = nid
        )
    });
    env.add_filter("opt_link_path", opt_link_path);
    env.add_filter("c14n_url", |s: String| libsqlitesite::c14n_url_w_slash(s));
    let url_prefix2 = url_prefix.clone();
    env.add_filter("c14n_url_prefix", move |s: String| {
        libsqlitesite::c14n_url_w_slash(format!("/{}/{}", url_prefix2, s))
    });
    env.add_filter("xml_encode", xml_encode);

    env.add_filter("fmt_num", |num: i64| num.to_formatted_string(&Locale::en));
    env.add_filter("pluralize", |num: i64| if num == 1 { "" } else { "s" });
    env.add_filter(
        "if_true",
        |test: bool, output: String| {
            if test {
                output
            } else {
                "".to_string()
            }
        },
    );
    env.add_filter(
        "if_false",
        |test: bool, output: String| {
            if !test {
                output
            } else {
                "".to_string()
            }
        },
    );

    Ok(env)
}

fn pairstring(v: &(&str, &str)) -> (String, String) {
    (v.0.to_string(), v.1.to_string())
}

fn http_headers_for_fileext(
    fileext: &str,
    global_http_response_headers: &[(&str, &str)],
) -> Vec<(String, String)> {
    let mut new_headers = global_http_response_headers
        .iter()
        .map(pairstring)
        .collect::<Vec<(String, String)>>();
    new_headers.extend(
        FILEEXT_HTTP_RESP_HEADERS
            .iter()
            .find(|(this_fileext, _hdrs)| *this_fileext == fileext)
            .into_iter()
            .flat_map(|(_fileext, hdrs)| hdrs.iter())
            .map(pairstring),
    );

    new_headers
}

// can't get the mutable borries on output_site_db to work without this
fn get_or_create_zstd_dictionaries(
    result: &mut HashMap<String, (u32, Box<[u8]>)>,
    output_site_db: &mut SqliteSite,
) {
    let dicts: &[(&str, &[u8])] = &[
        ("html", include_bytes!("html-zstd-dictionary-10K")),
        ("geojson", include_bytes!("geojson-zstd-dictionary-30K")),
    ];

    for (fileext, zstd_dictionary_bytes) in dicts.iter() {
        let dict_id = output_site_db
            .get_or_create_zstd_dictionary(zstd_dictionary_bytes)
            .unwrap();

        result.insert(
            fileext.to_string(),
            (dict_id, zstd_dictionary_bytes.to_vec().into_boxed_slice()),
        );
    }
}

fn connect_to_db() -> Result<Client> {
    Ok(Client::connect(
        "host=/var/run/postgresql/ application_name=\"waterwaymap.org-river\"",
        NoTls,
    )?)
}
