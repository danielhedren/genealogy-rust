extern crate actix;
extern crate actix_web;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde;
#[macro_use]
extern crate diesel;
extern crate futures;
#[macro_use]
extern crate json;

use std::{env, collections::HashSet, iter::FromIterator};
use diesel::*;
use diesel::dsl::sql;
use actix_web::*;
use futures::Future;

table! {
    geocodes_pending (id) {
        id -> Bigint,
        address -> Text,
        status -> Smallint,
    }
}

table! {
    geocodes (address, source) {
        address -> Text,
        latitude -> Double,
        longitude -> Double,
        valid -> Bool,
        source -> Text,
    }
}

#[derive(Serialize)]
struct QueueStatus {
    status: String,
    queue_current: i64
}

#[derive(Serialize)]
struct Geocode {
    address: String,
    latitude: f64,
    longitude: f64
}

#[derive(Serialize)]
struct GeocodeResponse {
    queue_current: i64,
    queue_target: i64,
    data: Vec<Geocode>
}

#[derive(Serialize, Deserialize, Debug)]
struct GeocodePost {
    address: Vec<String>
}

#[derive(Serialize, Deserialize, Debug)]
struct GeocodeInsert {
    address: String,
    latitude: f64,
    longitude: f64
}

fn index(_req: HttpRequest) -> &'static str {
    "OK"
}

fn queue_status(_req: HttpRequest) -> Result<Json<QueueStatus>> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL env var missing");
    let connection = PgConnection::establish(&url).expect("Couldn't connect to database");
    let current: i64 = sql("SELECT id FROM geocodes_pending ORDER BY id ASC LIMIT 1;").get_result(&connection).unwrap_or(-1);

    let mut status = String::from("OK");

    let result: QueryResult<i64> = sql("SELECT id FROM geocodes_pending WHERE address='OVER_QUERY_LIMIT' AND status=-1 LIMIT 1;").get_result(&connection);
    match result {
        Ok(_x) => status = String::from("OVER_QUERY_LIMIT"),
        Err(_x) => ()
    }

    Ok(Json(QueueStatus {status: status, queue_current: current}))
}

fn geocode_post(req: HttpRequest) -> Box<Future<Item=HttpResponse, Error=Error>> {
    req.json()
        .from_err()  // convert all errors into `Error`
        .and_then(|data: Vec<String>| {
            let url = env::var("DATABASE_URL").expect("DATABASE_URL env var missing");
            let connection = PgConnection::establish(&url).expect("Couldn't connect to database");
            let result: Vec<(String, Option<f64>, Option<f64>, bool)> = sql("SELECT address, longitude, latitude, valid FROM geocodes WHERE address = ANY (")
            .bind::<sql_types::Array<sql_types::Text>,_>(&data)
            .sql(") ORDER BY source DESC;")
            .load(&connection)
            .expect("Error selecting from geocodes");

            let mut data_hashset: HashSet<String> = HashSet::from_iter(data);
            let mut out_data: Vec<Geocode> = Vec::new();

            for row in &result {
                if row.3 {
                    if data_hashset.remove(&row.0) {
                        out_data.push(Geocode {address: row.0.clone(), longitude: row.1.unwrap_or(0.0), latitude: row.2.unwrap_or(0.0)});
                    }
                }
            }
            for row in &result {
                if !row.3 {
                    data_hashset.remove(&row.0);
                }
            }

            if data_hashset.len() > 0 {
                // TODO: Rewrite init as map
                let mut pending = Vec::new();
                for a in data_hashset {
                    pending.push(geocodes_pending::columns::address.eq(a));
                }

                insert_into(geocodes_pending::table)
                .values(&pending)
                .on_conflict(geocodes_pending::columns::address)
                .do_nothing()
                .execute(&connection)
                .unwrap_or(0);
            }

            let queue_target: i64 = sql("SELECT id FROM geocodes_pending ORDER BY id DESC LIMIT 1;").get_result(&connection).unwrap_or(0);
            let queue_current: i64 = sql("SELECT id FROM geocodes_pending ORDER BY id ASC LIMIT 1;").get_result(&connection).unwrap_or(0);

            let body = serde_json::to_string(&GeocodeResponse {queue_current: queue_current, queue_target: queue_target, data: out_data})?;

            Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(body))
        })
        .responder()
}

fn geocode_insert(req: HttpRequest) -> Box<Future<Item=HttpResponse, Error=Error>> {
    let remote_addr = String::from("remote_addr|") + req.connection_info().remote().unwrap_or("unknown").clone();
    req.json()
        .from_err()  // convert all errors into `Error`
        .and_then(move |data: GeocodeInsert| {
            println!("{:?}", data);
            let url = env::var("DATABASE_URL").expect("DATABASE_URL env var missing");
            let connection = PgConnection::establish(&url).expect("Couldn't connect to database");
            let query = insert_into(geocodes::table)
            .values((
                geocodes::columns::address.eq(data.address.to_lowercase()),
                geocodes::columns::latitude.eq(data.latitude),
                geocodes::columns::longitude.eq(data.longitude),
                geocodes::columns::valid.eq(true),
                geocodes::columns::source.eq(remote_addr)))
            .on_conflict((geocodes::columns::address, geocodes::columns::source))
            .do_update()
            .set((geocodes::columns::latitude.eq(data.latitude), geocodes::columns::longitude.eq(data.longitude)));
            println!("{}", debug_query::<pg::Pg, _>(&query));
            query.execute(&connection).unwrap_or(0);

            Ok(HttpResponse::Ok().finish())
        })
        .responder()
}

fn main() {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL env var missing");
    let connection = PgConnection::establish(&url).expect("Couldn't connect to database");
    sql::<usize>("CREATE TABLE IF NOT EXISTS geocodes (address TEXT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, valid BOOLEAN, source VARCHAR(50), PRIMARY KEY (address, source));")
    .execute(&connection)
    .unwrap_or(0);
    sql::<usize>("CREATE TABLE IF NOT EXISTS geocodes_pending (id BIGSERIAL PRIMARY KEY, address TEXT UNIQUE, status SMALLINT DEFAULT 0);")
    .execute(&connection)
    .unwrap_or(0);

    let sys = actix::System::new("guide");

    server::HttpServer::new(
        || App::new()
            .resource("/", |r| r.f(index))
            .resource("/api/queue_status", |r| r.f(queue_status))
            .resource("/api/geocodepost", |r| r.f(geocode_post))
            .resource("/api/geocode_insert", |r| r.f(geocode_insert)))
        .bind("0.0.0.0:8000").expect("Couldn't bind to address")
        .start();
    
    sys.run();
}
