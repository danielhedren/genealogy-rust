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

            // TODO: Move the lowercasing to the front-end
            let lower_data: Vec<String> = data.iter().map(|ref x| x.to_lowercase()).collect();

            let url = env::var("DATABASE_URL").expect("DATABASE_URL env var missing");
            let connection = PgConnection::establish(&url).expect("Couldn't connect to database");
            let result: Vec<(String, Option<f64>, Option<f64>, bool)> = sql("SELECT address, longitude, latitude, valid FROM geocodes WHERE lower(address) = ANY (")
            .bind::<sql_types::Array<sql_types::Text>,_>(&lower_data)
            .sql(") ORDER BY valid DESC;")
            .load(&connection)
            .expect("Error selecting from geocodes");

            let mut lower_data_hashset: HashSet<String> = HashSet::from_iter(lower_data);
            let mut out_data: Vec<Geocode> = Vec::new();

            for row in result {
                if lower_data_hashset.remove(&row.0.to_lowercase()) {
                    if row.3 {
                        out_data.push(Geocode {address: row.0, longitude: row.1.unwrap_or(0.0), latitude: row.2.unwrap_or(0.0)})
                    }
                }
            }

            if lower_data_hashset.len() > 0 {
                let mut pending = Vec::new();
                for a in lower_data_hashset {
                    // TODO: set status field default to 0 in DB
                    pending.push((geocodes_pending::columns::address.eq(a), geocodes_pending::columns::status.eq(0)));
                }

                let query = insert_into(geocodes_pending::table)
                .values(&pending)
                .on_conflict(geocodes_pending::columns::address)
                .do_nothing();
                println!("{}", debug_query::<pg::Pg, _>(&query));
                query.execute(&connection);
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

fn main() {
    server::HttpServer::new(
        || App::new()
            .resource("/", |r| r.f(index))
            .resource("/api/queue_status", |r| r.f(queue_status))
            .resource("/api/geocodepost", |r| r.f(geocode_post)))
        .bind("0.0.0.0:8000").expect("Couldn't bind to address")
        .run();
}
