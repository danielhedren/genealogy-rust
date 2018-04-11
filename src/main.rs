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

use std::env;
use diesel::*;
use diesel::dsl::sql;
use actix_web::*;
use futures::Future;

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
    let result: Vec<i64> = sql("SELECT id FROM geocodes_pending ORDER BY id ASC LIMIT 1;").load(&connection).expect("Error selecting from geocodes_pending");
    let mut current = -1;

    if result.len() == 1 {
        current = result[0];
    }

    let mut status = String::from("OK");

    let result: Vec<i64> = sql("SELECT id FROM geocodes_pending WHERE address='OVER_QUERY_LIMIT' AND status=-1 LIMIT 1;").load(&connection).expect("Error selecting from geocodes_pending");

    if result.len() == 1 {
        status = String::from("OVER_QUERY_LIMIT");
    }

    Ok(Json(QueueStatus {status: status, queue_current: current}))
}

fn geocode_post(req: HttpRequest) -> Box<Future<Item=HttpResponse, Error=Error>> {
    req.json()
        .from_err()  // convert all errors into `Error`
        .and_then(|data: Vec<String>| {

            let lower_data: Vec<String> = data.iter().map(|ref x| x.to_lowercase()).collect();

            let url = env::var("DATABASE_URL").expect("DATABASE_URL env var missing");
            let connection = PgConnection::establish(&url).expect("Couldn't connect to database");
            let result: Vec<(String, Option<f64>, Option<f64>, bool)> = sql("SELECT address, longitude, latitude, valid FROM geocodes WHERE lower(address) = ANY (")
            .bind::<sql_types::Array<sql_types::Text>,_>(lower_data)
            .sql(") ORDER BY valid DESC;")
            .load(&connection)
            .expect("Error selecting from geocodes");

            let mut out_data: Vec<Geocode> = Vec::new();

            for row in result {
                if row.3 {
                    out_data.push(Geocode {address: row.0, longitude: row.1.unwrap_or(0.0), latitude: row.2.unwrap_or(0.0)})
                }
            }

            let body = serde_json::to_string(&GeocodeResponse {queue_current: 0, queue_target: 0, data: out_data})?;

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
