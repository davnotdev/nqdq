use axum::{
    extract::{Query, State},
    routing::get,
    Router,
};
use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct NQParams {
    app: String,
}

#[derive(Debug, Deserialize)]
pub struct DQParams {
    app: String,
    id: String,
}

#[derive(Debug, Default)]
pub struct Data {
    registered_apps: HashMap<String, (VecDeque<(String, Instant)>, Instant)>,
    config: Config,
}

macro_rules! env_or {
    ($NAME: expr, $DEFAULT: expr) => {
        std::env::vars()
            .find_map(|(k, v)| (k == $NAME && !v.is_empty()).then(|| v.parse().unwrap()))
            .unwrap_or($DEFAULT)
    };
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    garbage_cycle_time: Duration,
    queue_expire_time: Duration,
    app_expire_time: Duration,
    queue_full_cap: u64,
    queue_max_cap: u64,
    app_max: u64,
    app_name_max_length: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            garbage_cycle_time: Duration::from_secs(env_or!("GARBAGE_CYCLE_TIME", 8)),
            queue_expire_time: Duration::from_secs(env_or!("QUEUE_EXPIRE_TIME", 4)),
            app_expire_time: Duration::from_secs(env_or!("APP_EXPIRE_TIME", 1800)),
            queue_full_cap: env_or!("QUEUE_FULL_CAP", 50),
            queue_max_cap: env_or!("QUEUE_MAX_CAP", u64::MAX),
            app_max: env_or!("APP_MAX", u64::MAX),
            app_name_max_length: env_or!("APP_NAME_MAX_LENGTH", 50),
        }
    }
}

const ERR_APP_NAME_LENGTH: &str = "APP_NAME_LENGTH";
const NQ_ERR_APPS_FULL: &str = "APPS_FULL";
const NQ_ERR_QUEUE_FULL: &str = "QUEUE_FULL";
const DQ_ERR_APP_EXPIRE: &str = "APP_EXPIRE";
const DQ_ERR_ID_EXPIRE: &str = "ID_EXPIRE";

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let data = Arc::new(Mutex::new(Data::default()));
    println!("{:#?}", { data.lock().unwrap().config });

    let app = Router::new()
        .route("/nq", get(get_nq))
        .route("/dq", get(get_dq))
        .with_state(Arc::clone(&data))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(format!(
        "{}:{}",
        env_or!("ADDR", "127.0.0.1".to_owned()),
        env_or!("PORT", 8080)
    ))
    .await
    .unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());
    tokio::try_join!(
        tokio::spawn(async move {
            let garbage_cycle_time = { data.lock().unwrap().config.garbage_cycle_time };
            loop {
                tokio::time::sleep(garbage_cycle_time).await;
                garbage_collector(&data).await;
            }
        }),
        tokio::spawn(async { axum::serve(listener, app).await.unwrap() })
    )
    .unwrap();
}

async fn get_nq(State(state): State<Arc<Mutex<Data>>>, Query(query): Query<NQParams>) -> String {
    let id_or_err = 'a: {
        let mut state = state.lock().unwrap();
        let config = state.config;

        if query.app.chars().count() > config.app_name_max_length {
            break 'a Err(ERR_APP_NAME_LENGTH);
        }

        if state.registered_apps.len() as u64 >= config.app_max {
            break 'a Err(NQ_ERR_APPS_FULL);
        }

        let (queries, _) = state
            .registered_apps
            .entry(query.app.clone())
            .or_insert((VecDeque::new(), Instant::now()));

        if queries.len() as u64 > config.queue_max_cap {
            break 'a Err(NQ_ERR_QUEUE_FULL);
        }

        let id = Uuid::new_v4().to_string();
        queries.push_back((id.clone(), Instant::now()));

        Ok(id)
    };

    match id_or_err {
        Ok(id) => format!("OK;{}", id),
        Err(err) => format!("ERR;{}", err),
    }
}

async fn get_dq(State(state): State<Arc<Mutex<Data>>>, Query(query): Query<DQParams>) -> String {
    let err = 'a: {
        let mut state = state.lock().unwrap();
        let config = state.config;

        if query.app.chars().count() > config.app_name_max_length {
            break 'a Some(ERR_APP_NAME_LENGTH);
        }

        let Some((queries, time_ping)) = state.registered_apps.get_mut(&query.app) else {
            break 'a Some(DQ_ERR_APP_EXPIRE);
        };
        *time_ping = Instant::now();

        let Some((idx, instant)) = queries
            .iter_mut()
            .enumerate()
            .find_map(|(idx, (id, instant))| (*id == query.id).then_some((idx, instant)))
        else {
            break 'a Some(DQ_ERR_ID_EXPIRE);
        };

        if idx as u64 > config.queue_full_cap {
            *instant = Instant::now();
            return "NO".to_owned();
        }

        queries.remove(idx);
        return "OK".to_owned();
    };

    match err {
        Some(err) => format!("ERR;{}", err),
        _ => unreachable!(),
    }
}

async fn garbage_collector(state: &Arc<Mutex<Data>>) {
    let mut state = state.lock().unwrap();
    let config = state.config;

    state.registered_apps.retain(|_, (queries, time_ping)| {
        if Instant::now().duration_since(*time_ping) > config.app_expire_time {
            return false;
        }

        queries.retain(|(_, time_ping)| {
            Instant::now().duration_since(*time_ping) < config.queue_expire_time
        });

        true
    })
}
