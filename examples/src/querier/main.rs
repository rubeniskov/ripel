use anyhow::{Result};
use ripel::{DynamicValue, ObjectValue};
use ripel::{refs::resolve_and_build, sql::QueryExt};
use ripel::sql::AsQuery;
use sqlx::{MySqlPool};
use ripel::Entity;
use ripel::core::environment::default_env;


#[derive(Debug)]
enum Gender {
    Male,
    Female,
    Other,
} 

#[derive(Debug)]
enum Modality {
    ProfessionalAmateur,
    CanadaCup,
    Foursome,
    Greensome,
    Team,
    GrensomeChampan,
    Individual,
    FourBall,
    Scramble,
    Unknown,
} 

impl TryFrom<DynamicValue> for Gender {
    type Error = anyhow::Error;

    fn try_from(value: DynamicValue) -> std::result::Result<Self, Self::Error> {
        match value.as_str() {
            Some("masculino" )=> Ok(Gender::Male),
            Some("femenino") => Ok(Gender::Female),
            Some("otro") => Ok(Gender::Other),
            Some("sin especificar") => Ok(Gender::Other),
            _ => Err(anyhow::anyhow!("Invalid gender")),
        }
    }
}

impl TryFrom<DynamicValue> for Modality {
    type Error = anyhow::Error;

    fn try_from(value: DynamicValue) -> std::result::Result<Self, Self::Error> {
        match value.as_str() {
            Some("A") => Ok(Modality::ProfessionalAmateur),
            Some("C") => Ok(Modality::CanadaCup),
            Some("E") => Ok(Modality::Team),
            Some("F") => Ok(Modality::Foursome),
            Some("G") => Ok(Modality::Greensome),
            Some("H") => Ok(Modality::GrensomeChampan),
            Some("I") => Ok(Modality::Individual),
            Some("P") => Ok(Modality::FourBall),
            Some("S") => Ok(Modality::Scramble),
            Some("N") => Ok(Modality::Unknown),
            _ => Err(anyhow::anyhow!("Invalid modality")),
        }
    }
}

#[derive(Debug)]
pub struct Date(String);

impl TryFrom<DynamicValue> for Date {
    type Error = anyhow::Error;

    fn try_from(value: DynamicValue) -> std::result::Result<Self, Self::Error> {
        match value.as_str() {
            Some(s) => Ok(Date(s.to_string())),
            _ => Err(anyhow::anyhow!("Invalid date")),
        }
    }
}

#[derive(Entity, Debug)]
#[ripel(table_name = "Cliente")]
pub struct Client {
    #[ripel(template = "ulid('client' ~ id, created_at)")]
    #[ripel(primary_key)]
    id: ulid::Ulid,
    #[ripel(column = "nombre")]
    name: String,
    #[ripel(column = "nombre_corto")]
    slug: String,
    #[ripel(column = "cif")]
    tin: Option<String>,
    created_at: String,
    updated_at: String,
    /* #[ripel(column = "club_id", reference = "Club.id")]
    club_id: Option<ulid::Ulid>, */
}

#[derive(Entity, Debug)]
#[ripel(table_name = "Jugador")]
pub struct Player {
    #[ripel(template = "ulid('player' ~ id, created_at)")]
    #[ripel(primary_key)]
    id: ulid::Ulid,
    #[ripel(column = "nombre")]
    name: String,
    #[ripel(column = "licencia")]
    license: String,
    #[ripel(column = "sexo")]
    gender: Gender,
    #[ripel(column = "apellidos")]
    lastname: String,
    #[ripel(column = "fecha")]
    birthdate: Option<Date>,
    #[ripel(column = "email")]
    email: Option<String>,
    #[ripel(column = "tlfn")]
    phone: Option<String>,
    #[ripel(column = "hcp")]
    handicap: Option<String>,
    #[ripel(column = "Pais_id")]
    country: Option<String>,
    #[ripel(column = "Nivel_id")]
    level: Option<String>,
    created_at: String,
    updated_at: String,
    /* #[ripel(column = "club_id", reference = "Club.id")]
    club_id: Option<ulid::Ulid>, */
}

#[derive(Entity, Debug)]
#[ripel(table_name = "Club")]
pub struct Club {
    #[ripel(primary_key, template = "ulid('club' ~ id, created_at)")]
    id: ulid::Ulid,
    #[ripel(column = "nombre")]
    name: String,
    #[ripel(column = "telefono")]
    phone: Option<String>,
    #[ripel(column = "direccion")]
    address: Option<String>,
    #[ripel(column = "web")]
    website: Option<String>,
    #[ripel(reference = "Client.id", via = "Cliente(id=Cliente_id)")]
    client_id: ulid::Ulid,
    created_at: String,
    updated_at: String,
}

#[derive(Entity, Debug)]
#[ripel(table_name = "Trazado")]
pub struct Course {
    #[ripel(primary_key, template = "ulid('course' ~ id, created_at)")]
    id: ulid::Ulid,
    #[ripel(column = "numero")]
    number: i32,
    #[ripel(reference = "Club.id")]
    club_id: ulid::Ulid,
    created_at: String,
    updated_at: String,
}

#[derive(Entity, Debug)]
#[ripel(name ="Hole", table_name = "Hoyo")]
pub struct Hole {
    #[ripel(primary_key, template = "ulid('hole' ~ id, created_at)")]
    id: ulid::Ulid,
    #[ripel(column = "numero")]
    number: i32,
    // Hole has no direct relation in database with Course so we need to specify it manually
    #[ripel(reference = "Course.id", via = "TrazadoBarra(id=TrazadoBarra_id) -> Trazado(id=TrazadoBarra.Trazado_id)")]
    course_id: ulid::Ulid,
    #[ripel(column = "par")]
    par: u32,
    #[ripel(column = "hcp")]
    handicap: u32,
    /* #[ripel(column = "alias")]
    alias: Option<String>, */
    #[ripel(column = "created_at")]
    created_at: String,
    #[ripel(column = "updated_at")]
    updated_at: String,
}

#[derive(Entity, Debug)]
#[ripel(table_name = "TrazadoBarra")]
pub struct TeeBar {
    #[ripel(primary_key, template = "ulid('teebar' ~ id, created_at)")]
    id: ulid::Ulid,
    #[ripel(reference = "Hole.id", via = "Hoyo(TrazadoBarra_id=id)")]
    hole_id: ulid::Ulid,
    #[ripel(column = "sexo")]
    gender: Gender,
    #[ripel(column = "metros")]
    distance: u32,
    #[ripel(column = "Barra_id")]
    color: String,
}

#[derive(Entity, Debug)]
#[ripel(table_name = "Competicion")]
pub struct Tournament {
    #[ripel(primary_key, template = "ulid('competition' ~ id, created_at)")]
    id: ulid::Ulid,
    #[ripel(column = "nombre")]
    name: String,
    #[ripel(reference = "Club.id", via = "Club(id=Club_id)")]
    club_id: ulid::Ulid,
    #[ripel(reference = "Club.id", via = "Club(id=Organizador_id)")]
    organizer_id: ulid::Ulid,
    #[ripel(column = "estricto")]
    strict: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Entity, Debug)]
#[ripel(table_name = "Ronda")]
pub struct Round {
    #[ripel(primary_key, template = "ulid('round' ~ id, created_at)")]
    id: ulid::Ulid,
    #[ripel(reference = "Tournament.id", via = "Competicion(id=Competicion_id)")]
    tournament_id: ulid::Ulid,
    #[ripel(column = "inicio")]
    started_at: Option<String>,
    #[ripel(column = "fin")]
    finished_at: Option<String>,
    #[ripel(column = "orden")]
    ordinal: u32,
    #[ripel(column = "Modalidad_id")]
    modality: Modality,
    created_at: String,
    updated_at: String,
}

fn dv_to_string(v: &DynamicValue) -> Option<String> {
    Some(format!("{:#?}", v))
}

fn extract_row_key(row: &ObjectValue) -> Option<String> {
    // Prefer a stable synthetic/projection key if you have one
    if let Some(v) = row.get("__pk") {
        if let Some(s) = dv_to_string(v) { return Some(s); }
    }
    // Exact "id"
    if let Some(v) = row.get("id") {
        if let Some(s) = dv_to_string(v) { return Some(s); }
    }
    // Any field ending with ".id" or "_id" (case-insensitive)
    // NOTE: `iter()` assumed to yield (&str, &DynamicValue). Adjust if your API differs.
    for (k, v) in row.iter() {
        let k_l = k.to_ascii_lowercase();
        if k_l.ends_with(".id") || k_l.ends_with("_id") {
            if let Some(s) = dv_to_string(v) { return Some(s); }
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<()> {
    let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set")).await?;
    let env = default_env();

    let expr = std::env::args().nth(1).expect("expected query expression as first argument");
    let val = env.compile_expression(&expr)?.eval(())?;
    eprintln!("kind={:?} debug={:?}", val.kind(), val);
    let q  = val.as_query()?;

    let rows = q.fetch_all(&pool).await?;

    // Collect all failures to summarize at the end
    struct Failure {
        idx: usize,
        table: String,
        key: Option<String>,
        err: anyhow::Error,
    }
    let mut failures: Vec<Failure> = Vec::new();

    // Little helper macro to avoid repeating the same match arms
    macro_rules! try_build {
        ($ty:ty, $table:literal,  $idx:expr, $row:expr) => {{
            match resolve_and_build::<$ty>($row, &env, &pool).await {
                Ok(_entity) => { 
                    println!("[{}] {}: {:#?}", $idx, $table, _entity);
                 }
                Err(err) => {
                    failures.push(Failure {
                        idx: $idx,
                        table: $table.to_string(),
                        key: extract_row_key($row),
                        err,
                    });
                }
            }
        }};
    }

    for (idx, row) in rows.into_iter().enumerate() {
        match q.table_name() {
            "Hoyo"        => try_build!(Hole,        "Hoyo",        idx, &row),
            "Jugador"     => try_build!(Player,      "Jugador",     idx, &row),
            "Club"        => try_build!(Club,        "Club",        idx, &row),
            "Trazado"     => try_build!(Course,      "Trazado",     idx, &row),
            "TrazadoBarra"=> try_build!(TeeBar,      "TrazadoBarra",idx, &row),
            "Cliente"     => try_build!(Client,      "Cliente",     idx, &row),
            "Competicion" => try_build!(Tournament,  "Competicion", idx, &row),
            "Ronda"       => try_build!(Round,       "Ronda",       idx, &row),
            other         => {
                eprintln!("Unknown table: {}", other);
            }
        }
    }

    // Print a clear summary
    if !failures.is_empty() {
        eprintln!("\n===== Parse failures ({}) =====", failures.len());
        for f in &failures {
            let k = f.key.as_deref().unwrap_or("-");
            eprintln!("[{}] table={} key={} error:\n  {:#}", f.idx, f.table, k, f.err);
        }
        // If you still want a failing exit status:
        // anyhow::bail!("{} rows failed to parse", failures.len());
    }

    Ok(())
}

