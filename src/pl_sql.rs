use color_eyre::eyre::Ok;
use indexmap::IndexMap;
use polars::prelude::*;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePool;
use std::path::Path;
use tokio::runtime::Runtime;
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteDataType {
    INTEGER,
    TEXT,
    REAL,
    BLOB,
    NUMERIC(Option<usize>, Option<usize>),
}
impl std::fmt::Display for SqliteDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqliteDataType::INTEGER => write!(f, "INTEGER"),
            SqliteDataType::TEXT => write!(f, "TEXT"),
            SqliteDataType::REAL => write!(f, "REAL"),
            SqliteDataType::BLOB => write!(f, "BLOB"),
            SqliteDataType::NUMERIC(p, s) => match (p, s) {
                (Some(p), Some(s)) => write!(f, "NUMERIC({}, {})", p, s),
                (Some(p), None) => write!(f, "NUMERIC({})", p),
                _ => write!(f, "NUMERIC"),
            },
        }
    }
}
impl SqliteDataType {
    pub fn from_polar_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => SqliteDataType::INTEGER,
            DataType::UInt8 => SqliteDataType::INTEGER,
            DataType::UInt16 => SqliteDataType::INTEGER,
            DataType::UInt32 => SqliteDataType::INTEGER,
            DataType::UInt64 => SqliteDataType::INTEGER,
            DataType::Int8 => SqliteDataType::INTEGER,
            DataType::Int16 => SqliteDataType::INTEGER,
            DataType::Int32 => SqliteDataType::INTEGER,
            DataType::Int64 => SqliteDataType::INTEGER,
            DataType::Int128 => SqliteDataType::INTEGER,
            DataType::Float32 => SqliteDataType::REAL,
            DataType::Float64 => SqliteDataType::REAL,
            DataType::Decimal(p, s) => SqliteDataType::NUMERIC(*p, *s),
            DataType::Binary => SqliteDataType::BLOB,
            DataType::BinaryOffset => SqliteDataType::BLOB,
            _ => SqliteDataType::TEXT,
        }
    }
}
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ForeinKey {
    table: String,
    column: String,
}
impl ForeinKey {
    pub fn new<T: Into<String>>(table: T, column: T) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
        }
    }
}
#[derive(Clone, PartialEq)]
pub struct SqliteColOption {
    type_sql: SqliteDataType,
    nullable: bool,
    primary_key: bool,
    unique: bool,
    default: Option<String>,
    foreing_key: Option<ForeinKey>,
}
impl Default for SqliteColOption {
    fn default() -> Self {
        Self {
            type_sql: SqliteDataType::TEXT,
            nullable: true,
            primary_key: false,
            unique: false,
            default: None,
            foreing_key: None,
        }
    }
}
impl SqliteColOption {
    pub fn with_type_sql(mut self, type_sql: SqliteDataType) -> Self {
        self.type_sql = type_sql;
        self
    }
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = if self.primary_key { false } else { nullable };
        self
    }
    pub fn with_primary_key(mut self, primary_key: bool) -> Self {
        self.primary_key = primary_key;
        self.nullable = false;
        self
    }
    pub fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }
    pub fn with_default<T: Into<String>>(mut self, default: Option<T>) -> Self {
        self.default = if self.primary_key {
            None
        } else {
            default.map(|d| d.into())
        };
        self
    }
    pub fn foreign_key(mut self, table: String, column: String) -> Self {
        self.foreing_key = Some(ForeinKey::new(table, column));
        self
    }
    pub fn build_col_def<T: Into<String>>(&self, column_name: T) -> String {
        let mut col_def = format!("{} {}", column_name.into(), self.type_sql);
        if self.primary_key {
            col_def.push_str(" PRIMARY KEY");
        }
        if self.unique {
            col_def.push_str(" UNIQUE");
        }
        if let Some(ref default) = self.default {
            col_def.push_str(&format!(" DEFAULT {}", default));
        }
        if !self.nullable {
            col_def.push_str(" NOT NULL");
        }
        col_def
    }
}
#[derive(Clone, PartialEq, Default)]
pub struct SqliteSchema {
    columns: IndexMap<String, SqliteColOption>,
}
impl SqliteSchema {
    pub fn new<T: Into<String>>(col_name: T, col_type: SqliteColOption) -> Self {
        let mut columns: IndexMap<String, SqliteColOption> = IndexMap::new();
        columns.insert(col_name.into(), col_type);
        Self { columns }
    }
    pub fn from_polars_schema(schema: &Schema) -> Self {
        let mut columns: IndexMap<String, SqliteColOption> = IndexMap::new();

        for field in schema.iter_fields() {
            let type_of = SqliteDataType::from_polar_type(field.dtype());
            columns.insert(
                field.name().to_string(),
                SqliteColOption::default().with_type_sql(type_of).clone(),
            );
        }
        Self { columns }
    }
    pub fn add_schema(mut self, other: &SqliteSchema) -> Self {
        self.columns
            .extend(other.iter_fields().map(|(c, t)| (c.clone(), t.clone())));
        self
    }
    pub fn iter_fields(&self) -> impl Iterator<Item = (&String, &SqliteColOption)> {
        self.columns.iter()
    }
    pub fn iter_columns(&self) -> impl Iterator<Item = &String> {
        self.columns.keys()
    }
    pub fn iter_types(&self) -> impl Iterator<Item = &SqliteColOption> {
        self.columns.values()
    }
    pub fn with_column<T: Into<String>>(mut self, column: T, type_of: SqliteColOption) -> Self {
        self.columns.insert(column.into(), type_of);
        self
    }
    pub fn finish<T: Into<String>>(&mut self, table_name: T) -> String {
        let mut col_definitions = Vec::new();
        let mut foreign_keys = Vec::new();

        for (column, options) in &self.columns {
            col_definitions.push(options.build_col_def(column));
            if let Some(fk) = &options.foreing_key {
                foreign_keys.push(format!(
                    "FOREIGN KEY ({}) REFERENCES {}({})",
                    column, fk.table, fk.column
                ));
            }
        }

        if !foreign_keys.is_empty() {
            col_definitions.extend(foreign_keys);
        }

        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    {}\n);",
            table_name.into(),
            col_definitions.join(",\n    ")
        )
    }
}

#[derive(Clone)]
pub struct SqlWriter {
    pool: SqlitePool,
    table_name: Option<String>,
    if_exists: Option<String>,
    index: bool,
    index_label: Option<String>,
    schema: Option<SqliteSchema>,
}
impl SqlWriter {
    pub fn new<P: AsRef<Path>>(db_url: P) -> Result<Self, color_eyre::eyre::Error> {
        let rt = Runtime::new()?;
        let options = SqliteConnectOptions::new()
            .filename(db_url)
            .create_if_missing(true);
        let pool = rt.block_on(SqlitePool::connect_with(options))?;
        Ok(SqlWriter {
            pool,
            if_exists: None,
            index: true,
            index_label: None,
            table_name: None,
            schema: None,
        })
    }
    pub fn with_table<T: Into<String>>(mut self, table_name: Option<T>) -> Self {
        self.table_name = table_name.map(|t| t.into());
        self
    }
    pub fn if_exists<T: Into<String> + std::fmt::Display + std::marker::Copy>(
        mut self,
        if_exists: Option<T>,
    ) -> Result<Self, color_eyre::eyre::Error> {
        match if_exists {
            Some(t) => match t.to_string().as_str() {
                "replace" => self.if_exists = Some("replace".into()),
                "fail" => {
                    let rt = Runtime::new().unwrap();
                    let table = self
                        .table_name
                        .as_ref()
                        .unwrap_or(&"test".to_string())
                        .clone();
                    let rows = rt.block_on(
                        sqlx::query("SELECT name FROM sqlite_master WHERE name = ?")
                            .bind(&table)
                            .fetch_all(&self.pool),
                    )?;
                    if rows.is_empty() {
                        return Ok(self);
                    } else {
                        return Err(color_eyre::eyre::eyre!("Table {} already exists", table));
                    }
                }
                "append" => self.if_exists = Some("append".into()),
                _ => {
                    return Err(color_eyre::eyre::eyre!("Invalid if_exists option: {}", t));
                }
            },
            None => self.if_exists = None,
        }

        Ok(self)
    }
    pub fn with_index(mut self, index: bool) -> Self {
        self.index = index;
        if index {
            self.index_label = Some("Index".into());
        }
        self
    }
    pub fn with_schema(mut self, schema: Option<SqliteSchema>) -> Self {
        self.schema = schema;
        self
    }
    pub fn with_index_label<T: Into<String>>(mut self, index_label: Option<T>) -> Self {
        self.index_label = index_label.map(|t| t.into());
        self.index = true;
        self
    }
    pub fn finish(&mut self, df: &DataFrame) -> Result<(), color_eyre::eyre::Error> {
        // Delete table and if create the schema
        let table_name = match self.table_name.as_ref() {
            Some(t) => t.clone(),
            None => "test".to_string(),
        };
        let if_exists = match self.if_exists.as_ref() {
            Some(t) => t.clone(),
            None => "fail".into(),
        };
        let rt = Runtime::new()?;
        let mut schema = SqliteSchema::from_polars_schema(df.schema());
        if let Some(index) = self.index_label.as_ref() {
            if df.get_column_names_str().contains(&index.as_str()) {
                return Err(color_eyre::eyre::eyre!(
                    "Column {} already exists try other name in index",
                    index
                ));
            }
            schema = SqliteSchema::new(
                index,
                SqliteColOption::default().with_type_sql(SqliteDataType::INTEGER),
            )
            .add_schema(&schema);
        };
        if self.schema.clone().is_some_and(|s| s != schema) {
            for (colums, types) in self.schema.clone().unwrap().iter_fields() {
                if schema.iter_columns().any(|c| c == colums) {
                    schema = schema.with_column(colums, types.clone());
                }
            }
        }
        let qry = schema.finish(&table_name);
        if if_exists == "replace" {
            rt.block_on(
                sqlx::query(&format!("DROP TABLE IF EXISTS {}", table_name)).execute(&self.pool),
            )?;
        } else if if_exists == "fail" {
            let table = self
                .table_name
                .as_ref()
                .unwrap_or(&"test".to_string())
                .clone();
            let rows = rt.block_on(
                sqlx::query("SELECT name FROM sqlite_master WHERE name = ?")
                    .bind(&table)
                    .fetch_all(&self.pool),
            )?;
            if !rows.is_empty() {
                return Err(color_eyre::eyre::eyre!("Table {} already exists", table));
            }
        }
        rt.block_on(sqlx::query(&qry).execute(&self.pool))?;
        let generate_insert_qry = |row: Option<Vec<AnyValue>>| -> Option<String> {
            match row {
                Some(r) => Some(
                    r.iter()
                        .map(|value| match value {
                            AnyValue::Null => "NULL".to_string(),
                            AnyValue::Boolean(v) => {
                                if *v {
                                    "1".to_string()
                                } else {
                                    "0".to_string()
                                }
                            }
                            AnyValue::String(v) => format!("'{}'", v.replace("'", "''")),
                            AnyValue::Int8(v) => v.to_string(),
                            AnyValue::Int16(v) => v.to_string(),
                            AnyValue::Int32(v) => v.to_string(),
                            AnyValue::Int128(v) => v.to_string(),
                            AnyValue::Float64(v) => v.to_string(),
                            AnyValue::Decimal(i, d) => format!("{}.{}", i, d),
                            _ => format!("'{}'", value).replace("'", "''"),
                        })
                        .collect::<Vec<String>>()
                        .join(","),
                ),
                None => None,
            }
        };
        let qyr_insert = if self.index {
            format!("INSERT INTO {} VALUES ('index','row')", table_name)
        } else {
            format!("INSERT INTO {} VALUES ('row')", table_name)
        };
        (0..df.height()).for_each(|i| {
            let row = df.get(i);
            let row = generate_insert_qry(row);
            if let Some(row) = row {
                let qry = qyr_insert
                    .replace("'index'", &i.to_string())
                    .replace("'row'", &row);
                let _ = rt.block_on(sqlx::query(&qry).execute(&self.pool));
            }
        });

        Ok(())
    }
}
