use color_eyre::eyre::Ok;
use indexmap::IndexMap;
use polars::prelude::*;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePool;
use std::num::NonZeroUsize;
use std::path::Path;
use tokio::runtime::Runtime;
#[derive(Debug, Clone, PartialEq, Default)]
pub enum SqliteDataType {
    INTEGER,
    #[default]
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
    auto_increment: bool,
    default: Option<String>,
    foreing_key: Option<ForeinKey>,
}
impl Default for SqliteColOption {
    fn default() -> Self {
        Self {
            type_sql: SqliteDataType::default(),
            nullable: true,
            primary_key: false,
            unique: false,
            auto_increment: false,
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
    pub fn with_auto_increment(mut self, auto_increment: bool) -> Self {
        self.auto_increment = auto_increment;
        self
    }
    pub fn foreign_key<T: Into<String>>(mut self, table: T, column: T) -> Self {
        self.foreing_key = Some(ForeinKey::new(table.into(), column.into()));
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
        if self.auto_increment {
            col_def.push_str(" AUTOINCREMENT");
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
    pub fn with_column<T: Into<String>>(&mut self, column: T, type_of: SqliteColOption) -> &Self {
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
#[derive(Clone, Default)]
pub enum IfExistsOption {
    #[default]
    Fail,
    Replace,
    Append,
}

#[derive(Clone)]
pub struct SqlWriter {
    pool: SqlitePool,
    table_name: Option<String>,
    if_exists: IfExistsOption,
    index: bool,
    parallel: bool,
    batch_size: NonZeroUsize,
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
            if_exists: IfExistsOption::default(),
            index: true,
            parallel: true,
            batch_size: NonZeroUsize::new(1024).unwrap(),
            index_label: None,
            table_name: None,
            schema: None,
        })
    }
    pub fn with_table<T: Into<String>>(mut self, table_name: Option<T>) -> Self {
        self.table_name = table_name.map(|t| t.into());
        self
    }
    pub fn if_exists(mut self, if_exists: IfExistsOption) -> Self {
        self.if_exists = if_exists;
        self
    }
    pub fn with_index(mut self, index: bool) -> Self {
        self.index = index;
        if index {
            self.index_label = Some("Id".into());
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
    pub fn with_batch_size(mut self, batch_size: NonZeroUsize) -> Self {
        self.batch_size = batch_size;
        self
    }
    pub fn with_parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }
    pub fn finish(&mut self, df: &mut DataFrame) -> Result<(), color_eyre::eyre::Error> {
        // Delete table and if create the schema
        let table_name = match self.table_name.as_ref() {
            Some(t) => t.clone(),
            None => "test".to_string(),
        };
        let rt = Runtime::new()?;
        let mut schema = SqliteSchema::from_polars_schema(df.schema());
        self.index_label = if self.index && self.index_label.is_none() {
            Some("Id".into())
        } else {
            None
        };
        if let Some(index) = self.index_label.as_ref() {
            if df.get_column_names_str().contains(&index.as_str()) {
                return Err(color_eyre::eyre::eyre!(
                    "Column {} already exists try other name in index",
                    index
                ));
            }
            schema = SqliteSchema::new(
                index,
                SqliteColOption::default()
                    .with_type_sql(SqliteDataType::INTEGER)
                    .with_auto_increment(true),
            )
            .add_schema(&schema);
        };
        if self.schema.clone().is_some_and(|s| s != schema) {
            for (colums, types) in self.schema.clone().unwrap().iter_fields() {
                if schema.iter_columns().any(|c| c == colums) {
                    schema.with_column(colums, types.clone());
                }
            }
        }
        let qry = schema.finish(&table_name);
        match self.if_exists {
            IfExistsOption::Append => {}
            IfExistsOption::Replace => {
                rt.block_on(
                    sqlx::query(&format!("DROP TABLE IF EXISTS {}", table_name))
                        .execute(&self.pool),
                )?;
            }
            IfExistsOption::Fail => {
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
                            _ => format!("'{}'", value.to_string().replace("'", "''")),
                        })
                        .collect::<Vec<String>>()
                        .join(","),
                ),
                None => None,
            }
        };
        if df.height() < self.batch_size.into() {
            self.batch_size =
                NonZeroUsize::new(df.height()).unwrap_or(NonZeroUsize::new(1).unwrap());
        }
        for dfs in df
            .to_owned()
            .split_chunks_by_n(self.batch_size.into(), true)
        {
            let mut row_sql = Vec::new();
            for i in 0..dfs.height() {
                let row = dfs.get(i);
                let row = generate_insert_qry(row);
                if let Some(row) = row {
                    row_sql.push(format!("({})", row));
                }
            }
            let full_insert = format!(
                "INSERT INTO {} ({}) VALUES {}",
                table_name,
                dfs.get_column_names_str().join(","),
                row_sql.join(",")
            );
            rt.block_on(sqlx::query(&full_insert).execute(&self.pool))?;
        }

        Ok(())
    }
}
