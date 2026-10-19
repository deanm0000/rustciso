//! Snapshot-preserving compaction. Intermediate sorted runs live in blob storage.
use super::*;
use chrono::{Months, NaiveDate, TimeZone};
use chrono_tz::America::Los_Angeles;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array};
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::datatypes::SchemaRef;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

mod sort;

const ROW_ID: &str = "_ducklake_internal_row_id";
const SNAPSHOT_ID: &str = "_ducklake_internal_snapshot_id";
const ROW_ID_FIELD: &str = "2147483540";
const SNAPSHOT_FIELD: &str = "2147483539";

fn progress(log: &mut Vec<String>, message: String) {
    eprintln!("repack: {message}");
    log.push(message);
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub table: String,
    pub begin_date: NaiveDate,
    pub end_date: NaiveDate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<String>>,
}

impl Job {
    pub fn validate(&self) -> Result<()> {
        if let Some(unique) = &self.unique {
            if unique.is_empty() {
                bail!("unique must contain at least one column");
            }
            let mut seen = HashSet::new();
            for name in unique {
                if name.is_empty() || !seen.insert(name) {
                    bail!("unique columns must be nonempty and distinct");
                }
            }
        }
        let parts: Vec<_> = self.table.split('.').collect();
        if parts.len() > 2
            || parts
                .iter()
                .any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'))
        {
            bail!(
                "table must be a table name or schema.table using letters, digits and underscores"
            );
        }
        if self.begin_date >= self.end_date {
            bail!("begin_date must precede end_date (exclusive)");
        }
        self.begin_date
            .checked_add_months(Months::new(1))
            .context("date is out of range")?;
        self.start()?;
        self.end()?;
        Ok(())
    }

    fn validate_schema(&self, schema: &SchemaRef) -> Result<()> {
        if let Some(unique) = &self.unique {
            for name in unique {
                schema
                    .index_of(name)
                    .with_context(|| format!("unknown unique column {name:?}"))?;
            }
            if !matches!(
                schema.field_with_name("downloaded_at")?.data_type(),
                DataType::Timestamp(TimeUnit::Microsecond, _)
            ) {
                bail!("deduplication requires microsecond timestamptz downloaded_at");
            }
        }
        Ok(())
    }

    fn names(&self) -> (&str, &str) {
        self.table.split_once('.').unwrap_or((SCHEMA, &self.table))
    }

    fn start(&self) -> Result<DateTime<Utc>> {
        Self::local_midnight(self.begin_date)
    }

    fn end(&self) -> Result<DateTime<Utc>> {
        Self::local_midnight(self.end_date)
    }

    fn local_midnight(date: NaiveDate) -> Result<DateTime<Utc>> {
        let midnight = date.and_hms_opt(0, 0, 0).context("invalid midnight")?;
        Los_Angeles
            .from_local_datetime(&midnight)
            .single()
            .with_context(|| {
                format!("{date} midnight is ambiguous or nonexistent in America/Los_Angeles")
            })
            .map(|time| time.with_timezone(&Utc))
    }

    fn node_only(&self) -> bool {
        self.begin_date
            .checked_add_months(Months::new(1))
            .is_some_and(|d| self.end_date >= d)
    }
}

struct Work {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    tmp: String,
    temps: HashSet<ObjPath>,
    outputs: Vec<ObjPath>,
}

impl Work {
    fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        let tmp = format!("{prefix}_tmp/repack-{}/", Uuid::now_v7());
        Self {
            store,
            prefix,
            tmp,
            temps: HashSet::new(),
            outputs: Vec::new(),
        }
    }

    fn temp_path(&mut self) -> ObjPath {
        let path = ObjPath::from(format!("{}{}.parquet", self.tmp, Uuid::now_v7()));
        self.temps.insert(path.clone());
        path
    }

    async fn remove_temp(&mut self, path: &ObjPath) -> Result<()> {
        self.store.delete(path).await?;
        self.temps.remove(path);
        Ok(())
    }

    async fn cleanup(&mut self, log: &mut Vec<String>) {
        for path in self.temps.iter().chain(&self.outputs) {
            if let Err(e) = self.store.delete(path).await {
                progress(log, format!("could not clean up {path}: {e}"));
            }
        }
    }
}

#[derive(Debug, PartialEq)]
struct Source {
    id: i64,
    path: String,
    relative: bool,
    rows: i64,
    row_id_start: Option<i64>,
    begin_snapshot: i64,
    delete: Option<DeleteFile>,
}

#[derive(Debug, PartialEq)]
struct DeleteFile {
    id: i64,
    path: String,
    relative: bool,
    count: i64,
}

async fn sources(
    c: &impl GenericClient,
    table_id: i64,
    utc_col: i64,
    job: &Job,
) -> Result<Vec<Source>> {
    let rows = c.query(
        "SELECT f.data_file_id, f.path, f.path_is_relative, f.record_count, f.row_id_start,
                f.begin_snapshot, f.file_format, f.mapping_id, f.encryption_key, f.partial_max,
                d.delete_file_id, d.path, d.path_is_relative, d.delete_count, d.format,
                d.encryption_key, d.partial_max
           FROM ducklake.ducklake_data_file f
           LEFT JOIN ducklake.ducklake_file_column_stats s
             ON s.table_id = f.table_id AND s.data_file_id = f.data_file_id AND s.column_id = $2
           LEFT JOIN ducklake.ducklake_delete_file d
             ON d.table_id = f.table_id AND d.data_file_id = f.data_file_id AND d.end_snapshot IS NULL
          WHERE f.table_id = $1 AND f.end_snapshot IS NULL
            AND (s.min_value IS NULL OR s.max_value IS NULL OR s.null_count > 0
                 OR (s.min_value::timestamptz < $4 AND s.max_value::timestamptz >= $3))
          ORDER BY f.data_file_id",
        &[&table_id, &utc_col, &job.start()?, &job.end()?]
    ).await.context("reading repack source files")?;
    let mut out = Vec::new();
    for r in rows {
        let id: i64 = r.get(0);
        if r.get::<_, String>(6) != "parquet"
            || r.get::<_, Option<i64>>(7).is_some()
            || r.get::<_, Option<String>>(8).is_some()
            || r.get::<_, Option<i64>>(9).is_some()
        {
            bail!(
                "source file {id}: mapped, encrypted, partial or non-Parquet files are not supported"
            );
        }
        let delete = if let Some(delete_id) = r.get::<_, Option<i64>>(10) {
            if r.get::<_, Option<String>>(14).as_deref() != Some("parquet")
                || r.get::<_, Option<String>>(15).is_some()
                || r.get::<_, Option<i64>>(16).is_some()
            {
                bail!(
                    "delete file {delete_id}: only unencrypted, non-partial Parquet positional deletes are supported"
                );
            }
            Some(DeleteFile {
                id: delete_id,
                path: r.get(11),
                relative: r.get::<_, Option<bool>>(12).unwrap_or(false),
                count: r.get(13),
            })
        } else {
            None
        };
        if out.last().is_some_and(|s: &Source| s.id == id) {
            bail!("multiple active delete files for source {id}");
        }
        out.push(Source {
            id,
            path: r.get(1),
            relative: r.get::<_, Option<bool>>(2).unwrap_or(false),
            rows: r.get(3),
            row_id_start: r.get(4),
            begin_snapshot: r.get(5),
            delete,
        });
    }
    Ok(out)
}

fn schema(meta: &TableMeta) -> Result<SchemaRef> {
    let mut cols: Vec<_> = meta.columns.iter().collect();
    cols.sort_by_key(|(_, c)| c.id);
    let fields = cols
        .into_iter()
        .map(|(name, c)| {
            let ty = match c.ty.as_str() {
                "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "varchar" => DataType::Utf8,
                "float64" => DataType::Float64,
                ty => bail!("unsupported repack column type {ty} for {name}"),
            };
            Ok(Field::new(name, ty, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.into(),
                c.id.to_string(),
            )])))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(fields));
    if schema.field_with_name("node")?.data_type() != &DataType::Utf8
        || !matches!(
            schema.field_with_name("utcbegin")?.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        )
    {
        bail!("repack requires varchar node and microsecond timestamptz utcbegin");
    }
    Ok(schema)
}

fn with_identity(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<_> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    for (name, id) in [(ROW_ID, ROW_ID_FIELD), (SNAPSHOT_ID, SNAPSHOT_FIELD)] {
        fields.push(
            Field::new(name, DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.into(),
                id.into(),
            )])),
        );
    }
    Arc::new(Schema::new(fields))
}

fn normalize(
    batch: &RecordBatch,
    target: &SchemaRef,
    source: &Source,
    offset: i64,
) -> Result<RecordBatch> {
    let input = batch.schema();
    let mut columns = Vec::new();
    for field in target.fields() {
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .context("missing target field ID")?;
        let found = input
            .fields()
            .iter()
            .position(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY) == Some(id));
        let array: ArrayRef = if let Some(idx) = found {
            let array = batch.column(idx);
            if array.data_type() != field.data_type() {
                bail!(
                    "source {} field {} has incompatible type {}",
                    source.id,
                    field.name(),
                    array.data_type()
                );
            }
            array.clone()
        } else if field.name() == ROW_ID {
            let base = source
                .row_id_start
                .context("source has neither row_id_start nor embedded row IDs")?;
            let start = base.checked_add(offset).context("row ID overflow")?;
            let end = start
                .checked_add(batch.num_rows() as i64)
                .context("row ID overflow")?;
            Arc::new(Int64Array::from_iter_values(start..end))
        } else if field.name() == SNAPSHOT_ID {
            Arc::new(Int64Array::from_value(
                source.begin_snapshot,
                batch.num_rows(),
            ))
        } else {
            bail!(
                "source {} is missing field ID {id} ({})",
                source.id,
                field.name()
            );
        };
        columns.push(array);
    }
    let batch = RecordBatch::try_new(target.clone(), columns)?;
    for name in ["node", "utcbegin", ROW_ID, SNAPSHOT_ID] {
        if batch.column(target.index_of(name)?).null_count() > 0 {
            bail!(
                "source {} contains null {name}; cannot safely repack",
                source.id
            );
        }
    }
    Ok(batch)
}

fn split(batch: &RecordBatch, job: &Job) -> Result<[RecordBatch; 3]> {
    let times = batch
        .column(batch.schema().index_of("utcbegin")?)
        .as_primitive::<TimestampMicrosecondType>();
    let start = job.start()?.timestamp_micros();
    let end = job.end()?.timestamp_micros();
    let regions: Vec<_> = times
        .values()
        .iter()
        .map(|t| {
            if *t < start {
                0
            } else if *t < end {
                1
            } else {
                2
            }
        })
        .collect();
    let mut batches = Vec::new();
    for region in 0..3 {
        batches.push(filter_record_batch(
            batch,
            &BooleanArray::from(regions.iter().map(|r| *r == region).collect::<Vec<_>>()),
        )?);
    }
    batches
        .try_into()
        .map_err(|_| anyhow!("expected three date regions"))
}

fn location(meta: &TableMeta, path: &str, relative: bool) -> String {
    if relative {
        format!("{}{path}", meta.root)
    } else {
        path.into()
    }
}

struct Deletes {
    stream: Option<
        datafusion::parquet::arrow::async_reader::ParquetRecordBatchStream<sort::BlobReader>,
    >,
    positions: Vec<i64>,
    index: usize,
    previous: Option<i64>,
    count: i64,
    expected: i64,
    source_rows: i64,
}

impl Deletes {
    async fn open(cfg: &Config, meta: &TableMeta, source: &Source) -> Result<Self> {
        let stream = if let Some(delete) = &source.delete {
            let full = location(meta, &delete.path, delete.relative);
            let (store, path) = object_store_for(cfg, &full)?;
            Some(sort::reader(store, ObjPath::from(path)).await?)
        } else {
            None
        };
        Ok(Self {
            stream,
            positions: Vec::new(),
            index: 0,
            previous: None,
            count: 0,
            expected: source.delete.as_ref().map_or(0, |d| d.count),
            source_rows: source.rows,
        })
    }

    async fn peek(&mut self) -> Result<Option<i64>> {
        while self.index == self.positions.len() {
            let Some(stream) = &mut self.stream else {
                return Ok(None);
            };
            let Some(batch) = stream.try_next().await? else {
                if self.count != self.expected {
                    bail!("delete count mismatch");
                }
                self.stream = None;
                return Ok(None);
            };
            let pos = cast(
                batch.column(batch.schema().index_of("pos")?),
                &DataType::Int64,
            )?;
            self.positions.clear();
            self.index = 0;
            for pos in pos
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .iter()
            {
                let pos = pos.context("null delete position")?;
                if pos < 0 || pos >= self.source_rows || self.previous.is_some_and(|p| pos <= p) {
                    bail!(
                        "delete positions must be unique, ascending and within source row bounds"
                    );
                }
                self.previous = Some(pos);
                self.positions.push(pos);
                self.count += 1;
            }
        }
        Ok(Some(self.positions[self.index]))
    }

    async fn mask(&mut self, offset: i64, rows: usize) -> Result<BooleanArray> {
        let mut keep = vec![true; rows];
        while let Some(pos) = self.peek().await? {
            if pos >= offset + rows as i64 {
                break;
            }
            if pos < offset {
                bail!("delete cursor fell behind source rows");
            }
            keep[(pos - offset) as usize] = false;
            self.index += 1;
        }
        Ok(BooleanArray::from(keep))
    }
}

async fn prepare(
    cfg: &Config,
    meta: &TableMeta,
    sources: &[Source],
    job: &Job,
    work: &mut Work,
    log: &mut Vec<String>,
) -> Result<Vec<FileInfo>> {
    let data_schema = schema(meta)?;
    job.validate_schema(&data_schema)?;
    let target = with_identity(&data_schema);
    let cols = resolve_columns(meta, &data_schema)?;
    let mut runs = [sort::Runs::new(), sort::Runs::new(), sort::Runs::new()];
    if let Some(unique) = &job.unique {
        runs[1] = sort::Runs::for_unique(unique);
    }
    let mut live_rows = 0i64;
    for (index, source) in sources.iter().enumerate() {
        let started = std::time::Instant::now();
        progress(log, format!(
            "reading source {}/{}: id={}, rows={}, deletes={}",
            index + 1, sources.len(), source.id, source.rows,
            source.delete.as_ref().map_or(0, |d| d.count)
        ));
        let mut deleted = Deletes::open(cfg, meta, source).await
            .with_context(|| format!("opening deletes for source {}", source.id))?;
        let full = location(meta, &source.path, source.relative);
        let (store, path) = object_store_for(cfg, &full)?;
        let mut stream = sort::reader(store, ObjPath::from(path)).await
            .with_context(|| format!("opening source {} at {full}", source.id))?;
        let mut offset = 0i64;
        while let Some(batch) = stream.try_next().await
            .with_context(|| format!("reading source {} at row {offset}", source.id))? {
            let normalized = normalize(&batch, &target, source, offset)?;
            let keep = deleted.mask(offset, batch.num_rows()).await?;
            offset += batch.num_rows() as i64;
            let batch = filter_record_batch(&normalized, &keep)?;
            live_rows += batch.num_rows() as i64;
            for (run, batch) in runs.iter_mut().zip(split(&batch, job)?) {
                run.push(batch, work).await?;
            }
        }
        if offset != source.rows {
            bail!("record count mismatch for source {}", source.id);
        }
        if deleted.peek().await?.is_some() {
            bail!("unconsumed delete positions for source {}", source.id);
        }
        progress(log, format!(
            "read source {}: {offset} rows in {:.1}s; total live rows={live_rows}",
            source.id, started.elapsed().as_secs_f64()
        ));
    }
    let mut files = Vec::new();
    let mut removed = 0i64;
    for (i, run) in runs.into_iter().enumerate() {
        progress(log, format!("merging {} region", ["before", "in-range", "after"][i]));
        let run = if i == 1 && let Some(unique) = &job.unique {
            let (run, count) = run.deduplicate(work, unique).await?;
            removed = count;
            progress(log, format!("deduplicated by {unique:?}: removed {removed} rows"));
            run
        } else {
            run
        };
        if let Some(file) = run.finish(work, i != 1 || job.node_only(), &cols).await? {
            progress(log, format!(
                "{}: {} rows, {} bytes in {}",
                ["before", "in-range", "after"][i],
                file.rows,
                file.size,
                file.name
            ));
            files.push(file);
        }
    }
    if files.iter().map(|f| f.rows).sum::<i64>() != live_rows - removed {
        bail!("repack row count mismatch");
    }
    Ok(files)
}

async fn lock_table(client: &Client, job: &Job) -> Result<()> {
    let (schema, table) = job.names();
    let key = format!("rustciso.repack:{schema}.{table}");
    // Session-scoped so sorting and all commit retries stay protected without
    // holding an idle transaction. The owning connection releases it on exit.
    client
        .query_one("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&key])
        .await
        .context("acquiring repack advisory lock")?;
    Ok(())
}

async fn has_current_files(c: &impl GenericClient, table_id: i64, job: &Job) -> Result<bool> {
    let Some(files) = &job.files else {
        return Ok(true);
    };
    Ok(c
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM ducklake.ducklake_data_file
                 WHERE table_id = $1 AND end_snapshot IS NULL AND path = ANY($2)
            )",
            &[&table_id, files],
        )
        .await
        .context("checking repack files in current snapshot")?
        .try_get(0)?)
}

async fn check_inlined(c: &impl GenericClient, table_id: i64, job: &Job) -> Result<()> {
    if has_inlined_rows(c, table_id, job.start()?, job.end()?).await? {
        bail!("range contains inlined data; flush it to Parquet before repacking");
    }
    Ok(())
}

async fn commit(
    client: &mut Client,
    meta: &TableMeta,
    utc_col: i64,
    selected: &[Source],
    job: &Job,
    files: &[FileInfo],
) -> Result<i64> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let snap = tx
        .query_one(
            "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
         FROM ducklake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1",
            &[],
        )
        .await?;
    let (prev, version, next_catalog, next_file): (i64, i64, i64, i64) =
        (snap.get(0), snap.get(1), snap.get(2), snap.get(3));
    let current = sources(&tx, meta.table_id, utc_col, job).await?;
    if current != selected {
        bail!("repack sources changed while sorting; retry the job");
    }
    check_inlined(&tx, meta.table_id, job).await?;
    // A schema change can leave the same files active. Reject rather than publish stale field IDs.
    let (schema_name, table_name) = job.names();
    if table_meta(&tx, schema_name, table_name).await? != *meta {
        bail!("table metadata changed while sorting; retry the job");
    }
    let removed = selected
        .iter()
        .map(|s| s.rows - s.delete.as_ref().map_or(0, |d| d.count))
        .sum::<i64>()
        - files.iter().map(|f| f.rows).sum::<i64>();
    if removed < 0 || (job.unique.is_none() && removed != 0) {
        bail!("invalid repack logical row count change: {removed}");
    }
    let snapshot_id = prev + 1;
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot
         (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
         VALUES ($1, now(), $2, $3, $4)",
        &[
            &snapshot_id,
            &version,
            &next_catalog,
            &(next_file + files.len() as i64),
        ],
    )
    .await?;
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot_changes
         (snapshot_id, changes_made, author, commit_message, commit_extra_info)
         VALUES ($1, $2, $3, $4, NULL)",
        &[
            &snapshot_id,
            &if removed > 0 {
                format!("compacted_table:{0},deleted_from_table:{0}", meta.table_id)
            } else {
                format!("compacted_table:{}", meta.table_id)
            },
            &AUTHOR,
            &format!(
                "repack {} [{}, {}) America/Los_Angeles",
                job.table, job.begin_date, job.end_date
            ),
        ],
    )
    .await?;
    let ids: Vec<_> = selected.iter().map(|s| s.id).collect();
    let n = tx
        .execute(
            "UPDATE ducklake.ducklake_data_file SET end_snapshot = $1
         WHERE table_id = $2 AND end_snapshot IS NULL AND data_file_id = ANY($3)",
            &[&snapshot_id, &meta.table_id, &ids],
        )
        .await?;
    if n != ids.len() as u64 {
        bail!("source files changed while committing");
    }
    tx.execute(
        "UPDATE ducklake.ducklake_delete_file SET end_snapshot = $1
         WHERE table_id = $2 AND end_snapshot IS NULL AND data_file_id = ANY($3)",
        &[&snapshot_id, &meta.table_id, &ids],
    )
    .await?;
    for (i, file) in files.iter().enumerate() {
        let id = next_file + i as i64;
        tx.execute(
            "INSERT INTO ducklake.ducklake_data_file
             (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path,
              path_is_relative, file_format, record_count, file_size_bytes, footer_size,
              row_id_start, partition_id, encryption_key, mapping_id, partial_max)
             VALUES ($1,$2,$3,NULL,NULL,$4,true,'parquet',$5,$6,$7,NULL,NULL,NULL,NULL,NULL)",
            &[
                &id,
                &meta.table_id,
                &snapshot_id,
                &file.name,
                &file.rows,
                &file.size,
                &file.footer,
            ],
        )
        .await?;
        for s in &file.stats {
            tx.execute(
                "INSERT INTO ducklake.ducklake_file_column_stats
                 (data_file_id,table_id,column_id,column_size_bytes,value_count,null_count,
                  min_value,max_value,contains_nan,extra_stats)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL)",
                &[
                    &id,
                    &meta.table_id,
                    &s.column_id,
                    &s.size_bytes,
                    &s.value_count,
                    &s.null_count,
                    &s.min,
                    &s.max,
                    &s.contains_nan,
                ],
            )
            .await?;
        }
    }
    // Deleted rows leave safe, conservative column bounds and do not reuse row IDs.
    let updated = tx
        .execute(
            "UPDATE ducklake.ducklake_table_stats SET
         file_size_bytes = (SELECT COALESCE(SUM(file_size_bytes),0)::bigint
           FROM ducklake.ducklake_data_file WHERE table_id = $1 AND end_snapshot IS NULL),
         record_count = record_count - $2
         WHERE table_id = $1",
            &[&meta.table_id, &removed],
        )
        .await?;
    if updated != 1 {
        bail!("table statistics missing while committing compaction");
    }
    tx.commit().await.context("committing repack snapshot")?;
    Ok(snapshot_id)
}

pub async fn run(cfg: &Config, job: &Job, log: &mut Vec<String>) -> Result<()> {
    job.validate()?;
    let started = std::time::Instant::now();
    progress(log, format!(
        "{} [{}, {}) UTC: connecting to catalog", job.table, job.start()?, job.end()?
    ));
    let mut client = connect(cfg).await?;
    progress(log, format!("waiting for repack advisory lock on {}", job.table));
    lock_table(&client, job).await?;
    progress(log, format!("acquired repack advisory lock on {}", job.table));
    let (schema_name, table_name) = job.names();
    let meta = table_meta(&client, schema_name, table_name).await?;
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    if !has_current_files(&tx, meta.table_id, job).await? {
        tx.commit().await?;
        progress(log, "none of the supplied files are current; skipping already completed repack".into());
        return Ok(());
    }
    let utc_col = meta
        .columns
        .get("utcbegin")
        .context("table has no utcbegin column")?
        .id;
    job.validate_schema(&schema(&meta)?)?;
    progress(log, format!("checking inlined rows and source files for table {}", meta.table_id));
    check_inlined(&tx, meta.table_id, job).await?;
    let selected = sources(&tx, meta.table_id, utc_col, job).await?;
    tx.commit().await?;
    if selected.is_empty() {
        progress(log, "no source files overlap the requested range".into());
        return Ok(());
    }
    progress(log, format!(
        "repacking {} files; node-only row groups: {}",
        selected.len(),
        job.node_only()
    ));
    let (store, prefix) = object_store_for(cfg, &meta.root)?;
    let mut work = Work::new(store, prefix);
    progress(log, format!("temporary runs: {}", work.tmp));
    let result = async {
        let files = prepare(cfg, &meta, &selected, job, &mut work, log).await?;
        // Never delete published files if a lost COMMIT response makes the outcome uncertain.
        work.outputs.clear();
        let mut attempt = 1;
        let snapshot = loop {
            progress(log, format!("publishing {} replacement files (attempt {attempt})", files.len()));
            match commit(&mut client, &meta, utc_col, &selected, job, &files).await {
                Ok(snapshot) => break snapshot,
                Err(e) if is_commit_conflict(&e) && attempt < MAX_COMMIT_ATTEMPTS => {
                    progress(log, format!("repack commit conflict (attempt {attempt}); retrying: {e:#}"));
                    tokio::time::sleep(std::time::Duration::from_millis(250 * attempt as u64)).await;
                    attempt += 1;
                }
                Err(e) => {
                    progress(log, format!("commit failed; retaining candidate files {:?} under {} to avoid \
                        deleting files after an uncertain commit; unreferenced files may be cleaned up later",
                        files.iter().map(|f| &f.name).collect::<Vec<_>>(), meta.root));
                    return Err(e);
                }
            }
        };
        progress(log, format!("committed compaction snapshot {snapshot}; retired {} files in {:.1}s",
            selected.len(), started.elapsed().as_secs_f64()));
        Ok(())
    }.await;
    work.cleanup(log).await;
    progress(log, format!("cleanup complete; elapsed {:.1}s; success={}", started.elapsed().as_secs_f64(), result.is_ok()));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray};

    #[test]
    fn optional_files_roundtrip_preserves_existing_jobs() {
        let base = serde_json::json!({
            "table": "rtprices", "begin_date": "2026-01-01", "end_date": "2026-02-01"
        });
        let job: Job = serde_json::from_value(base.clone()).unwrap();
        job.validate().unwrap();
        assert!(job.files.is_none());
        assert_eq!(serde_json::to_value(job).unwrap(), base);
        for files in [
            serde_json::Value::Null,
            serde_json::json!([]),
            serde_json::json!(["daily-1.parquet", "daily-2.parquet"]),
        ] {
            let mut payload = base.clone();
            payload["files"] = files.clone();
            let job: Job = serde_json::from_value(payload).unwrap();
            job.validate().unwrap();
            if files.is_null() {
                assert!(job.files.is_none());
            } else {
                assert_eq!(serde_json::to_value(job).unwrap()["files"], files);
            }
        }
        for files in [serde_json::json!("file.parquet"), serde_json::json!([42])] {
            let mut payload = base.clone();
            payload["files"] = files;
            assert!(serde_json::from_value::<Job>(payload).is_err());
        }
    }

    async fn test_database() -> (Client, tokio::task::JoinHandle<()>) {
        let url = std::env::var("MAINTENANCE_TEST_DATABASE_URL").unwrap();
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        (client, tokio::spawn(async move { connection.await.unwrap() }))
    }

    #[tokio::test]
    #[ignore = "requires MAINTENANCE_TEST_DATABASE_URL pointing to isolated test Postgres"]
    async fn postgres_advisory_lock_serializes_aliases_and_overlapping_ranges() {
        use std::time::Duration;
        use tokio::time::timeout;

        let (mut owner, owner_connection) = test_database().await;
        let (waiter, waiter_connection) = test_database().await;
        let (other, other_connection) = test_database().await;
        let job: Job = serde_json::from_value(serde_json::json!({
            "table": "rtprices", "begin_date": "2026-01-01", "end_date": "2026-02-01"
        })).unwrap();
        lock_table(&owner, &job).await.unwrap();
        let alias: Job = serde_json::from_value(serde_json::json!({
            "table": "ciso.rtprices", "begin_date": "2026-01-15", "end_date": "2026-02-15"
        })).unwrap();
        let mut waiting = tokio::spawn(async move {
            lock_table(&waiter, &alias).await.unwrap();
            waiter
        });
        assert!(timeout(Duration::from_millis(100), &mut waiting).await.is_err());
        // A commit must not release the lock while sorting or between retries.
        owner.transaction().await.unwrap().commit().await.unwrap();
        assert!(timeout(Duration::from_millis(100), &mut waiting).await.is_err());
        let other_job: Job = serde_json::from_value(serde_json::json!({
            "table": "ciso.fmprices", "begin_date": "2026-01-01", "end_date": "2026-02-01"
        })).unwrap();
        timeout(Duration::from_secs(5), lock_table(&other, &other_job)).await.unwrap().unwrap();
        drop(other);
        other_connection.await.unwrap();
        // Early returns/errors drop the owning client just like normal completion.
        drop(owner);
        owner_connection.await.unwrap();
        let waiter = timeout(Duration::from_secs(5), waiting).await.unwrap().unwrap();
        drop(waiter);
        waiter_connection.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires MAINTENANCE_TEST_DATABASE_URL pointing to a test Postgres without a ducklake schema"]
    async fn postgres_file_guard_checks_any_current_path_in_the_target_table() {
        let (mut client, connection) = test_database().await;
        let tx = client.transaction().await.unwrap();
        tx.batch_execute("
            CREATE SCHEMA ducklake;
            CREATE TABLE ducklake.ducklake_data_file (
                table_id bigint, path text, end_snapshot bigint
            );
            INSERT INTO ducklake.ducklake_data_file VALUES
                (1, 'daily-1.parquet', NULL),
                (1, 'daily-2.parquet', 10),
                (1, 'monthly.parquet', NULL),
                (2, 'other-table.parquet', NULL);
        ").await.unwrap();
        let mut job: Job = serde_json::from_value(serde_json::json!({
            "table": "rtprices", "begin_date": "2026-01-01", "end_date": "2026-02-01"
        })).unwrap();
        for (files, expected) in [
            (None, true),
            (Some(vec![]), false),
            (Some(vec!["missing.parquet"]), false),
            (Some(vec!["daily-2.parquet"]), false),
            (Some(vec!["other-table.parquet"]), false),
            (Some(vec!["daily-1.parquet"]), true),
            (Some(vec!["daily-2.parquet", "daily-1.parquet"]), true),
        ] {
            job.files = files.map(|paths| paths.into_iter().map(String::from).collect());
            assert_eq!(has_current_files(&tx, 1, &job).await.unwrap(), expected);
        }
        tx.execute(
            "UPDATE ducklake.ducklake_data_file SET end_snapshot = 11
              WHERE table_id = 1 AND path = 'daily-1.parquet'",
            &[],
        ).await.unwrap();
        assert!(!has_current_files(&tx, 1, &job).await.unwrap());
        tx.rollback().await.unwrap();
        drop(client);
        connection.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires configured Postgres catalog; reads ciso.rtprices without modifying data"]
    async fn inlined_text_timestamps_accept_datetime_parameters() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let cfg = Config::from_env().unwrap();
        let mut client = connect(&cfg).await.unwrap();
        let tx = client.build_transaction().read_only(true).start().await.unwrap();
        let meta = table_meta(&tx, "ciso", "rtprices").await.unwrap();
        let count: i64 = tx.query_one(
            "SELECT count(*) FROM ducklake.ducklake_inlined_data_tables WHERE table_id = $1",
            &[&meta.table_id],
        ).await.unwrap().get(0);
        assert!(count > 0, "regression test requires an inlined data table");
        let job = Job {
            table: "ciso.rtprices".into(),
            begin_date: "1900-01-01".parse().unwrap(),
            end_date: "1900-01-02".parse().unwrap(),
            unique: None,
            files: None,
        };
        check_inlined(&tx, meta.table_id, &job).await.unwrap();
    }

    #[test]
    fn validates_optional_unique_columns() {
        let base = serde_json::json!({
            "table": "rtprices", "begin_date": "2026-01-01", "end_date": "2026-01-02"
        });
        let schema = crate::transform::target_schema("rt");
        for unique in [
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::json!(["utcbegin", "node"])),
        ] {
            let mut payload = base.clone();
            if let Some(unique) = unique {
                payload["unique"] = unique;
            }
            let job: Job = serde_json::from_value(payload).unwrap();
            job.validate().unwrap();
            job.validate_schema(&schema).unwrap();
        }
        for unique in [
            serde_json::json!([]),
            serde_json::json!([""]),
            serde_json::json!(["node", "node"]),
            serde_json::json!(["missing"]),
            serde_json::json!([ROW_ID]),
        ] {
            let mut payload = base.clone();
            payload["unique"] = unique;
            let job: Job = serde_json::from_value(payload).unwrap();
            assert!(
                job.validate()
                    .and_then(|_| job.validate_schema(&schema))
                    .is_err()
            );
        }
        let mut payload = base;
        payload["unique"] = serde_json::json!(["node"]);
        let job: Job = serde_json::from_value(payload).unwrap();
        let no_download = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .filter(|f| f.name() != "downloaded_at")
                .cloned()
                .collect::<Vec<_>>(),
        ));
        assert!(job.validate_schema(&no_download).is_err());
        let wrong_download = Arc::new(Schema::new(vec![
            Field::new("node", DataType::Utf8, true),
            Field::new("downloaded_at", DataType::Utf8, true),
        ]));
        assert!(job.validate_schema(&wrong_download).is_err());
    }

    #[test]
    fn validates_dates_table_and_calendar_month_cutoff() {
        for (start, end, monthly) in [
            ("2026-01-01", "2026-01-31", false),
            ("2026-01-01", "2026-02-01", true),
            ("2026-02-01", "2026-03-01", true),
            ("2026-01-31", "2026-02-28", true),
        ] {
            let job: Job = serde_json::from_value(serde_json::json!({
                "table":"rtprices","begin_date":start,"end_date":end}))
            .unwrap();
            job.validate().unwrap();
            assert_eq!(job.node_only(), monthly);
        }
        for table in ["", "a.b.c", "a;DROP", "../rtprices"] {
            let job = Job {
                table: table.into(),
                begin_date: "2026-01-01".parse().unwrap(),
                end_date: "2026-02-01".parse().unwrap(),
                unique: None,
                files: None,
            };
            assert!(job.validate().is_err());
        }
        for (start, end) in [("2026-01-01", "2026-01-01"), ("2026-02-01", "2026-01-01")] {
            let job: Job = serde_json::from_value(serde_json::json!({
                "table":"ciso.rtprices","begin_date":start,"end_date":end}))
            .unwrap();
            assert!(job.validate().is_err());
        }
        assert!(
            serde_json::from_value::<Job>(serde_json::json!({
            "table":"rtprices", "begin_date":"2026-02-30", "end_date":"2026-03-01"}))
            .is_err()
        );
    }

    #[test]
    fn los_angeles_boundaries_follow_daylight_saving() {
        for (begin, end, expected_start, expected_end, hours) in [
            (
                "2026-01-01",
                "2026-01-02",
                "2026-01-01T08:00:00Z",
                "2026-01-02T08:00:00Z",
                24,
            ),
            (
                "2026-07-01",
                "2026-07-02",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                24,
            ),
            (
                "2026-03-08",
                "2026-03-09",
                "2026-03-08T08:00:00Z",
                "2026-03-09T07:00:00Z",
                23,
            ),
            (
                "2026-11-01",
                "2026-11-02",
                "2026-11-01T07:00:00Z",
                "2026-11-02T08:00:00Z",
                25,
            ),
        ] {
            let job = Job {
                table: "rtprices".into(),
                begin_date: begin.parse().unwrap(),
                end_date: end.parse().unwrap(),
                unique: None,
                files: None,
            };
            job.validate().unwrap();
            let start = DateTime::parse_from_rfc3339(expected_start)
                .unwrap()
                .with_timezone(&Utc);
            let end = DateTime::parse_from_rfc3339(expected_end)
                .unwrap()
                .with_timezone(&Utc);
            assert_eq!(job.start().unwrap(), start);
            assert_eq!(job.end().unwrap(), end);
            assert_eq!((end - start).num_hours(), hours);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "utcbegin",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        start.timestamp_micros() - 1,
                        start.timestamp_micros(),
                        end.timestamp_micros() - 1,
                        end.timestamp_micros(),
                    ])
                    .with_timezone("UTC"),
                )],
            )
            .unwrap();
            assert_eq!(
                split(&batch, &job)
                    .unwrap()
                    .each_ref()
                    .map(|b| b.num_rows()),
                [1, 2, 1]
            );
        }
        let job = Job {
            table: "rtprices".into(),
            begin_date: "2026-03-01".parse().unwrap(),
            end_date: "2026-04-01".parse().unwrap(),
            unique: None,
            files: None,
        };
        assert!(job.node_only());
        assert_eq!(
            (job.end().unwrap() - job.start().unwrap()).num_hours(),
            31 * 24 - 1
        );
    }

    #[test]
    fn boundary_splits_preserve_all_rows_and_embedded_identity() {
        let job: Job = serde_json::from_value(serde_json::json!({
            "table":"rtprices", "begin_date":"2026-01-01", "end_date":"2026-02-01"}))
        .unwrap();
        let meta = TableMeta {
            table_id: 1,
            root: String::new(),
            columns: HashMap::from([
                (
                    "node".into(),
                    Column {
                        id: 11,
                        ty: "varchar".into(),
                    },
                ),
                (
                    "utcbegin".into(),
                    Column {
                        id: 22,
                        ty: "timestamptz".into(),
                    },
                ),
            ]),
        };
        let data_schema = schema(&meta).unwrap();
        let target = with_identity(&data_schema);
        let times = [
            job.start().unwrap().timestamp_micros() - 1,
            job.start().unwrap().timestamp_micros(),
            job.end().unwrap().timestamp_micros() - 1,
            job.end().unwrap().timestamp_micros(),
        ];
        let batch = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(StringArray::from(vec!["A"; 4])),
                Arc::new(TimestampMicrosecondArray::from(times.to_vec()).with_timezone("UTC")),
            ],
        )
        .unwrap();
        let source = Source {
            id: 1,
            path: String::new(),
            relative: true,
            rows: 4,
            row_id_start: Some(100),
            begin_snapshot: 7,
            delete: None,
        };
        let normalized = normalize(&batch, &target, &source, 0).unwrap();
        let parts = split(&normalized, &job).unwrap();
        assert_eq!(parts.each_ref().map(|b| b.num_rows()), [1, 2, 1]);
        for (part, expected) in parts.iter().zip([vec![100], vec![101, 102], vec![103]]) {
            assert_eq!(
                part.column(2)
                    .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                    .values()
                    .as_ref(),
                expected.as_slice()
            );
        }
        let again = normalize(&normalized, &target, &source, 999).unwrap();
        assert_eq!(again, normalized);
        let mut fields = batch.schema().fields().to_vec();
        fields[0] = Arc::new(Field::new("node", DataType::Utf8, true));
        let unmapped =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap();
        assert!(normalize(&unmapped, &target, &source, 0).is_err());
    }

    #[tokio::test]
    async fn positional_deletes_stream_across_batch_boundaries() {
        use datafusion::parquet::arrow::AsyncArrowWriter;
        use object_store::buffered::BufWriter;
        use object_store::memory::InMemory;
        for (positions, expected_count, valid) in [
            (vec![0, 2, 4], 3, true),
            (vec![0, 2, 2], 3, false),
            (vec![0, 5], 2, false),
            (vec![0, 2], 3, false),
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = ObjPath::from("deletes.parquet");
            let batch = RecordBatch::try_from_iter([(
                "pos",
                Arc::new(Int64Array::from(positions)) as ArrayRef,
            )])
            .unwrap();
            let mut writer = AsyncArrowWriter::try_new(
                BufWriter::new(store.clone(), path.clone()),
                batch.schema(),
                None,
            )
            .unwrap();
            writer.write(&batch).await.unwrap();
            writer.close().await.unwrap();
            let mut deletes = Deletes {
                stream: Some(sort::reader(store, path).await.unwrap()),
                positions: Vec::new(),
                index: 0,
                previous: None,
                count: 0,
                expected: expected_count,
                source_rows: 5,
            };
            let result: Result<Vec<bool>> = async {
                let mut kept = Vec::new();
                for (start, n) in [(0, 2), (2, 2), (4, 1)] {
                    kept.extend(deletes.mask(start, n).await?.iter().map(|v| v.unwrap()));
                }
                assert!(deletes.peek().await?.is_none());
                Ok(kept)
            }
            .await;
            if valid {
                assert_eq!(result.unwrap(), [false, true, false, true, false]);
            } else {
                assert!(result.is_err());
            }
        }
    }
}
