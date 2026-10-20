//! Snapshot-preserving compaction. External-sort intermediate runs live in blob storage.
use super::*;
use chrono::{Months, NaiveDate, TimeZone};
use chrono_tz::America::Los_Angeles;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array};
use datafusion::arrow::compute::{cast, concat_batches, filter_record_batch};
use datafusion::arrow::datatypes::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

mod sort;

const ROW_ID: &str = "_ducklake_internal_row_id";
const SNAPSHOT_ID: &str = "_ducklake_internal_snapshot_id";
const ROW_ID_FIELD: &str = "2147483540";
const SNAPSHOT_FIELD: &str = "2147483539";
const FAST_READ_CONCURRENCY: usize = 8;
const FAST_PROGRESS_INTERVAL: Duration = Duration::from_secs(15);

fn progress(log: &mut Vec<String>, message: String) {
    eprintln!("repack: {message}");
    log.push(message);
}

fn fast_progress(log: &mut Vec<String>, job: &Job, message: String) {
    progress(
        log,
        format!(
            "{} [{}, {}) fastpack: {message}",
            job.table, job.begin_date, job.end_date
        ),
    );
}

async fn fast_wait<T: Send>(
    log: &mut Vec<String>,
    job: &Job,
    phase: &str,
    future: impl std::future::Future<Output = T> + Send,
) -> T {
    let started = Instant::now();
    tokio::pin!(future);
    loop {
        tokio::select! {
            result = &mut future => return result,
            _ = tokio::time::sleep(FAST_PROGRESS_INTERVAL) => {
                fast_progress(log, job, format!("{phase}; waiting {:.1}s", started.elapsed().as_secs_f64()));
            }
        }
    }
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
    fn report(&self) -> &'static crate::report::Report {
        let (schema, table) = self.names();
        crate::report::REPORTS
            .iter()
            .copied()
            .find(|report| schema == SCHEMA && report.name == table)
            // Preserve the existing price-table layout for manually named tables.
            .unwrap_or(&crate::report::RTPRICES)
    }

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
    run_bytes: usize,
    node_bytes: usize,
    temps: HashSet<ObjPath>,
    outputs: Vec<ObjPath>,
}

impl Work {
    #[cfg(test)]
    fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self::with_limits(
            store,
            prefix,
            crate::config::DEFAULT_REPACK_RUN_BYTES,
            crate::config::DEFAULT_REPACK_NODE_BYTES,
        )
    }

    fn with_limits(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        run_bytes: usize,
        node_bytes: usize,
    ) -> Self {
        let tmp = format!("{prefix}_tmp/repack-{}/", Uuid::now_v7());
        Self {
            store,
            prefix,
            tmp,
            run_bytes,
            node_bytes,
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

    async fn discard_outputs(&mut self) -> Result<()> {
        while let Some(path) = self.outputs.first().cloned() {
            self.store.delete(&path).await?;
            self.outputs.remove(0);
        }
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

#[derive(Debug, Clone, PartialEq)]
struct Source {
    id: i64,
    path: String,
    relative: bool,
    rows: i64,
    row_id_start: Option<i64>,
    begin_snapshot: i64,
    delete: Option<DeleteFile>,
}

#[derive(Debug, Clone, PartialEq)]
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

struct LoadedGroup {
    batch: RecordBatch,
    position: usize,
    estimated_bytes: usize,
}

struct FastCursor {
    source: Source,
    deletes: Deletes,
    groups: sort::RowGroupFile,
    row_offset: i64,
    current: Option<LoadedGroup>,
    finished: bool,
}

enum FastLoad {
    Loaded,
    Finished,
    OverBudget { required: usize, budget: usize },
    Ineligible(String),
    Deferred,
}

struct ReadBudget {
    used: AtomicUsize,
    limit: usize,
}

impl ReadBudget {
    fn new(used: usize, limit: usize) -> Self {
        Self {
            used: AtomicUsize::new(used),
            limit,
        }
    }

    fn reserve(&self, bytes: usize) -> std::result::Result<(), FastLoad> {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .map(|_| ())
            .map_err(|used| FastLoad::OverBudget {
                required: used.saturating_add(bytes),
                budget: self.limit,
            })
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }
}

impl FastCursor {
    async fn load_next(
        &mut self,
        target: &SchemaRef,
        row_group_col: &str,
        budget: &ReadBudget,
        optional: bool,
    ) -> Result<FastLoad> {
        loop {
            let Some(next_bounds) = self.groups.next_bounds() else {
                if self.row_offset != self.source.rows {
                    bail!("record count mismatch for source {}", self.source.id);
                }
                if self.deletes.peek().await?.is_some() {
                    bail!("unconsumed delete positions for source {}", self.source.id);
                }
                self.finished = true;
                return Ok(FastLoad::Finished);
            };
            let reserved = next_bounds.bytes;
            if let Err(reason) = budget.reserve(reserved) {
                return Ok(if optional { FastLoad::Deferred } else { reason });
            }
            let Some((bounds, raw)) = self.groups.next_group().await? else {
                bail!("source {} row-group metadata changed", self.source.id);
            };
            if let Some(reason) = row_group_values_mismatch(&raw, row_group_col, &bounds)? {
                return Ok(FastLoad::Ineligible(format!(
                    "source {} row group {} {reason}",
                    self.source.id,
                    self.groups.last_index()
                )));
            }
            let count = i64::try_from(raw.num_rows())?;
            let next_offset = self
                .row_offset
                .checked_add(count)
                .context("source row offset overflow")?;
            if next_offset > self.source.rows {
                bail!("record count mismatch for source {}", self.source.id);
            }
            let normalized = normalize(&raw, target, &self.source, self.row_offset)?;
            let keep = self.deletes.mask(self.row_offset, raw.num_rows()).await?;
            self.row_offset = next_offset;
            let batch = filter_record_batch(&normalized, &keep)?;
            if batch.num_rows() == 0 {
                budget.release(reserved);
                continue;
            }
            let estimated_bytes = batch.get_array_memory_size().max(bounds.bytes);
            if let Err(reason) = budget.reserve(estimated_bytes - reserved) {
                return Ok(reason);
            }
            self.current = Some(LoadedGroup {
                batch,
                position: 0,
                estimated_bytes,
            });
            return Ok(FastLoad::Loaded);
        }
    }
}

fn fast_load_reason(source_id: i64, result: FastLoad) -> Option<String> {
    match result {
        FastLoad::Loaded | FastLoad::Finished | FastLoad::Deferred => None,
        FastLoad::OverBudget { required, budget } => Some(format!(
            "source {source_id} needs {required} bytes to load a row group, above the {budget}-byte node budget"
        )),
        FastLoad::Ineligible(reason) => Some(reason),
    }
}

async fn fill_cursors(
    cursors: &mut [FastCursor],
    target: &SchemaRef,
    row_group_col: &str,
    retained_bytes: usize,
    limit: usize,
    optional: bool,
) -> Result<Option<String>> {
    let budget = ReadBudget::new(loaded_bytes(cursors).saturating_add(retained_bytes), limit);
    let mut pending = Vec::new();
    for cursor in cursors {
        if cursor.current.is_none() && !cursor.finished {
            let budget = &budget;
            pending.push(async move {
                let result = cursor
                    .load_next(target, row_group_col, budget, optional)
                    .await
                    .with_context(|| {
                        format!("reading row group for source {}", cursor.source.id)
                    })?;
                Ok::<_, anyhow::Error>(fast_load_reason(cursor.source.id, result))
            });
        }
    }
    let mut reads = futures::stream::iter(pending).buffer_unordered(FAST_READ_CONCURRENCY);
    let mut fallback = None;
    // Drain all reads before returning so no I/O remains in flight during fallback.
    while let Some(reason) = reads.try_next().await? {
        if fallback.is_none() {
            fallback = reason;
        }
    }
    Ok(fallback)
}

async fn write_with_read_ahead(
    cursors: &mut [FastCursor],
    target: &SchemaRef,
    row_group_col: &str,
    retained_bytes: usize,
    budget: usize,
    write: impl std::future::Future<Output = Result<()>>,
) -> Result<Option<String>> {
    // Complete both sides even on fallback/error, before closing or deleting output.
    let (read, written) = tokio::join!(
        fill_cursors(cursors, target, row_group_col, retained_bytes, budget, true),
        write,
    );
    written?;
    read
}

fn row_group_values_mismatch(
    batch: &RecordBatch,
    row_group_col: &str,
    bounds: &sort::RowGroupBounds,
) -> Result<Option<String>> {
    let nodes = batch
        .column(batch.schema().index_of(row_group_col)?)
        .as_string::<i32>();
    if nodes.null_count() != 0 {
        return Ok(Some(format!("contains null {row_group_col} values")));
    }
    let mut previous: Option<&str> = None;
    let mut min: Option<&str> = None;
    let mut max: Option<&str> = None;
    for node in nodes.iter().flatten() {
        if previous.is_some_and(|last| last > node) {
            return Ok(Some(format!(
                "is not ordered by {row_group_col:?} in its rows"
            )));
        }
        min.get_or_insert(node);
        max = Some(node);
        previous = Some(node);
    }
    if min != Some(bounds.min.as_str()) || max != Some(bounds.max.as_str()) {
        return Ok(Some(format!(
            "actual {row_group_col:?} bounds do not match statistics ({:?}..={:?})",
            bounds.min, bounds.max
        )));
    }
    Ok(None)
}

fn loaded_bytes(cursors: &[FastCursor]) -> usize {
    cursors
        .iter()
        .filter_map(|cursor| cursor.current.as_ref())
        .fold(0usize, |total, group| {
            total.saturating_add(group.estimated_bytes)
        })
}

async fn try_aligned(
    cfg: &Config,
    meta: &TableMeta,
    sources: &[Source],
    job: &Job,
    work: &mut Work,
    target: &SchemaRef,
    cols: &[&Column],
    log: &mut Vec<String>,
) -> Result<Option<(Vec<FileInfo>, i64, i64)>> {
    fast_progress(
        log,
        job,
        format!(
            "attempting aligned fastpack for {} sources using row-group column {:?} (node budget={} bytes)",
            sources.len(),
            job.report().row_group_col,
            work.node_bytes
        ),
    );
    let Some(row_group_col) = job.report().row_group_col else {
        fast_progress(
            log,
            job,
            "unable to fastpack: report has no row-group column".into(),
        );
        return Ok(None);
    };
    if let Some(unique) = &job.unique
        && !unique.iter().any(|name| name == row_group_col)
    {
        fast_progress(
            log,
            job,
            format!(
                "unable to fastpack: unique columns {unique:?} do not include row-group column {row_group_col:?}"
            ),
        );
        return Ok(None);
    }
    if job.report().sort_cols.first().copied() != Some(row_group_col) {
        fast_progress(
            log,
            job,
            format!(
                "unable to fastpack: row-group column {row_group_col:?} is not the first output sort column"
            ),
        );
        return Ok(None);
    }
    fast_progress(
        log,
        job,
        format!(
            "opening metadata: 0/{} sources; up to {FAST_READ_CONCURRENCY} concurrent reads",
            sources.len()
        ),
    );
    let started = Instant::now();
    let mut last_progress = Instant::now();
    let mut pending = Vec::with_capacity(sources.len());
    for source in sources {
        pending.push(async move {
            let full = location(meta, &source.path, source.relative);
            let (store, path) = object_store_for(cfg, &full)?;
            let groups = sort::RowGroupFile::open(store, ObjPath::from(path), row_group_col)
                .await
                .with_context(|| format!("reading row-group metadata for source {}", source.id))?;
            let groups = match groups {
                sort::RowGroupOpen::Eligible(groups) => groups,
                sort::RowGroupOpen::Ineligible(reason) => {
                    return Ok::<_, anyhow::Error>(Err(format!(
                        "source {} is not eligible: {reason}",
                        source.id
                    )));
                }
            };
            Ok(Ok(FastCursor {
                source: source.clone(),
                deletes: Deletes::open(cfg, meta, source)
                    .await
                    .with_context(|| format!("opening deletes for source {}", source.id))?,
                groups,
                row_offset: 0,
                current: None,
                finished: false,
            }))
        });
    }
    let mut openings = futures::stream::iter(pending).buffered(FAST_READ_CONCURRENCY);
    let mut cursors = Vec::with_capacity(sources.len());
    while let Some(cursor) = fast_wait(
        log,
        job,
        &format!(
            "opening metadata: {}/{} sources",
            cursors.len(),
            sources.len()
        ),
        openings.try_next(),
    )
    .await?
    {
        match cursor {
            Ok(cursor) => cursors.push(cursor),
            Err(reason) => {
                fast_progress(log, job, format!("unable to fastpack: {reason}"));
                return Ok(None);
            }
        }
        if cursors.len() % 16 == 0
            || cursors.len() == sources.len()
            || last_progress.elapsed() >= FAST_PROGRESS_INTERVAL
        {
            fast_progress(
                log,
                job,
                format!(
                    "opened metadata: {}/{} sources, {} row groups in {:.1}s",
                    cursors.len(),
                    sources.len(),
                    cursors
                        .iter()
                        .map(|cursor| cursor.groups.group_count())
                        .sum::<usize>(),
                    started.elapsed().as_secs_f64()
                ),
            );
            last_progress = Instant::now();
        }
    }
    align_cursors(cursors, job, work, target, cols, row_group_col, log).await
}

async fn align_cursors(
    mut cursors: Vec<FastCursor>,
    job: &Job,
    work: &mut Work,
    target: &SchemaRef,
    cols: &[&Column],
    row_group_col: &str,
    log: &mut Vec<String>,
) -> Result<Option<(Vec<FileInfo>, i64, i64)>> {
    let started = std::time::Instant::now();
    let node_index = target.index_of(row_group_col)?;
    let mut writers: [Option<sort::OutputWriter>; 3] = std::array::from_fn(|_| None);
    macro_rules! fallback {
        ($reason:expr) => {{
            fast_progress(
                log,
                job,
                format!(
                    "unable to fastpack: {}; falling back to external sort",
                    $reason
                ),
            );
            for writer in &mut writers {
                if let Some(writer) = writer.take() {
                    fast_wait(log, job, "closing output before fallback", writer.finish()).await?;
                }
            }
            return Ok(None);
        }};
    }
    let mut live_rows = 0i64;
    let mut removed = 0i64;
    let mut nodes_written = 0usize;
    let mut last_progress = Instant::now();
    let total_groups = cursors
        .iter()
        .map(|cursor| cursor.groups.group_count())
        .sum::<usize>();
    fast_progress(
        log,
        job,
        format!(
            "processing {} sources, {total_groups} row groups; up to {FAST_READ_CONCURRENCY} concurrent reads with write-overlapped read-ahead",
            cursors.len()
        ),
    );
    loop {
        let phase =
            format!("loading row groups after {nodes_written} nodes, {live_rows} live rows");
        if let Some(reason) = fast_wait(
            log,
            job,
            &phase,
            fill_cursors(
                &mut cursors,
                target,
                row_group_col,
                0,
                work.node_bytes,
                false,
            ),
        )
        .await?
        {
            fallback!(reason);
        }
        let candidate = cursors
            .iter()
            .filter_map(|cursor| {
                let group = cursor.current.as_ref()?;
                Some(
                    group
                        .batch
                        .column(node_index)
                        .as_string::<i32>()
                        .value(group.position)
                        .to_owned(),
                )
            })
            .min();
        let Some(candidate) = candidate else {
            break;
        };

        let mut node_batches = Vec::new();
        let mut node_buffer_bytes = 0usize;
        for cursor_index in 0..cursors.len() {
            loop {
                if cursors[cursor_index].current.is_none() && !cursors[cursor_index].finished {
                    let budget = ReadBudget::new(
                        loaded_bytes(&cursors).saturating_add(node_buffer_bytes),
                        work.node_bytes,
                    );
                    let result = fast_wait(
                        log,
                        job,
                        &format!(
                            "reading additional row group for node {candidate:?}, source {}",
                            cursors[cursor_index].source.id
                        ),
                        cursors[cursor_index].load_next(target, row_group_col, &budget, false),
                    )
                    .await?;
                    if let Some(reason) = fast_load_reason(cursors[cursor_index].source.id, result)
                    {
                        fallback!(reason);
                    }
                }
                let source_id = cursors[cursor_index].source.id;
                let Some(group) = cursors[cursor_index].current.as_mut() else {
                    break;
                };
                let current_nodes = group.batch.column(node_index).as_string::<i32>();
                let current = current_nodes.value(group.position);
                if current < candidate.as_str() {
                    fallback!(format!(
                        "source {} has node {:?} before the next aligned node {:?}",
                        source_id, current, candidate
                    ));
                }
                if current > candidate.as_str() {
                    break;
                }
                let start = group.position;
                let mut end = start + 1;
                while end < group.batch.num_rows() && current_nodes.value(end) == candidate {
                    end += 1;
                }
                let batch = group.batch.slice(start, end - start);
                let group_finished = end == group.batch.num_rows();
                group.position = end;
                let batch_bytes = batch.get_array_memory_size();
                let _ = group;
                node_batches.push(batch);
                if group_finished {
                    cursors[cursor_index].current = None;
                    node_buffer_bytes = node_buffer_bytes.saturating_add(batch_bytes);
                } else {
                    if loaded_bytes(&cursors).saturating_add(node_buffer_bytes) > work.node_bytes {
                        fallback!(format!(
                            "node {:?} exceeds the {}-byte working-set budget",
                            candidate, work.node_bytes
                        ));
                    }
                    break;
                }
                if loaded_bytes(&cursors).saturating_add(node_buffer_bytes) > work.node_bytes {
                    fallback!(format!(
                        "node {:?} exceeds the {}-byte working-set budget",
                        candidate, work.node_bytes
                    ));
                }
                let next = cursors[cursor_index].groups.next_bounds();
                if next.is_some_and(|bounds| bounds.min.as_str() < candidate.as_str()) {
                    fallback!(format!(
                        "source {} has a later row group starting before current node {:?}",
                        cursors[cursor_index].source.id, candidate
                    ));
                }
                if !next.is_some_and(|bounds| bounds.min == candidate) {
                    break;
                }
            }
        }
        if node_batches.is_empty() {
            fallback!("no node data could be aligned across the sources".to_string());
        }
        let node = concat_batches(target, &node_batches)?;
        if loaded_bytes(&cursors)
            .saturating_add(node_buffer_bytes)
            .saturating_add(node.get_array_memory_size())
            > work.node_bytes
        {
            fallback!(format!(
                "node {:?} exceeded the {}-byte working-set budget after alignment",
                candidate, work.node_bytes
            ));
        }
        live_rows += node.num_rows() as i64;
        let mut regions = split(&node, job)?;
        if let Some(unique) = &job.unique {
            let (deduplicated, count) = sort::deduplicate_batch(&regions[1], unique)?;
            removed += count;
            regions[1] = deduplicated;
        }
        drop(node);
        drop(node_batches);
        let ordering = job
            .report()
            .sort_cols
            .iter()
            .map(|name| ((*name).to_string(), false))
            .collect::<Vec<_>>();
        for batch in &mut regions {
            *batch = sort::sort_batch(batch, &ordering)?;
        }
        let retained_bytes = regions.iter().fold(0usize, |bytes, batch| {
            bytes.saturating_add(batch.get_array_memory_size())
        });
        let phase = format!(
            "writing node {candidate:?} and reading ahead; {nodes_written} nodes written, {live_rows} live rows"
        );
        let budget = work.node_bytes;
        let write = async {
            for (index, batch) in regions.iter().enumerate() {
                if batch.num_rows() == 0 {
                    continue;
                }
                if writers[index].is_none() {
                    writers[index] = Some(sort::OutputWriter::new(
                        work,
                        target.clone(),
                        index != 1 || job.node_only(),
                        cols,
                        Some(row_group_col),
                    )?);
                }
                writers[index]
                    .as_mut()
                    .context("missing aligned output writer")?
                    .write(batch)
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        if let Some(reason) = fast_wait(
            log,
            job,
            &phase,
            write_with_read_ahead(
                &mut cursors,
                target,
                row_group_col,
                retained_bytes,
                budget,
                write,
            ),
        )
        .await?
        {
            fallback!(reason);
        }
        nodes_written += 1;
        if nodes_written == 1
            || nodes_written % 5000 == 0
            || last_progress.elapsed() >= FAST_PROGRESS_INTERVAL
        {
            fast_progress(
                log,
                job,
                format!(
                    "progress: {nodes_written} nodes written (last {candidate:?}), {live_rows} live rows, removed {removed} duplicates; row groups read {}/{total_groups}; buffered={} bytes; elapsed {:.1}s",
                    cursors
                        .iter()
                        .map(|cursor| cursor.groups.groups_read())
                        .sum::<usize>(),
                    loaded_bytes(&cursors),
                    started.elapsed().as_secs_f64()
                ),
            );
            last_progress = Instant::now();
        }
    }
    for index in 0..cursors.len() {
        if cursors[index].current.is_some() {
            fallback!(format!(
                "source {} still has buffered rows after all aligned nodes were emitted",
                cursors[index].source.id
            ));
        }
        if !cursors[index].finished {
            let budget = ReadBudget::new(loaded_bytes(&cursors), work.node_bytes);
            match fast_wait(
                log,
                job,
                "verifying end of source",
                cursors[index].load_next(target, row_group_col, &budget, false),
            )
            .await?
            {
                FastLoad::Finished => {}
                FastLoad::Loaded => fallback!(format!(
                    "source {} has additional rows after alignment completed",
                    cursors[index].source.id
                )),
                FastLoad::OverBudget { required, budget } => fallback!(format!(
                    "source {} needs {required} bytes to load a row group, above the {budget}-byte node budget",
                    cursors[index].source.id
                )),
                FastLoad::Ineligible(reason) => fallback!(reason),
                FastLoad::Deferred => unreachable!("required reads cannot be deferred"),
            }
        }
    }
    let mut files = Vec::new();
    for writer in writers.into_iter().flatten() {
        files.push(fast_wait(log, job, "finalizing output file", writer.finish()).await?);
    }
    if files.iter().map(|file| file.rows).sum::<i64>() != live_rows - removed {
        bail!("aligned repack row count mismatch");
    }
    fast_progress(
        log,
        job,
        format!(
            "aligned fastpack completed successfully: {live_rows} live rows, removed {removed} duplicates, wrote {} files without temporary sort runs in {:.1}s",
            files.len(),
            started.elapsed().as_secs_f64()
        ),
    );
    Ok(Some((files, live_rows, removed)))
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
    if let Some((files, _, _)) =
        try_aligned(cfg, meta, sources, job, work, &target, &cols, log).await?
    {
        return Ok(files);
    }
    if !work.outputs.is_empty() {
        work.discard_outputs().await?;
    }
    let mut runs: [_; 3] = std::array::from_fn(|_| sort::Runs::for_report(job.report()));
    if let Some(unique) = &job.unique {
        runs[1].set_unique(unique);
    }
    let mut live_rows = 0i64;
    for (index, source) in sources.iter().enumerate() {
        let started = std::time::Instant::now();
        progress(
            log,
            format!(
                "reading source {}/{}: id={}, rows={}, deletes={}",
                index + 1,
                sources.len(),
                source.id,
                source.rows,
                source.delete.as_ref().map_or(0, |d| d.count)
            ),
        );
        let mut deleted = Deletes::open(cfg, meta, source)
            .await
            .with_context(|| format!("opening deletes for source {}", source.id))?;
        let full = location(meta, &source.path, source.relative);
        let (store, path) = object_store_for(cfg, &full)?;
        let mut stream = sort::reader(store, ObjPath::from(path))
            .await
            .with_context(|| format!("opening source {} at {full}", source.id))?;
        let mut offset = 0i64;
        while let Some(batch) = stream
            .try_next()
            .await
            .with_context(|| format!("reading source {} at row {offset}", source.id))?
        {
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
        progress(
            log,
            format!(
                "read source {}: {offset} rows in {:.1}s; total live rows={live_rows}",
                source.id,
                started.elapsed().as_secs_f64()
            ),
        );
    }
    let mut files = Vec::new();
    let mut removed = 0i64;
    for (i, run) in runs.into_iter().enumerate() {
        progress(
            log,
            format!("merging {} region", ["before", "in-range", "after"][i]),
        );
        let run = if i == 1
            && let Some(unique) = &job.unique
        {
            let (run, count) = run.deduplicate(work, unique).await?;
            removed = count;
            progress(
                log,
                format!("deduplicated by {unique:?}: removed {removed} rows"),
            );
            run
        } else {
            run
        };
        if let Some(file) = run.finish(work, i != 1 || job.node_only(), &cols).await? {
            progress(
                log,
                format!(
                    "{}: {} rows, {} bytes in {}",
                    ["before", "in-range", "after"][i],
                    file.rows,
                    file.size,
                    file.name
                ),
            );
            files.push(file);
        }
    }
    if files.iter().map(|f| f.rows).sum::<i64>() != live_rows - removed {
        bail!("repack row count mismatch");
    }
    Ok(files)
}

async fn lock_range(client: &Client, job: &Job) -> Result<()> {
    let (schema, table) = job.names();
    let statement = client
        .prepare("SELECT pg_advisory_lock(hashtextextended($1, 0))")
        .await
        .context("preparing repack advisory locks")?;
    // Local dates match the job's half-open bounds even across DST. Acquire
    // chronologically to avoid deadlocks; keep session locks across commits.
    let mut date = job.begin_date;
    while date < job.end_date {
        let key = format!("rustciso.repack:{schema}.{table}:{date}");
        client
            .query_one(&statement, &[&key])
            .await
            .with_context(|| {
                format!("acquiring repack advisory lock for {schema}.{table} {date}")
            })?;
        date = date
            .succ_opt()
            .context("repack lock date is out of range")?;
    }
    Ok(())
}

async fn has_current_files(c: &impl GenericClient, table_id: i64, job: &Job) -> Result<bool> {
    let Some(files) = &job.files else {
        return Ok(true);
    };
    Ok(c.query_one(
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
    progress(
        log,
        format!(
            "{} [{}, {}) UTC: connecting to catalog",
            job.table,
            job.start()?,
            job.end()?
        ),
    );
    let mut client = connect(cfg).await?;
    progress(
        log,
        format!(
            "waiting for repack advisory locks on {} [{}, {}) America/Los_Angeles",
            job.table, job.begin_date, job.end_date
        ),
    );
    lock_range(&client, job).await?;
    progress(
        log,
        format!(
            "acquired repack advisory locks on {} [{}, {}) America/Los_Angeles",
            job.table, job.begin_date, job.end_date
        ),
    );
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
        progress(
            log,
            "none of the supplied files are current; skipping already completed repack".into(),
        );
        return Ok(());
    }
    let utc_col = meta
        .columns
        .get("utcbegin")
        .context("table has no utcbegin column")?
        .id;
    job.validate_schema(&schema(&meta)?)?;
    progress(
        log,
        format!(
            "checking inlined rows and source files for table {}",
            meta.table_id
        ),
    );
    check_inlined(&tx, meta.table_id, job).await?;
    let selected = sources(&tx, meta.table_id, utc_col, job).await?;
    tx.commit().await?;
    if selected.is_empty() {
        progress(log, "no source files overlap the requested range".into());
        return Ok(());
    }
    progress(
        log,
        format!(
            "repacking {} files; node-only row groups: {}",
            selected.len(),
            job.node_only()
        ),
    );
    let (store, prefix) = object_store_for(cfg, &meta.root)?;
    let mut work = Work::with_limits(store, prefix, cfg.repack_run_bytes, cfg.repack_node_bytes);
    progress(
        log,
        format!(
            "temporary prefix {}; sort-run target={} bytes; aligned node budget={} bytes",
            work.tmp, work.run_bytes, work.node_bytes
        ),
    );
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
    progress(
        log,
        format!(
            "cleanup complete; elapsed {:.1}s; success={}",
            started.elapsed().as_secs_f64(),
            result.is_ok()
        ),
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray};
    use datafusion::parquet::arrow::AsyncArrowWriter;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::file::properties::EnabledStatistics;
    use futures::stream::BoxStream;
    use object_store::buffered::BufWriter;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::ops::Range;

    #[derive(Debug, Default)]
    struct ReadActivity {
        active: AtomicUsize,
        maximum: AtomicUsize,
        calls: AtomicUsize,
    }

    impl ReadActivity {
        async fn observe(&self) {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug, Default)]
    struct ObservedStore {
        inner: InMemory,
        reads: ReadActivity,
        gate_reads_on_write: std::sync::atomic::AtomicBool,
        write_started: tokio::sync::Notify,
    }

    impl std::fmt::Display for ObservedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ObservedStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ObservedStore {
        async fn put_opts(
            &self,
            path: &ObjPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(path, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            path: &ObjPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.write_started.notify_one();
            self.inner.put_multipart_opts(path, options).await
        }

        async fn get_opts(
            &self,
            path: &ObjPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.reads.observe().await;
            self.inner.get_opts(path, options).await
        }

        async fn get_ranges(
            &self,
            path: &ObjPath,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            if self.gate_reads_on_write.load(Ordering::SeqCst) {
                self.write_started.notified().await;
            }
            self.reads.observe().await;
            self.inner.get_ranges(path, ranges).await
        }

        fn delete_stream(
            &self,
            paths: BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(paths)
        }

        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn test_meta() -> TableMeta {
        TableMeta {
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
        }
    }

    fn test_job(unique: Option<Vec<String>>) -> Job {
        Job {
            table: "rtprices".into(),
            begin_date: "2026-01-01".parse().unwrap(),
            end_date: "2026-02-01".parse().unwrap(),
            unique,
            files: None,
        }
    }

    async fn write_row_groups(
        store: &Arc<dyn ObjectStore>,
        path: &str,
        schema: SchemaRef,
        groups: &[Vec<(&str, i64)>],
        start: i64,
    ) {
        let writer_properties = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_statistics_truncate_length(None)
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(None)
            .build();
        let path = ObjPath::from(path);
        let mut writer = AsyncArrowWriter::try_new(
            BufWriter::new(store.clone(), path),
            schema.clone(),
            Some(writer_properties),
        )
        .unwrap();
        for group in groups {
            let nodes: Vec<_> = group.iter().map(|(node, _)| *node).collect();
            let times: Vec<_> = group
                .iter()
                .map(|(_, seconds)| start + seconds * 1_000_000)
                .collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(nodes)),
                    Arc::new(TimestampMicrosecondArray::from(times).with_timezone("UTC")),
                ],
            )
            .unwrap();
            writer.write(&batch).await.unwrap();
            writer.flush().await.unwrap();
        }
        writer.close().await.unwrap();
    }

    async fn fast_cursor(store: Arc<dyn ObjectStore>, path: &str, source: Source) -> FastCursor {
        let groups = sort::RowGroupFile::open(store, ObjPath::from(path), "node")
            .await
            .unwrap();
        let groups = match groups {
            sort::RowGroupOpen::Eligible(groups) => groups,
            sort::RowGroupOpen::Ineligible(reason) => panic!("ineligible test file: {reason}"),
        };
        FastCursor {
            source: source.clone(),
            deletes: Deletes {
                stream: None,
                positions: Vec::new(),
                index: 0,
                previous: None,
                count: 0,
                expected: 0,
                source_rows: source.rows,
            },
            groups,
            row_offset: 0,
            current: None,
            finished: false,
        }
    }

    fn test_source(id: i64, path: &str, rows: i64, row_id_start: i64) -> Source {
        Source {
            id,
            path: path.into(),
            relative: false,
            rows,
            row_id_start: Some(row_id_start),
            begin_snapshot: 42,
            delete: None,
        }
    }

    #[tokio::test]
    async fn fastpack_batches_reads_with_bounded_concurrency() {
        let observed = Arc::new(ObservedStore::default());
        let store: Arc<dyn ObjectStore> = observed.clone();
        let data_schema = schema(&test_meta()).unwrap();
        let target = with_identity(&data_schema);
        let start = test_job(None).start().unwrap().timestamp_micros();
        let mut cursors = Vec::new();
        for id in 0..17 {
            let path = format!("source-{id}.parquet");
            write_row_groups(&store, &path, data_schema.clone(), &[vec![("A", 1)]], start).await;
            cursors.push(fast_cursor(store.clone(), &path, test_source(id, &path, 1, id)).await);
        }
        observed.reads.maximum.store(0, Ordering::SeqCst);
        observed.reads.calls.store(0, Ordering::SeqCst);
        let limit = crate::config::DEFAULT_REPACK_NODE_BYTES;
        assert!(
            fill_cursors(&mut cursors, &target, "node", 0, limit, false)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(observed.reads.calls.load(Ordering::SeqCst), 17);
        assert_eq!(
            observed.reads.maximum.load(Ordering::SeqCst),
            FAST_READ_CONCURRENCY
        );
        assert_eq!(observed.reads.active.load(Ordering::SeqCst), 0);
        assert!(loaded_bytes(&cursors) <= limit);
        assert!(
            cursors
                .iter()
                .all(|cursor| cursor.current.as_ref().unwrap().batch.num_rows() == 1)
        );
    }

    #[tokio::test]
    async fn fastpack_read_ahead_defers_when_output_uses_the_budget() {
        let observed = Arc::new(ObservedStore::default());
        let store: Arc<dyn ObjectStore> = observed.clone();
        let data_schema = schema(&test_meta()).unwrap();
        let target = with_identity(&data_schema);
        let start = test_job(None).start().unwrap().timestamp_micros();
        write_row_groups(
            &store,
            "source.parquet",
            data_schema,
            &[vec![("A", 1)]],
            start,
        )
        .await;
        let cursor = fast_cursor(
            store.clone(),
            "source.parquet",
            test_source(1, "source.parquet", 1, 100),
        )
        .await;
        let mut cursors = vec![cursor];
        observed.reads.calls.store(0, Ordering::SeqCst);
        let limit = crate::config::DEFAULT_REPACK_NODE_BYTES;
        assert!(
            fill_cursors(&mut cursors, &target, "node", limit, limit, true)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(observed.reads.calls.load(Ordering::SeqCst), 0);
        assert_eq!(cursors[0].groups.groups_read(), 0);
        assert!(cursors[0].current.is_none());
        assert!(
            fill_cursors(&mut cursors, &target, "node", 0, limit, false)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(cursors[0].groups.groups_read(), 1);
        assert!(cursors[0].current.is_some());
    }

    #[tokio::test]
    async fn fastpack_reads_ahead_while_an_output_write_is_in_flight() {
        use tokio::io::AsyncWriteExt;
        let observed = Arc::new(ObservedStore::default());
        let store: Arc<dyn ObjectStore> = observed.clone();
        let data_schema = schema(&test_meta()).unwrap();
        let target = with_identity(&data_schema);
        let start = test_job(None).start().unwrap().timestamp_micros();
        write_row_groups(
            &store,
            "source.parquet",
            data_schema,
            &[vec![("A", 1)]],
            start,
        )
        .await;
        let mut cursors = vec![
            fast_cursor(
                store.clone(),
                "source.parquet",
                test_source(1, "source.parquet", 1, 100),
            )
            .await,
        ];
        observed.gate_reads_on_write.store(true, Ordering::SeqCst);
        let mut writer = BufWriter::with_capacity(store.clone(), ObjPath::from("output"), 1);
        let write = async {
            writer.write_all(b"output").await?;
            writer.shutdown().await?;
            Ok(())
        };
        // The row-group read cannot complete until the concurrent write starts a multipart upload.
        let reason = tokio::time::timeout(
            Duration::from_secs(5),
            write_with_read_ahead(
                &mut cursors,
                &target,
                "node",
                1024,
                crate::config::DEFAULT_REPACK_NODE_BYTES,
                write,
            ),
        )
        .await
        .expect("read-ahead serialized behind writing")
        .unwrap();
        assert!(reason.is_none());
        assert!(cursors[0].current.is_some());
        assert_eq!(
            store
                .get(&ObjPath::from("output"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"output")
        );
    }

    #[tokio::test]
    async fn fastpack_parallel_read_errors_are_not_fallbacks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let data_schema = schema(&test_meta()).unwrap();
        let target = with_identity(&data_schema);
        let start = test_job(None).start().unwrap().timestamp_micros();
        write_row_groups(
            &store,
            "source.parquet",
            data_schema,
            &[vec![("A", 1)]],
            start,
        )
        .await;
        let mut cursors = vec![
            fast_cursor(
                store.clone(),
                "source.parquet",
                test_source(1, "source.parquet", 1, 100),
            )
            .await,
        ];
        store
            .delete(&ObjPath::from("source.parquet"))
            .await
            .unwrap();
        let error = fill_cursors(
            &mut cursors,
            &target,
            "node",
            0,
            crate::config::DEFAULT_REPACK_NODE_BYTES,
            false,
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("reading row group for source 1"));
    }

    #[tokio::test]
    async fn fastpack_write_errors_do_not_leave_read_ahead_running() {
        let observed = Arc::new(ObservedStore::default());
        let store: Arc<dyn ObjectStore> = observed.clone();
        let data_schema = schema(&test_meta()).unwrap();
        let target = with_identity(&data_schema);
        let start = test_job(None).start().unwrap().timestamp_micros();
        write_row_groups(
            &store,
            "source.parquet",
            data_schema,
            &[vec![("A", 1)]],
            start,
        )
        .await;
        let mut cursors = vec![
            fast_cursor(
                store,
                "source.parquet",
                test_source(1, "source.parquet", 1, 100),
            )
            .await,
        ];
        let error = write_with_read_ahead(
            &mut cursors,
            &target,
            "node",
            0,
            crate::config::DEFAULT_REPACK_NODE_BYTES,
            async { bail!("injected output write failure") },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("injected output write failure"));
        assert!(cursors[0].current.is_some());
        assert_eq!(observed.reads.active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fastpack_read_reservations_include_in_flight_and_retained_bytes() {
        let budget = ReadBudget::new(40, 100);
        assert!(budget.reserve(30).is_ok());
        assert!(budget.reserve(30).is_ok());
        assert!(matches!(
            budget.reserve(1),
            Err(FastLoad::OverBudget {
                required: 101,
                budget: 100
            })
        ));
        budget.release(30);
        assert!(budget.reserve(30).is_ok());
        assert!(budget.reserve(usize::MAX).is_err());
        assert_eq!(budget.used.load(Ordering::Relaxed), 100);
    }

    #[tokio::test]
    async fn fastpack_wait_logs_identify_the_job_and_phase() {
        let job = test_job(None);
        let mut log = Vec::new();
        fast_wait(
            &mut log,
            &job,
            "opening metadata: 0/24 sources",
            tokio::time::sleep(FAST_PROGRESS_INTERVAL + Duration::from_secs(1)),
        )
        .await;
        assert!(log.iter().any(|message| message.contains(
            "rtprices [2026-01-01, 2026-02-01) fastpack: opening metadata: 0/24 sources; waiting"
        )));
    }

    #[tokio::test]
    async fn aligned_repack_splits_mismatched_row_groups_without_temp_runs() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = test_meta();
        let data_schema = schema(&meta).unwrap();
        let target = with_identity(&data_schema);
        let cols = resolve_columns(&meta, &data_schema).unwrap();
        let job = test_job(None);
        let start = job.start().unwrap().timestamp_micros();
        write_row_groups(
            &store,
            "source-1.parquet",
            data_schema.clone(),
            &[vec![("A", 2), ("B", 1)], vec![("C", 3), ("E", 4)]],
            start,
        )
        .await;
        write_row_groups(
            &store,
            "source-2.parquet",
            data_schema,
            &[vec![("A", 1), ("B", 2), ("C", 1)], vec![("D", 3), ("E", 1)]],
            start,
        )
        .await;
        let cursors = vec![
            fast_cursor(
                store.clone(),
                "source-1.parquet",
                test_source(1, "source-1.parquet", 4, 100),
            )
            .await,
            fast_cursor(
                store.clone(),
                "source-2.parquet",
                test_source(2, "source-2.parquet", 5, 200),
            )
            .await,
        ];
        let mut work = Work::with_limits(
            store.clone(),
            "table/".into(),
            crate::config::DEFAULT_REPACK_RUN_BYTES,
            crate::config::DEFAULT_REPACK_NODE_BYTES,
        );
        let mut log = Vec::new();
        let (files, live_rows, removed) =
            align_cursors(cursors, &job, &mut work, &target, &cols, "node", &mut log)
                .await
                .unwrap()
                .unwrap();
        assert!(
            log.iter()
                .any(|message| message.contains("fastpack completed successfully"))
        );
        assert!(
            log.iter()
                .all(|message| message.starts_with("rtprices [2026-01-01, 2026-02-01) fastpack:"))
        );
        assert!(
            log.iter()
                .any(|message| message.contains("progress: 1 nodes written")
                    && message.contains("row groups read")
                    && message.contains("buffered="))
        );
        assert_eq!((live_rows, removed), (9, 0));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].rows, 9);
        assert!(work.temps.is_empty());
        let bytes = store
            .get(&ObjPath::from(format!("table/{}", files[0].name)))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 5);
        for group in builder.metadata().row_groups() {
            let stats = group.column(0).statistics().unwrap();
            assert_eq!(stats.min_bytes_opt(), stats.max_bytes_opt());
        }
        let batches = builder
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let output = concat_batches(&target, &batches).unwrap();
        assert_eq!(
            output
                .column(target.index_of("node").unwrap())
                .as_string::<i32>()
                .iter()
                .flatten()
                .collect::<Vec<_>>(),
            ["A", "A", "B", "B", "C", "C", "D", "E", "E"]
        );
        let times = output
            .column(target.index_of("utcbegin").unwrap())
            .as_primitive::<TimestampMicrosecondType>()
            .values();
        assert_eq!(
            times.iter().map(|value| *value - start).collect::<Vec<_>>(),
            [1, 2, 1, 2, 1, 3, 3, 1, 4].map(|seconds| seconds * 1_000_000)
        );
    }

    #[tokio::test]
    async fn aligned_repack_preserves_nodes_spanning_groups_and_skips_deleted_groups() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = test_meta();
        let data_schema = schema(&meta).unwrap();
        let target = with_identity(&data_schema);
        let cols = resolve_columns(&meta, &data_schema).unwrap();
        let job = test_job(None);
        let start = job.start().unwrap().timestamp_micros();
        write_row_groups(
            &store,
            "source-1.parquet",
            data_schema.clone(),
            &[vec![("A", 1)], vec![("A", 2), ("B", 1)], vec![("C", 1)]],
            start,
        )
        .await;
        write_row_groups(
            &store,
            "source-2.parquet",
            data_schema,
            &[vec![("A", 3), ("B", 2)], vec![("B", 3), ("C", 2)]],
            start,
        )
        .await;
        let mut first = fast_cursor(
            store.clone(),
            "source-1.parquet",
            test_source(1, "source-1.parquet", 4, 100),
        )
        .await;
        first.deletes.positions = vec![0];
        first.deletes.previous = Some(0);
        first.deletes.count = 1;
        first.deletes.expected = 1;
        let second = fast_cursor(
            store.clone(),
            "source-2.parquet",
            test_source(2, "source-2.parquet", 4, 200),
        )
        .await;
        let mut work = Work::new(store.clone(), "table/".into());
        let (files, live_rows, removed) = align_cursors(
            vec![first, second],
            &job,
            &mut work,
            &target,
            &cols,
            "node",
            &mut Vec::new(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!((live_rows, removed), (7, 0));
        assert!(work.temps.is_empty());
        let bytes = store
            .get(&ObjPath::from(format!("table/{}", files[0].name)))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 3);
        let batches = builder
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let output = concat_batches(&target, &batches).unwrap();
        assert_eq!(
            output
                .column(target.index_of("node").unwrap())
                .as_string::<i32>()
                .iter()
                .flatten()
                .collect::<Vec<_>>(),
            ["A", "A", "B", "B", "B", "C", "C"]
        );
        assert_eq!(
            output
                .column(target.index_of(ROW_ID).unwrap())
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .values()
                .as_ref(),
            [101, 200, 102, 201, 202, 103, 203]
        );
    }

    #[tokio::test]
    async fn aligned_repack_falls_back_on_unsorted_values_or_node_budget() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = test_meta();
        let data_schema = schema(&meta).unwrap();
        let target = with_identity(&data_schema);
        let cols = resolve_columns(&meta, &data_schema).unwrap();
        let job = test_job(None);
        let start = job.start().unwrap().timestamp_micros();
        write_row_groups(
            &store,
            "unsorted.parquet",
            data_schema.clone(),
            &[vec![("B", 1), ("A", 2)]],
            start,
        )
        .await;
        let cursor = fast_cursor(
            store.clone(),
            "unsorted.parquet",
            test_source(1, "unsorted.parquet", 2, 100),
        )
        .await;
        let mut work = Work::with_limits(store.clone(), "table/".into(), 1, 1024);
        let mut log = Vec::new();
        assert!(
            align_cursors(
                vec![cursor],
                &job,
                &mut work,
                &target,
                &cols,
                "node",
                &mut log
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(work.outputs.is_empty());
        assert!(work.temps.is_empty());
        assert!(
            log.iter()
                .any(|message| message.contains("source 1 row group 0"))
        );
        assert!(
            log.iter()
                .any(|message| message.contains("falling back to external sort"))
        );

        let cursor = fast_cursor(
            store,
            "unsorted.parquet",
            test_source(1, "unsorted.parquet", 2, 100),
        )
        .await;
        let mut work = Work::with_limits(Arc::new(InMemory::new()), "table/".into(), 1, 1024);
        work.node_bytes = 1;
        let mut log = Vec::new();
        assert!(
            align_cursors(
                vec![cursor],
                &job,
                &mut work,
                &target,
                &cols,
                "node",
                &mut log
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(work.outputs.is_empty());
        assert!(log.iter().any(|message| message.contains("node budget")));
    }

    #[tokio::test]
    async fn aligned_repack_closes_and_removes_candidates_before_budget_fallback() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = test_meta();
        let data_schema = schema(&meta).unwrap();
        let target = with_identity(&data_schema);
        let cols = resolve_columns(&meta, &data_schema).unwrap();
        let job = test_job(None);
        let start = job.start().unwrap().timestamp_micros();
        let large_group = std::iter::repeat(("B", 1))
            .take(200_000)
            .collect::<Vec<_>>();
        write_row_groups(
            &store,
            "budget-source.parquet",
            data_schema,
            &[vec![("A", 1)], large_group],
            start,
        )
        .await;
        let cursor = fast_cursor(
            store.clone(),
            "budget-source.parquet",
            test_source(1, "budget-source.parquet", 200_001, 100),
        )
        .await;
        let mut work = Work::with_limits(store.clone(), "table/".into(), 128, 1024 * 1024);
        assert!(
            align_cursors(
                vec![cursor],
                &job,
                &mut work,
                &target,
                &cols,
                "node",
                &mut Vec::new()
            )
            .await
            .unwrap()
            .is_none()
        );
        assert_eq!(work.outputs.len(), 1);
        work.discard_outputs().await.unwrap();
        assert!(work.outputs.is_empty());
        assert!(
            store
                .list(Some(&ObjPath::from("table/")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn aligned_repack_deduplicates_compatible_unique_keys_by_latest_row() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
                (
                    "downloaded_at".into(),
                    Column {
                        id: 33,
                        ty: "timestamptz".into(),
                    },
                ),
            ]),
        };
        let data_schema = schema(&meta).unwrap();
        let target = with_identity(&data_schema);
        let cols = resolve_columns(&meta, &data_schema).unwrap();
        let job = test_job(Some(vec!["node".into(), "utcbegin".into()]));
        let start = job.start().unwrap().timestamp_micros();
        let path = ObjPath::from("unique-source.parquet");
        let properties = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_statistics_truncate_length(None)
            .build();
        let mut writer = AsyncArrowWriter::try_new(
            BufWriter::new(store.clone(), path.clone()),
            data_schema.clone(),
            Some(properties),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(StringArray::from(vec!["A", "A", "A", "B"])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        start + 1_000_000,
                        start + 1_000_000,
                        start + 1_000_000,
                        start + 2_000_000,
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        start + 1_000_000,
                        start + 3_000_000,
                        start + 3_000_000,
                        start + 2_000_000,
                    ])
                    .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
        let source = test_source(1, "unique-source.parquet", 4, 100);
        let cursor = fast_cursor(store.clone(), "unique-source.parquet", source).await;
        let mut work = Work::with_limits(
            store.clone(),
            "table/".into(),
            crate::config::DEFAULT_REPACK_RUN_BYTES,
            crate::config::DEFAULT_REPACK_NODE_BYTES,
        );
        let (files, live_rows, removed) = align_cursors(
            vec![cursor],
            &job,
            &mut work,
            &target,
            &cols,
            "node",
            &mut Vec::new(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!((live_rows, removed), (4, 2));
        assert_eq!(files[0].rows, 2);
        let bytes = store
            .get(&ObjPath::from(format!("table/{}", files[0].name)))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let batches = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let output = concat_batches(&target, &batches).unwrap();
        assert_eq!(
            output
                .column(target.index_of(ROW_ID).unwrap())
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .values()
                .as_ref(),
            [102, 103]
        );
    }

    #[tokio::test]
    async fn aligned_repack_preserves_boundary_rows_and_applies_positional_deletes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = test_meta();
        let data_schema = schema(&meta).unwrap();
        let target = with_identity(&data_schema);
        let cols = resolve_columns(&meta, &data_schema).unwrap();
        let job = test_job(None);
        let start = job.start().unwrap().timestamp_micros();
        let end_seconds = (job.end().unwrap() - job.start().unwrap()).num_seconds();
        write_row_groups(
            &store,
            "boundary-source.parquet",
            data_schema,
            &[vec![("A", -1), ("A", 1), ("A", end_seconds + 1), ("B", 2)]],
            start,
        )
        .await;
        let source = test_source(1, "boundary-source.parquet", 4, 100);
        let mut cursor =
            fast_cursor(store.clone(), "boundary-source.parquet", source.clone()).await;
        cursor.deletes = Deletes {
            stream: None,
            positions: vec![1],
            index: 0,
            previous: Some(1),
            count: 1,
            expected: 1,
            source_rows: source.rows,
        };
        let mut work = Work::with_limits(
            store.clone(),
            "table/".into(),
            crate::config::DEFAULT_REPACK_RUN_BYTES,
            crate::config::DEFAULT_REPACK_NODE_BYTES,
        );
        let (files, live_rows, removed) = align_cursors(
            vec![cursor],
            &job,
            &mut work,
            &target,
            &cols,
            "node",
            &mut Vec::new(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!((live_rows, removed), (3, 0));
        assert_eq!(
            files.iter().map(|file| file.rows).collect::<Vec<_>>(),
            [1, 1, 1]
        );
        let output_nodes = futures::future::try_join_all(files.iter().map(|file| async {
            let bytes = store
                .get(&ObjPath::from(format!("table/{}", file.name)))
                .await?
                .bytes()
                .await?;
            let batches = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            Ok::<_, object_store::Error>(
                concat_batches(&target, &batches)
                    .unwrap()
                    .column(target.index_of("node").unwrap())
                    .as_string::<i32>()
                    .value(0)
                    .to_string(),
            )
        }))
        .await
        .unwrap();
        assert_eq!(output_nodes, ["A", "B", "A"]);
    }

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
        (
            client,
            tokio::spawn(async move { connection.await.unwrap() }),
        )
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
        }))
        .unwrap();
        lock_range(&owner, &job).await.unwrap();
        let owner_pid: i32 = owner
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        let waiter_pid: i32 = waiter
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        let held: i64 = other.query_one(
            "SELECT count(*) FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND granted",
            &[&owner_pid],
        ).await.unwrap().get(0);
        assert_eq!(held, 31);
        let alias: Job = serde_json::from_value(serde_json::json!({
            "table": "ciso.rtprices", "begin_date": "2026-01-15", "end_date": "2026-02-15"
        }))
        .unwrap();
        let mut waiting = tokio::spawn(async move {
            lock_range(&waiter, &alias).await.unwrap();
            waiter
        });
        timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = other
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks
                      WHERE pid = $1 AND locktype = 'advisory' AND NOT granted)",
                        &[&waiter_pid],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if blocked {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("overlapping job should wait for a date lock");
        assert!(!waiting.is_finished());
        // A commit must not release the lock while sorting or between retries.
        owner.transaction().await.unwrap().commit().await.unwrap();
        assert!(
            timeout(Duration::from_millis(100), &mut waiting)
                .await
                .is_err()
        );
        for (table, begin, end, expected_days) in [
            ("ciso.fmprices", "2026-01-01", "2026-02-01", 31),
            ("ciso.rtprices", "2025-12-01", "2026-01-01", 31),
            ("ciso.rtprices", "2026-02-01", "2026-03-01", 28),
            ("ciso.rtprices", "2026-03-07", "2026-03-10", 3),
            ("ciso.rtprices", "2026-10-31", "2026-11-03", 3),
        ] {
            let other_job: Job = serde_json::from_value(serde_json::json!({
                "table": table, "begin_date": begin, "end_date": end
            }))
            .unwrap();
            timeout(Duration::from_secs(5), lock_range(&other, &other_job))
                .await
                .unwrap()
                .unwrap();
            let held: i64 = other
                .query_one(
                    "SELECT count(*) FROM pg_locks
                  WHERE pid = pg_backend_pid() AND locktype = 'advisory' AND granted",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(held, expected_days);
            other
                .query_one("SELECT pg_advisory_unlock_all()", &[])
                .await
                .unwrap();
        }
        drop(other);
        other_connection.await.unwrap();
        // Early returns/errors drop the owning client just like normal completion.
        drop(owner);
        owner_connection.await.unwrap();
        let waiter = timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        let held: i64 = waiter
            .query_one(
                "SELECT count(*) FROM pg_locks
              WHERE pid = pg_backend_pid() AND locktype = 'advisory' AND granted",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(held, 31);
        drop(waiter);
        waiter_connection.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires MAINTENANCE_TEST_DATABASE_URL pointing to a test Postgres without a ducklake schema"]
    async fn postgres_file_guard_checks_any_current_path_in_the_target_table() {
        let (mut client, connection) = test_database().await;
        let tx = client.transaction().await.unwrap();
        tx.batch_execute(
            "
            CREATE SCHEMA ducklake;
            CREATE TABLE ducklake.ducklake_data_file (
                table_id bigint, path text, end_snapshot bigint
            );
            INSERT INTO ducklake.ducklake_data_file VALUES
                (1, 'daily-1.parquet', NULL),
                (1, 'daily-2.parquet', 10),
                (1, 'monthly.parquet', NULL),
                (2, 'other-table.parquet', NULL);
        ",
        )
        .await
        .unwrap();
        let mut job: Job = serde_json::from_value(serde_json::json!({
            "table": "rtprices", "begin_date": "2026-01-01", "end_date": "2026-02-01"
        }))
        .unwrap();
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
        )
        .await
        .unwrap();
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
        let tx = client
            .build_transaction()
            .read_only(true)
            .start()
            .await
            .unwrap();
        let meta = table_meta(&tx, "ciso", "rtprices").await.unwrap();
        let count: i64 = tx
            .query_one(
                "SELECT count(*) FROM ducklake.ducklake_inlined_data_tables WHERE table_id = $1",
                &[&meta.table_id],
            )
            .await
            .unwrap()
            .get(0);
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
