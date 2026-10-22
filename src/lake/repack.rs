use crate::config::Config;
use anyhow::Result;

pub use dkplus::repack::Job;

pub async fn run(cfg: &Config, job: &Job, log: &mut Vec<String>) -> Result<()> {
    let mut job = job.clone();
    job.layout = super::repack_layout(&job.table);
    dkplus::repack::run(&cfg.lake(), &job, log).await
}
