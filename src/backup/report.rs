use std::io::Write;
use std::process::{Command, Stdio};

use chrono::{DateTime, Local};

use crate::config::model::ReportOptions;
use crate::error::{Result, TimevaultError};

#[derive(Debug, Clone)]
pub struct BackupRunReport {
    pub disk_id: String,
    pub mountpoint: String,
    pub started_at: DateTime<Local>,
    pub finished_at: DateTime<Local>,
    pub jobs: Vec<BackupJobReport>,
}

#[derive(Debug, Clone)]
pub struct BackupJobReport {
    pub name: String,
    pub description: Option<String>,
    pub source: String,
    pub destination: String,
    pub backup_day: String,
    pub status: BackupJobStatus,
    pub attempts: usize,
    pub rsync_code: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupJobStatus {
    Success,
    Partial,
    Failed,
    Skipped,
}

impl BackupJobStatus {
    fn as_str(self) -> &'static str {
        match self {
            BackupJobStatus::Success => "success",
            BackupJobStatus::Partial => "partial",
            BackupJobStatus::Failed => "failed",
            BackupJobStatus::Skipped => "skipped",
        }
    }
}

pub fn render_markdown(reports: &[BackupRunReport]) -> String {
    let summary = report_summary(reports);

    let mut body = String::new();
    body.push_str("# Timevault Backup Report\n\n");
    body.push_str(&format!(
        "- Started: {}\n",
        summary.started.format("%Y-%m-%d %H:%M:%S %Z")
    ));
    body.push_str(&format!(
        "- Finished: {}\n",
        summary.finished.format("%Y-%m-%d %H:%M:%S %Z")
    ));
    body.push_str(&format!("- Disk runs: {}\n", reports.len()));
    body.push_str(&format!("- Jobs: {}\n", summary.total_jobs));
    body.push_str(&format!("- Failed: {}\n", summary.failed_jobs));
    body.push_str(&format!("- Partial: {}\n\n", summary.partial_jobs));

    for report in reports {
        body.push_str(&format!(
            "## Disk `{}`\n\n",
            escape_markdown(&report.disk_id)
        ));
        body.push_str(&format!(
            "- Mountpoint: `{}`\n",
            escape_markdown(&report.mountpoint)
        ));
        body.push_str(&format!(
            "- Started: {}\n",
            report.started_at.format("%Y-%m-%d %H:%M:%S %Z")
        ));
        body.push_str(&format!(
            "- Finished: {}\n\n",
            report.finished_at.format("%Y-%m-%d %H:%M:%S %Z")
        ));
        body.push_str(
            "| Job | Name | Status | Rsync | Attempts | Snapshot | Source | Destination |\n",
        );
        body.push_str("| --- | --- | --- | ---: | ---: | --- | --- | --- |\n");
        for job in &report.jobs {
            body.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | `{}` | `{}` |\n",
                escape_markdown(job_display_name(job)),
                escape_markdown(&job.name),
                job.status.as_str(),
                job.rsync_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                job.attempts,
                escape_markdown(&job.backup_day),
                escape_markdown(&job.source),
                escape_markdown(&job.destination)
            ));
        }
        body.push('\n');
    }

    body
}

pub fn render_html(reports: &[BackupRunReport]) -> String {
    let summary = report_summary(reports);
    let mut body = String::new();

    body.push_str("<!doctype html>\n<html><head><meta charset=\"utf-8\"></head>");
    body.push_str("<body style=\"margin:0; padding:24px; background:#f6f8fb; font-family:Arial,sans-serif; color:#172033;\">");
    body.push_str("<div style=\"max-width:1100px; margin:0 auto;\">");
    body.push_str("<div style=\"background:#0f766e; color:#ffffff; padding:22px 24px; border-radius:8px 8px 0 0;\">");
    body.push_str(
        "<h1 style=\"font-size:24px; line-height:1.25; margin:0;\">Timevault Backup Report</h1>",
    );
    body.push_str("<p style=\"margin:8px 0 0; color:#ccfbf1;\">Automated backup summary</p>");
    body.push_str("</div>");
    body.push_str("<div style=\"background:#ffffff; border:1px solid #d8e2ea; border-top:0; padding:20px 24px;\">");
    body.push_str("<table cellpadding=\"0\" cellspacing=\"0\" style=\"border-collapse:collapse; width:100%; margin-bottom:22px;\"><tr>");
    body.push_str(&summary_card(
        "Started",
        &summary.started.format("%Y-%m-%d %H:%M:%S %Z").to_string(),
        "#e0f2fe",
        "#075985",
    ));
    body.push_str(&summary_card(
        "Finished",
        &summary.finished.format("%Y-%m-%d %H:%M:%S %Z").to_string(),
        "#dcfce7",
        "#166534",
    ));
    body.push_str(&summary_card(
        "Disk runs",
        &reports.len().to_string(),
        "#fef3c7",
        "#92400e",
    ));
    body.push_str(&summary_card(
        "Jobs",
        &summary.total_jobs.to_string(),
        "#ede9fe",
        "#5b21b6",
    ));
    body.push_str(&summary_card(
        "Failed",
        &summary.failed_jobs.to_string(),
        "#fee2e2",
        "#991b1b",
    ));
    body.push_str(&summary_card(
        "Partial",
        &summary.partial_jobs.to_string(),
        "#ffedd5",
        "#9a3412",
    ));
    body.push_str("</tr></table>");

    for report in reports {
        body.push_str(&format!(
            "<h2 style=\"font-size:18px; margin:24px 0 8px; color:#0f766e;\">Disk <code style=\"background:#eef7f6; padding:2px 5px; border-radius:4px;\">{}</code></h2>",
            escape_html(&report.disk_id)
        ));
        body.push_str("<p style=\"margin:0 0 12px; color:#465264;\">");
        body.push_str(&format!(
            "<strong>Mountpoint:</strong> <code style=\"background:#f1f5f9; padding:2px 5px; border-radius:4px;\">{}</code><br>",
            escape_html(&report.mountpoint)
        ));
        body.push_str(&format!(
            "<strong>Started:</strong> {}<br>",
            report.started_at.format("%Y-%m-%d %H:%M:%S %Z")
        ));
        body.push_str(&format!(
            "<strong>Finished:</strong> {}",
            report.finished_at.format("%Y-%m-%d %H:%M:%S %Z")
        ));
        body.push_str("</p>");
        body.push_str("<table cellpadding=\"8\" cellspacing=\"0\" style=\"border-collapse:collapse; width:100%; margin-bottom:18px; border:1px solid #d8e2ea;\">");
        body.push_str("<thead><tr>");
        for heading in [
            "Job",
            "Name",
            "Status",
            "Rsync",
            "Attempts",
            "Snapshot",
            "Source",
            "Destination",
        ] {
            body.push_str(&format!(
                "<th align=\"left\" style=\"background:#134e4a; color:#ffffff; border-bottom:1px solid #0f766e; font-size:12px; text-transform:uppercase; letter-spacing:.04em;\">{}</th>",
                heading
            ));
        }
        body.push_str("</tr></thead><tbody>");
        for (index, job) in report.jobs.iter().enumerate() {
            let row_bg = if index % 2 == 0 { "#ffffff" } else { "#f8fafc" };
            body.push_str(&format!("<tr style=\"background:{};\">", row_bg));
            body.push_str(&table_cell(job_display_name(job)));
            body.push_str(&table_cell(&job.name));
            body.push_str(&status_cell(job.status));
            body.push_str(&table_cell(
                &job.rsync_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ));
            body.push_str(&table_cell(&job.attempts.to_string()));
            body.push_str(&table_cell(&job.backup_day));
            body.push_str(&table_cell(&job.source));
            body.push_str(&table_cell(&job.destination));
            body.push_str("</tr>");
        }
        body.push_str("</tbody></table>");
    }

    body.push_str("</div></div></body></html>\n");
    body
}

pub fn email_html_report(options: &ReportOptions, html: &str) -> Result<()> {
    if options.email_to.trim().is_empty() {
        return Ok(());
    }
    let sendmail = options.sendmail.as_deref().unwrap_or("/usr/sbin/sendmail");
    let from_address = first_header_line(
        options
            .email_from
            .as_deref()
            .unwrap_or("timevault@localhost"),
    );
    let to = first_header_line(&options.email_to);
    println!("sending backup report to {}", to);
    println!("sendmail command: {} -t", sendmail);

    let mut child = Command::new(sendmail)
        .arg("-t")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| TimevaultError::message(format!("start sendmail {}: {}", sendmail, err)))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(build_email_message(&to, &from_address, html).as_bytes())?;
    }

    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(TimevaultError::message(format!(
            "sendmail failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

fn build_email_message(to: &str, from_address: &str, html: &str) -> String {
    let now = Local::now();
    let mut message = String::new();
    let from_address = header_address(from_address);

    message.push_str(&format!("To: {}\n", to));
    message.push_str(&format!("From: Timevault <{}>\n", from_address));
    message.push_str("Subject: Timevault backup report\n");
    message.push_str(&format!("Date: {}\n", now.to_rfc2822()));
    message.push_str(&format!(
        "Message-ID: <timevault-{}@{}>\n",
        now.timestamp_millis(),
        message_id_domain(&from_address)
    ));
    message.push_str("Auto-Submitted: auto-generated\n");
    message.push_str("MIME-Version: 1.0\n");
    message.push_str("Content-Type: text/html; charset=utf-8\n");
    message.push_str("Content-Transfer-Encoding: quoted-printable\n\n");
    message.push_str(&encode_quoted_printable(html));

    message
}

struct ReportSummary {
    started: DateTime<Local>,
    finished: DateTime<Local>,
    total_jobs: usize,
    failed_jobs: usize,
    partial_jobs: usize,
}

fn report_summary(reports: &[BackupRunReport]) -> ReportSummary {
    ReportSummary {
        started: reports
            .iter()
            .map(|report| report.started_at)
            .min()
            .unwrap_or_else(Local::now),
        finished: reports
            .iter()
            .map(|report| report.finished_at)
            .max()
            .unwrap_or_else(Local::now),
        total_jobs: reports
            .iter()
            .map(|report| report.jobs.len())
            .sum::<usize>(),
        failed_jobs: reports
            .iter()
            .flat_map(|report| &report.jobs)
            .filter(|job| job.status == BackupJobStatus::Failed)
            .count(),
        partial_jobs: reports
            .iter()
            .flat_map(|report| &report.jobs)
            .filter(|job| job.status == BackupJobStatus::Partial)
            .count(),
    }
}

fn summary_card(label: &str, value: &str, background: &str, color: &str) -> String {
    format!(
        "<td style=\"padding:0 8px 8px 0;\"><div style=\"background:{}; color:{}; border-radius:6px; padding:10px 12px;\"><div style=\"font-size:11px; text-transform:uppercase; letter-spacing:.04em; font-weight:bold;\">{}</div><div style=\"font-size:16px; font-weight:bold; margin-top:4px;\">{}</div></div></td>",
        background,
        color,
        escape_html(label),
        escape_html(value)
    )
}

fn table_cell(value: &str) -> String {
    format!(
        "<td style=\"border-bottom:1px solid #e5edf3; vertical-align:top; color:#263445;\">{}</td>",
        escape_html(value)
    )
}

fn job_display_name(job: &BackupJobReport) -> &str {
    job.description.as_deref().unwrap_or(&job.name)
}

fn status_cell(status: BackupJobStatus) -> String {
    let (background, color) = match status {
        BackupJobStatus::Success => ("#dcfce7", "#166534"),
        BackupJobStatus::Partial => ("#ffedd5", "#9a3412"),
        BackupJobStatus::Failed => ("#fee2e2", "#991b1b"),
        BackupJobStatus::Skipped => ("#e2e8f0", "#334155"),
    };
    format!(
        "<td style=\"border-bottom:1px solid #e5edf3; vertical-align:top;\"><span style=\"display:inline-block; background:{}; color:{}; border-radius:999px; padding:3px 9px; font-weight:bold; font-size:12px;\">{}</span></td>",
        background,
        color,
        status.as_str()
    )
}

fn header_address(value: &str) -> String {
    if value.contains('<') && value.contains('>') {
        value
            .split('<')
            .nth(1)
            .and_then(|rest| rest.split('>').next())
            .unwrap_or(value)
            .trim()
            .to_string()
    } else {
        value.trim().to_string()
    }
}

fn message_id_domain(address: &str) -> String {
    address
        .split('@')
        .nth(1)
        .map(|domain| {
            domain
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-'))
                .collect::<String>()
        })
        .filter(|domain| !domain.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

fn first_header_line(value: &str) -> String {
    value.lines().next().unwrap_or("").trim().to_string()
}

fn escape_markdown(value: &str) -> String {
    value.replace('|', "\\|")
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn encode_quoted_printable(value: &str) -> String {
    const MAX_LINE_LEN: usize = 76;
    let mut encoded = String::new();
    let mut line_len = 0usize;

    for &byte in value.as_bytes() {
        if byte == b'\n' {
            encoded.push('\n');
            line_len = 0;
            continue;
        }
        if byte == b'\r' {
            continue;
        }

        let chunk = quoted_printable_chunk(byte);
        if line_len + chunk.len() > MAX_LINE_LEN - 1 {
            encoded.push_str("=\n");
            line_len = 0;
        }
        encoded.push_str(&chunk);
        line_len += chunk.len();
    }

    if line_len > 0 {
        encoded.push('\n');
    }

    encoded
}

fn quoted_printable_chunk(byte: u8) -> String {
    match byte {
        b'\t' | b' '..=b'<' | b'>'..=b'~' => (byte as char).to_string(),
        b'=' => "=3D".to_string(),
        _ => format!("={:02X}", byte),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_message_uses_timevault_display_name_and_html_body() {
        let message = build_email_message(
            "admin@example.com",
            "timevault@example.com",
            "<h1>Timevault Backup Report</h1>\n",
        );

        assert!(message.contains("From: Timevault <timevault@example.com>\n"));
        assert!(message.contains("Content-Type: text/html; charset=utf-8\n"));
        assert!(message.contains("Content-Transfer-Encoding: quoted-printable\n"));
        assert!(!message.contains("Content-Type: multipart/alternative;"));
        assert!(!message.contains("Content-Type: text/plain; charset=utf-8\n"));
        assert!(message.contains("<h1>Timevault Backup Report</h1>\n"));
        assert!(!message.contains("Content-Disposition: attachment"));
    }

    #[test]
    fn email_message_replaces_configured_display_name() {
        let message = build_email_message("admin@example.com", "Backup <backup@example.com>", "ok");

        assert!(message.contains("From: Timevault <backup@example.com>\n"));
        assert!(message.contains("Message-ID: <timevault-"));
        assert!(message.contains("@example.com>\n"));
    }

    #[test]
    fn html_report_escapes_job_values() {
        let now = Local::now();
        let html = render_html(&[BackupRunReport {
            disk_id: "disk<&>".to_string(),
            mountpoint: "/mnt/backup".to_string(),
            started_at: now,
            finished_at: now,
            jobs: vec![BackupJobReport {
                name: "root <main>".to_string(),
                description: Some("Main filesystem".to_string()),
                source: "/source".to_string(),
                destination: "/dest".to_string(),
                backup_day: "20260601".to_string(),
                status: BackupJobStatus::Success,
                attempts: 1,
                rsync_code: Some(0),
            }],
        }]);

        assert!(html.contains("disk&lt;&amp;&gt;"));
        assert!(html.contains("root &lt;main&gt;"));
        assert!(html.contains("Main filesystem"));
        assert!(html.contains(">Job</th>"));
        assert!(html.contains(">Name</th>"));
        assert!(html.contains("background:#0f766e"));
        assert!(html.contains("background:#dcfce7"));
        assert!(html.contains("<table"));
    }

    #[test]
    fn quoted_printable_wraps_long_lines_for_smtp() {
        let long_value = "x".repeat(1200);
        let encoded = encode_quoted_printable(&long_value);

        assert!(encoded.lines().all(|line| line.len() <= 76));
        assert!(encoded.lines().any(|line| line.ends_with('=')));
    }
}
