//! `copalctl top`: the deployment live in a terminal.
//!
//! One screen, polled on an interval through the admin surface: a
//! figure strip (tenants, files, bytes), the tenant population, and
//! the audit tail. Rendering is a pure function over a snapshot, so
//! the golden tests draw into a test backend and never need a
//! terminal; only the run loop touches one.

use anyhow::Context as _;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::Frame;
use serde_json::Value;

use crate::Api;

/// Everything one frame shows.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub tenants: Vec<Value>,
    pub audit: Vec<Value>,
}

impl Snapshot {
    fn files(&self) -> i64 {
        self.tenants
            .iter()
            .filter_map(|row| row.get("files").and_then(Value::as_i64))
            .sum()
    }

    fn bytes(&self) -> i64 {
        self.tenants
            .iter()
            .filter_map(|row| row.get("bytes").and_then(Value::as_i64))
            .sum()
    }
}

/// One poll of the admin surface.
pub async fn fetch(api: &Api) -> anyhow::Result<Snapshot> {
    let tenants = api
        .admin_get("/v1/admin/tenants", &[])
        .await
        .context("tenant population")?;
    let (audit, _) = api
        .admin_text("/v1/admin/audit/export", &[("limit", "18".to_owned())])
        .await
        .context("audit tail")?;
    Ok(Snapshot {
        tenants: tenants["items"].as_array().cloned().unwrap_or_default(),
        audit: audit
            .lines()
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect(),
    })
}

fn accent() -> Style {
    Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD)
}

fn dim() -> Style {
    Style::default().fg(Color::DarkGray)
}

fn field<'a>(row: &'a Value, key: &str) -> &'a str {
    row.get(key).and_then(Value::as_str).unwrap_or("")
}

/// Draw one frame from a snapshot. Pure over its inputs.
pub fn render(frame: &mut Frame, snapshot: &Snapshot, interval_secs: u64) {
    let [strip, tenants_area, audit_area, footer] = split(frame.area());

    let figures = Line::from(vec![
        Span::styled(format!(" {} ", snapshot.tenants.len()), accent()),
        Span::raw("tenants   "),
        Span::styled(format!(" {} ", snapshot.files()), accent()),
        Span::raw("files   "),
        Span::styled(format!(" {} ", snapshot.bytes()), accent()),
        Span::raw("bytes"),
    ]);
    frame.render_widget(
        Paragraph::new(figures).block(Block::default().borders(Borders::BOTTOM)),
        strip,
    );

    let tenant_rows: Vec<Row> = snapshot
        .tenants
        .iter()
        .map(|row| {
            Row::new(vec![
                Cell::from(field(row, "tenant_id").to_owned()),
                Cell::from(
                    row.get("files")
                        .and_then(Value::as_i64)
                        .unwrap_or(0)
                        .to_string(),
                ),
                Cell::from(
                    row.get("bytes")
                        .and_then(Value::as_i64)
                        .unwrap_or(0)
                        .to_string(),
                ),
            ])
        })
        .collect();
    let tenants_table = Table::new(
        tenant_rows,
        [
            Constraint::Percentage(50),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ],
    )
    .header(Row::new(vec!["tenant", "files", "bytes"]).style(dim()))
    .block(Block::default().borders(Borders::NONE).title(" tenants "));
    frame.render_widget(tenants_table, tenants_area);

    let audit_rows: Vec<Row> = snapshot
        .audit
        .iter()
        .rev()
        .map(|row| {
            Row::new(vec![
                Cell::from(field(row, "created_at").to_owned()).style(dim()),
                Cell::from(field(row, "tenant_id").to_owned()),
                Cell::from(field(row, "action").to_owned()),
                Cell::from(field(row, "subject").to_owned()).style(dim()),
            ])
        })
        .collect();
    let audit_table = Table::new(
        audit_rows,
        [
            Constraint::Length(27),
            Constraint::Percentage(20),
            Constraint::Percentage(30),
            Constraint::Percentage(35),
        ],
    )
    .header(Row::new(vec!["at", "tenant", "action", "subject"]).style(dim()))
    .block(
        Block::default()
            .borders(Borders::NONE)
            .title(" audit tail "),
    );
    frame.render_widget(audit_table, audit_area);

    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            format!(" q quits · polls every {interval_secs}s"),
            dim(),
        )))
        .block(Block::default().borders(Borders::TOP)),
        footer,
    );
}

fn split(area: Rect) -> [Rect; 4] {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Percentage(40),
            Constraint::Min(4),
            Constraint::Length(2),
        ])
        .split(area);
    [chunks[0], chunks[1], chunks[2], chunks[3]]
}

/// The live loop: draw, wait out the interval while watching for
/// `q`, poll again. Owns the terminal for its lifetime and restores
/// it on every exit path.
pub async fn run(api: &Api, interval_secs: u64) -> anyhow::Result<()> {
    let mut terminal = ratatui::try_init().context("terminal setup")?;
    let outcome = live(api, interval_secs, &mut terminal).await;
    ratatui::try_restore().context("terminal restore")?;
    outcome
}

async fn live(
    api: &Api,
    interval_secs: u64,
    terminal: &mut ratatui::DefaultTerminal,
) -> anyhow::Result<()> {
    use crossterm::event::{Event, KeyCode};

    loop {
        let snapshot = fetch(api).await.unwrap_or_default();
        terminal.draw(|frame| render(frame, &snapshot, interval_secs))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(interval_secs);
        while std::time::Instant::now() < deadline {
            if crossterm::event::poll(std::time::Duration::from_millis(150))? {
                if let Event::Key(key) = crossterm::event::read()? {
                    if matches!(key.code, KeyCode::Char('q') | KeyCode::Esc) {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use serde_json::json;

    fn seeded() -> Snapshot {
        Snapshot {
            tenants: vec![
                json!({ "tenant_id": "acme", "files": 12, "bytes": 4096 }),
                json!({ "tenant_id": "beta", "files": 3, "bytes": 512 }),
            ],
            audit: vec![
                json!({
                    "created_at": "2026-08-03T10:00:00Z", "tenant_id": "acme",
                    "action": "key.minted", "subject": "ck1_ci",
                }),
                json!({
                    "created_at": "2026-08-03T10:05:00Z", "tenant_id": "beta",
                    "action": "grant.issued", "subject": "grant_a",
                }),
            ],
        }
    }

    fn frame_text(snapshot: &Snapshot) -> String {
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| render(frame, snapshot, 5)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let mut out = String::new();
        for y in 0..buffer.area.height {
            for x in 0..buffer.area.width {
                out.push_str(buffer.cell((x, y)).unwrap().symbol());
            }
            out.push('\n');
        }
        out
    }

    #[test]
    fn the_frame_carries_the_deployment() {
        let text = frame_text(&seeded());
        assert!(text.contains("2"), "tenant figure renders");
        assert!(text.contains("15"), "file total sums across tenants");
        assert!(text.contains("4608"), "byte total sums across tenants");
        assert!(text.contains("acme"), "tenants list");
        assert!(text.contains("key.minted"), "the audit tail shows custody");
        assert!(text.contains("q quits"), "the footer explains itself");
    }

    #[test]
    fn an_empty_deployment_still_draws() {
        let text = frame_text(&Snapshot::default());
        assert!(text.contains("0"), "zeros render rather than crash");
        assert!(text.contains("tenants"));
    }
}
