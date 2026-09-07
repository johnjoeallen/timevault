//! An opt-in `npm install`-style progress renderer: one spinner line redrawn in
//! place on stderr, with "notes" printed as permanent lines on stdout that
//! scroll above it.
//!
//! It is a process-global singleton (like [`crate::mount::signals`]). When not
//! enabled (`--progress` off, not a TTY, or `--verbose`/`--dry-run`), [`note`]
//! is a plain `println!` and [`status`] does nothing, so callers can route all
//! run output through here unconditionally.

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const TICK: Duration = Duration::from_millis(90);

static RENDERER: OnceLock<Option<Arc<Renderer>>> = OnceLock::new();
static TICKER: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

struct Renderer {
    state: Mutex<State>,
    width: usize,
    running: AtomicBool,
}

#[derive(Default)]
struct State {
    status: String,
    frame: usize,
    drawn: bool,
}

fn renderer() -> Option<&'static Arc<Renderer>> {
    RENDERER.get().and_then(|slot| slot.as_ref())
}

/// Install the renderer once. `enabled` should already fold in the TTY check and
/// the `--verbose`/`--dry-run` exclusions.
pub fn init(enabled: bool) {
    let renderer = enabled.then(|| {
        let renderer = Arc::new(Renderer {
            state: Mutex::new(State::default()),
            width: terminal_width(),
            running: AtomicBool::new(true),
        });
        let ticker = Arc::clone(&renderer);
        let handle = thread::spawn(move || ticker_loop(&ticker));
        if let Ok(mut slot) = TICKER.lock() {
            *slot = Some(handle);
        }
        renderer
    });
    let _ = RENDERER.set(renderer);
}

pub fn enabled() -> bool {
    renderer().is_some()
}

/// Replace the transient spinner line's text. No-op when the renderer is off.
pub fn status<S: Into<String>>(text: S) {
    if let Some(renderer) = renderer() {
        if let Ok(mut state) = renderer.state.lock() {
            state.status = text.into();
            draw(renderer, &mut state);
        }
    }
}

/// Print a line that stays. Plain `println!` when the renderer is off; otherwise
/// the spinner is erased, the line is written to stdout, and the spinner redraws.
/// Prefer the [`crate::pnote!`] macro, which mirrors `println!`.
pub fn note<T: std::fmt::Display>(message: T) {
    note_fmt(format_args!("{}", message));
}

#[doc(hidden)]
pub fn note_fmt(args: std::fmt::Arguments) {
    match renderer() {
        Some(renderer) => {
            let Ok(mut state) = renderer.state.lock() else {
                println!("{}", args);
                return;
            };
            erase(&mut state);
            let mut out = io::stdout().lock();
            let _ = out.write_fmt(args);
            let _ = out.write_all(b"\n");
            let _ = out.flush();
            draw(renderer, &mut state);
        }
        None => println!("{}", args),
    }
}

/// `println!`-compatible: a preserved line that scrolls above the spinner.
#[macro_export]
macro_rules! pnote {
    ($($arg:tt)*) => { $crate::progress::note_fmt(std::format_args!($($arg)*)) };
}

/// `format!`-compatible: replace the transient spinner line's text.
#[macro_export]
macro_rules! pstatus {
    ($($arg:tt)*) => { $crate::progress::status(std::format!($($arg)*)) };
}

/// Erase the transient line (before exiting, on error, from the signal handler).
pub fn clear() {
    if let Some(renderer) = renderer() {
        if let Ok(mut state) = renderer.state.lock() {
            erase(&mut state);
        }
    }
}

/// Stop the ticker thread and clear the line. Call once at the end of a run.
pub fn shutdown() {
    if let Some(renderer) = renderer() {
        renderer.running.store(false, Ordering::Relaxed);
    }
    if let Ok(mut slot) = TICKER.lock() {
        if let Some(handle) = slot.take() {
            let _ = handle.join();
        }
    }
    clear();
}

fn ticker_loop(renderer: &Arc<Renderer>) {
    while renderer.running.load(Ordering::Relaxed) {
        thread::sleep(TICK);
        if !renderer.running.load(Ordering::Relaxed) {
            break;
        }
        if let Ok(mut state) = renderer.state.lock() {
            state.frame = state.frame.wrapping_add(1);
            draw(renderer, &mut state);
        }
    }
}

fn draw(renderer: &Renderer, state: &mut State) {
    if state.status.is_empty() {
        return;
    }
    let frame = FRAMES[state.frame % FRAMES.len()];
    let line = truncate(
        &format!("{} {}", frame, state.status),
        renderer.width.saturating_sub(1),
    );
    let mut err = io::stderr().lock();
    let _ = write!(err, "\r{}\x1b[K", line);
    let _ = err.flush();
    state.drawn = true;
}

fn erase(state: &mut State) {
    if state.drawn {
        let mut err = io::stderr().lock();
        let _ = write!(err, "\r\x1b[2K");
        let _ = err.flush();
        state.drawn = false;
    }
}

fn truncate(text: &str, max: usize) -> String {
    let count = text.chars().count();
    if count <= max || max == 0 {
        return text.to_string();
    }
    let keep = max.saturating_sub(1);
    let mut out: String = text.chars().take(keep).collect();
    out.push('…');
    out
}

fn terminal_width() -> usize {
    #[repr(C)]
    struct WinSize {
        rows: libc::c_ushort,
        cols: libc::c_ushort,
        x: libc::c_ushort,
        y: libc::c_ushort,
    }
    let mut ws = WinSize {
        rows: 0,
        cols: 0,
        x: 0,
        y: 0,
    };
    // SAFETY: ws is a valid, sized-out WinSize; ioctl only writes into it.
    let ok = unsafe { libc::ioctl(libc::STDERR_FILENO, libc::TIOCGWINSZ, &mut ws) } == 0;
    if ok && ws.cols > 0 {
        ws.cols as usize
    } else {
        80
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_before_init() {
        // In the test binary init() is never called for this module.
        assert!(!enabled());
        // Must not panic.
        status("hello");
        note("world");
        clear();
    }

    #[test]
    fn truncate_keeps_short_and_ellipsizes_long() {
        assert_eq!(truncate("short", 40), "short");
        assert_eq!(truncate("abcdefgh", 4), "abc…");
        assert_eq!(truncate("abc", 0), "abc");
    }
}
