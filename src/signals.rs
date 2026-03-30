use std::thread;

use signal_hook::consts::{SIGINT, SIGPIPE, SIGTERM};
use signal_hook::iterator::Signals;

use crate::errors::{Error, Result};

pub fn install() -> Result<()> {
    install_sigpipe()?;
    install_sigint()?;
    Ok(())
}

fn install_sigpipe() -> Result<()> {
    #[cfg(unix)]
    unsafe {
        let result = libc::signal(SIGPIPE, libc::SIG_DFL);
        if result == libc::SIG_ERR {
            return Err(Error::message("failed to set SIGPIPE handler"));
        }
    }
    Ok(())
}

fn install_sigint() -> Result<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM])
        .map_err(|err| Error::message(format!("failed to register signal handlers: {err}")))?;

    thread::spawn(move || {
        for signal in signals.forever() {
            eprintln!("ria: received signal {signal}");
            std::process::exit(128 + signal);
        }
    });

    Ok(())
}
