use vergen::{Config, vergen};

fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Generate the default 'cargo:' instruction output
  vergen(Config::default())?;
  Ok(())
}
