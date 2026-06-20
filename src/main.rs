use clap::Parser;

#[derive(Parser)]
#[command(name = "ember-query", about = "A small vectorized SQL engine")]
struct Cli {
    #[arg(long)]
    input: String,

    #[arg(long)]
    sql: String,

    #[arg(long, default_value_t = false)]
    explain: bool,
}

fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();
    println!("ember-query — not yet implemented");
    Ok(())
}
