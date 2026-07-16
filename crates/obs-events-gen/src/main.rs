#![forbid(unsafe_code)]
//! CLI over the obs-events-gen lib: generate / publish / verify.

use std::io::Write;

use obs_events_gen::{PublishConfig, Sim, SimConfig, Vocab};

fn usage() -> ! {
    eprintln!(
        "usage:
  obs-events-gen generate --seed N --events N [--vocab catalog|orch-asbuilt]
                 [--run-id ID] [--producer-id ID] [--inject-bad N] [--goal]
                 --out FILE
  obs-events-gen publish  --addr http://HOST:7470 --seed N --events N
                 [--vocab ...] [--run-id ID] [--producer-id ID]
                 [--inject-bad N] [--goal] [--rate N] [--bulk] [--resume]
                 [--profile firehose|bursty|reconnect-storm]
  obs-events-gen verify   --db PATH --seed N --events N [--vocab ...]
                 [--run-id ID] [--producer-id ID] [--inject-bad N] [--goal]"
    );
    std::process::exit(2);
}

struct Args {
    values: std::collections::BTreeMap<String, String>,
    flags: std::collections::BTreeSet<String>,
}

impl Args {
    fn parse(args: &[String]) -> Self {
        let mut values = std::collections::BTreeMap::new();
        let mut flags = std::collections::BTreeSet::new();
        let mut iter = args.iter().peekable();
        while let Some(arg) = iter.next() {
            let Some(name) = arg.strip_prefix("--") else {
                usage();
            };
            match iter.peek() {
                Some(next) if !next.starts_with("--") => {
                    values.insert(name.to_owned(), iter.next().unwrap().clone());
                }
                _ => {
                    flags.insert(name.to_owned());
                }
            }
        }
        Self { values, flags }
    }

    fn u64_or(&self, key: &str, default: u64) -> u64 {
        self.values
            .get(key)
            .map(|value| value.parse().unwrap_or_else(|_| usage()))
            .unwrap_or(default)
    }

    fn required(&self, key: &str) -> &str {
        self.values.get(key).map(String::as_str).unwrap_or_else(|| {
            eprintln!("missing --{key}");
            usage()
        })
    }
}

fn sim_config(args: &Args) -> SimConfig {
    let mut config = SimConfig::new(args.u64_or("seed", 1), args.u64_or("events", 10_000));
    if let Some(vocab) = args.values.get("vocab") {
        config.vocab = vocab.parse::<Vocab>().unwrap_or_else(|error| {
            eprintln!("{error}");
            usage()
        });
    }
    if let Some(run_id) = args.values.get("run-id") {
        config.run_id = run_id.clone();
    }
    if let Some(producer_id) = args.values.get("producer-id") {
        config.producer_id = producer_id.clone();
    }
    config.inject_bad = args.u64_or("inject-bad", 0);
    config.goal = args.flags.contains("goal");
    config
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some((mode, rest)) = argv.split_first() else {
        usage();
    };
    let args = Args::parse(rest);

    match mode.as_str() {
        "generate" => {
            let out = args.required("out").to_owned();
            let config = sim_config(&args);
            let file = std::fs::File::create(&out).unwrap_or_else(|error| {
                eprintln!("cannot create {out}: {error}");
                std::process::exit(1);
            });
            let mut writer = std::io::BufWriter::new(file);
            let mut sim = Sim::new(config);
            let mut lines = 0u64;
            for envelope in sim.by_ref() {
                writeln!(writer, "{}", obs_events_gen::to_jsonl_line(&envelope)).unwrap();
                lines += 1;
            }
            writer.flush().unwrap();
            eprintln!("wrote {lines} envelopes to {out}");
            eprintln!("counts: {:?}", sim.counts().by_type);
        }
        "publish" => {
            let addr = args.required("addr").to_owned();
            let config = sim_config(&args);
            let profile = args
                .values
                .get("profile")
                .map(String::as_str)
                .unwrap_or("firehose");
            let publish_config = PublishConfig {
                addr,
                rate: match profile {
                    "bursty" => Some(args.u64_or("rate", 2_000)),
                    _ => args
                        .values
                        .get("rate")
                        .map(|rate| rate.parse().unwrap_or_else(|_| usage())),
                },
                bulk: args.flags.contains("bulk"),
                resume: args.flags.contains("resume") || profile == "reconnect-storm",
            };
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            match runtime.block_on(obs_events_gen::publish(config, publish_config)) {
                Ok(report) => {
                    println!(
                        "published: sent={} acked_seq={} rejections={} reconnects={}",
                        report.sent, report.acked_seq, report.rejections, report.reconnects
                    );
                }
                Err(error) => {
                    eprintln!("publish failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        "verify" => {
            let db = args.required("db").to_owned();
            let config = sim_config(&args);
            match obs_events_gen::verify(std::path::Path::new(&db), config) {
                Ok(report) if report.ok => {
                    println!(
                        "verify OK: {} events, {} unknown-flagged, checksum {:#x}",
                        report.stored, report.stored_unknown, report.checksum_stored
                    );
                }
                Ok(report) => {
                    eprintln!("verify FAILED:");
                    for mismatch in &report.mismatches {
                        eprintln!("  {mismatch}");
                    }
                    std::process::exit(1);
                }
                Err(error) => {
                    eprintln!("verify error: {error}");
                    std::process::exit(1);
                }
            }
        }
        _ => usage(),
    }
}
