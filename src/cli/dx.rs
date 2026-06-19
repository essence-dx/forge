use std::path::Path;

use anyhow::Result;

use crate::core::repository::Repository;
use crate::dx_status::{inspect_dx_status, DxArtifactState, DxStatusReport};

pub fn run_status(json: bool) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let report = inspect_dx_status(&repo.root)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_status(&report);
    }

    Ok(())
}

fn print_status(report: &DxStatusReport) {
    println!("Forge DX status:");
    println!("  project: {}", report.project_root.display());
    println!("  .dx present: {}", report.dx_root_present);
    println!(
        "  package manifest configured: {}",
        report.package_manifest_configured
    );
    println!(
        "  serializer machines: {} fresh={} stale={} invalid={}",
        report.serializer_machines.len(),
        count_state(&report.serializer_machines, DxArtifactState::Fresh),
        count_state(&report.serializer_machines, DxArtifactState::Stale),
        count_state(&report.serializer_machines, DxArtifactState::Invalid)
    );
    println!(
        "  typed caches: {} fresh={} stale={} invalid={}",
        report.typed_caches.len(),
        count_state(&report.typed_caches, DxArtifactState::Fresh),
        count_state(&report.typed_caches, DxArtifactState::Stale),
        count_state(&report.typed_caches, DxArtifactState::Invalid)
    );
    if !report.unknown_machines.is_empty() {
        println!("  unknown machine files: {}", report.unknown_machines.len());
    }
    if !report.warnings.is_empty() {
        println!();
        println!("Warnings:");
        for warning in &report.warnings {
            println!("  - {warning}");
        }
    }
}

fn count_state(machines: &[crate::dx_status::DxMachineStatus], state: DxArtifactState) -> usize {
    machines
        .iter()
        .filter(|machine| machine.state == state)
        .count()
}
