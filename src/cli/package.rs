use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::core::repository::Repository;
use crate::packages::{
    add_local_slice_package, build_package_lock, build_package_status, write_package_lock,
    write_package_status_receipt, DependencyConstraint, PackageAddReceipt, PackageStatusReceipt,
};

pub fn run_add(
    name: &str,
    version: &str,
    source: &str,
    files: &[String],
    dependencies: &[String],
    dependency_boundary: &str,
    json: bool,
) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let dependencies = dependencies
        .iter()
        .map(|dependency| parse_dependency(dependency, dependency_boundary))
        .collect::<Result<Vec<_>>>()?;
    let receipt = add_local_slice_package(&repo, name, version, source, files, dependencies)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&receipt)?);
    } else {
        print_add_receipt(&receipt);
    }
    Ok(())
}

pub fn run_list(json: bool) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let status = build_package_status(&repo)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&status.packages)?);
        return Ok(());
    }

    if status.packages.is_empty() {
        println!("No Forge packages declared.");
        return Ok(());
    }

    println!("Forge packages:");
    for package in &status.packages {
        println!(
            "  {}@{} ({:?}, files={}, constraints={})",
            package.name,
            package.version,
            package.integrity_state,
            package.files.len(),
            package.dependency_constraints.len()
        );
    }
    Ok(())
}

pub fn run_status(json: bool, write_receipt: bool) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let status = build_package_status(&repo)?;
    if write_receipt {
        write_package_status_receipt(&repo, &status)?;
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print_status(&status);
        if write_receipt {
            println!("Receipt: {}", repo.package_status_receipt_path().display());
        }
    }
    Ok(())
}

pub fn run_lock(json: bool, write: bool) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let status = build_package_status(&repo)?;
    let lock = if write {
        write_package_lock(&repo, &status)?
    } else {
        build_package_lock(&status)
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&lock)?);
    } else {
        println!(
            "Package lock: {} packages, {} remotes, {} media assets",
            lock.packages.len(),
            lock.remotes.len(),
            lock.media.len()
        );
        if write {
            println!("Wrote {}", repo.package_lock_path().display());
        } else {
            println!("Dry run only. Add --write to update the lockfile.");
        }
    }
    Ok(())
}

fn print_status(status: &PackageStatusReceipt) {
    println!("Forge package status:");
    println!("  manifest: {}", status.manifest_path.display());
    println!("  lock: {}", status.lock_path.display());
    println!("  lock present: {}", status.package_lock_present);
    println!(
        "  packages: {} valid={} missing={} mismatched={}",
        status.summary.package_count,
        status.summary.valid_packages,
        status.summary.missing_packages,
        status.summary.mismatched_packages
    );
    println!(
        "  remotes: {} unsafe={}",
        status.summary.remote_count, status.summary.unsafe_remote_count
    );
    println!(
        "  media: {} tracked={} chunks={}",
        status.summary.media_asset_count,
        status.summary.tracked_media_assets,
        status.summary.media_chunk_count
    );

    if !status.packages.is_empty() {
        println!();
        println!("Packages:");
        for package in &status.packages {
            println!(
                "  - {}@{} {:?} files={} constraints={}",
                package.name,
                package.version,
                package.integrity_state,
                package.files.len(),
                package.dependency_constraints.len()
            );
        }
    }

    if !status.media.is_empty() {
        println!();
        println!("Media:");
        for asset in &status.media {
            println!(
                "  - {} {} chunks={} hash={}",
                asset.asset_id,
                asset.media_type,
                asset.chunk_count,
                asset.content_hash.as_deref().unwrap_or("missing")
            );
        }
    }
}

fn parse_dependency(value: &str, dependency_boundary: &str) -> Result<DependencyConstraint> {
    let (name, constraint) = value
        .split_once('=')
        .or_else(|| value.rsplit_once('@'))
        .with_context(|| {
            format!("dependency `{value}` must use NAME=CONSTRAINT or NAME@CONSTRAINT syntax")
        })?;
    let name = name.trim();
    let constraint = constraint.trim();
    if name.is_empty() || constraint.is_empty() {
        bail!("dependency `{value}` must include both name and constraint");
    }
    Ok(DependencyConstraint {
        name: name.to_string(),
        constraint: constraint.to_string(),
        boundary: Some(dependency_boundary.to_string()),
    })
}

fn print_add_receipt(receipt: &PackageAddReceipt) {
    println!(
        "Added Forge package {}@{}",
        receipt.package.name, receipt.package.version
    );
    println!("  manifest: {}", receipt.manifest_path);
    println!("  lock: {}", receipt.lock_path);
    println!("  status receipt: {}", receipt.status_receipt_path);
    println!("  package receipt: {}", receipt.package_receipt_path);
    println!(
        "  cache: {} files={}",
        receipt.cache.cache_path,
        receipt.cache.cached_files.len()
    );
}
