use std::path::Path;

use anyhow::Result;

use crate::core::repository::Repository;
use crate::mirror::auth::AuthStore;
use crate::sync::{
    load_remote_registry, parse_branch_mapping, parse_remote_kind, plan_sync, remote_definition,
    remove_remote, upsert_remote,
};

pub fn run_list() -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let auth = AuthStore::open(&repo.forge_dir)?;
    let registry = load_remote_registry(&repo, &auth)?;

    if registry.remotes.is_empty() {
        println!("No remotes configured.");
        return Ok(());
    }

    println!("Configured remotes:");
    for remote in &registry.remotes {
        let primary = if registry.primary.as_deref() == Some(remote.name.as_str()) {
            " [primary]"
        } else {
            ""
        };
        let auth_backend = remote.auth_backend.as_deref().unwrap_or("none");
        println!(
            "  {}{} -> {} ({:?}, auth: {}, mappings: {})",
            remote.name,
            primary,
            remote.locator,
            remote.kind,
            auth_backend,
            remote.branch_mappings.len()
        );
    }

    Ok(())
}

pub fn run_add(
    name: &str,
    kind: &str,
    locator: &str,
    auth_backend: Option<&str>,
    branch_maps: &[String],
    primary: bool,
) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let auth = AuthStore::open(&repo.forge_dir)?;
    let kind = parse_remote_kind(kind)?;
    let mappings = branch_maps
        .iter()
        .map(|mapping| parse_branch_mapping(mapping))
        .collect::<Result<Vec<_>>>()?;
    let remote = remote_definition(
        name,
        kind,
        locator,
        auth_backend.map(str::to_string),
        mappings,
        primary,
    );
    let registry = upsert_remote(&repo, &auth, remote, primary)?;
    println!(
        "Configured remote '{}' ({} total remote(s)).",
        name,
        registry.remotes.len()
    );
    Ok(())
}

pub fn run_remove(name: &str) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let auth = AuthStore::open(&repo.forge_dir)?;
    let registry = remove_remote(&repo, &auth, name)?;
    println!(
        "Removed remote '{}' ({} remaining remote(s)).",
        name,
        registry.remotes.len()
    );
    Ok(())
}

pub fn run_plan(remote: Option<&str>) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let auth = AuthStore::open(&repo.forge_dir)?;
    let plan = plan_sync(&repo, &auth, remote)?;
    crate::cli::sync::print_plan(&repo, &plan);
    Ok(())
}
