use std::process::Command;

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn git_dirty() -> &'static str {
    let worktree = Command::new("git")
        .args(["diff", "--quiet"])
        .status()
        .ok()
        .is_some_and(|status| status.success());
    let index = Command::new("git")
        .args(["diff", "--cached", "--quiet"])
        .status()
        .ok()
        .is_some_and(|status| status.success());
    if worktree && index {
        "clean"
    } else {
        "dirty"
    }
}

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
    println!(
        "cargo:rustc-env=ANCHORBELL_GIT_SHA={}",
        command_output("git", &["rev-parse", "HEAD"])
    );
    println!("cargo:rustc-env=ANCHORBELL_GIT_DIRTY={}", git_dirty());
    println!(
        "cargo:rustc-env=ANCHORBELL_RUSTC={}",
        command_output("rustc", &["-Vv"]).replace('\n', " ")
    );
}
