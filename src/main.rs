use raft::{DiskPersist, Persist};

#[tokio::main]
async fn main() {
    eprintln!("{}", 5 / 2);
    let persist = DiskPersist::<String>::new("saved/2");
    let data = persist.load().await.unwrap().unwrap();
    println!("ITERS {}", data.log.len());
    println!(
        "{}",
        data.log
            .into_iter()
            .map(|mut x| {
                x.command.push('\n');
                x.command
            })
            .collect::<String>()
    );
}
