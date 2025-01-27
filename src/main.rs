use std::{fs::File, io::Write};

use raft::{DiskPersist, Persist};

#[tokio::main]
async fn main() {
    eprintln!("{}", 5 / 2);

    for i in 0..3 {
        let persist = DiskPersist::<String>::new(format!("saved/{}", i));
        let data = persist.load().await.unwrap().unwrap();
        println!("ITERS {}", data.log.len());
        let mut output = File::create(format!("{i}.txt")).unwrap();
        let data = data
            .log
            .into_iter()
            .map(|mut x| {
                x.command.push('\n');
                x.command
            })
            .collect::<String>();
        output.write_all(data.as_bytes()).unwrap();
        output.flush().unwrap();
    }
}
