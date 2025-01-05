use futures::stream::{FuturesUnordered, StreamExt};
use rand::{thread_rng, Rng};
use std::{future::Future, pin::Pin};
use tokio::{
    sync::{
        mpsc::{self, Receiver},
        oneshot,
    },
    time::{sleep, Duration},
};

const ELECTION_TIMEOUT_MIN: u64 = 150;
const ELECTION_TIMEOUT_MAX: u64 = 300;

pub trait Transport<LogCommand> {
    async fn append_entries(
        &self,
        peer: &Peer,
        msg: AppendEntriesRequest<LogCommand>,
    ) -> AppendEntriesResponse;
    async fn request_votes(&self, peer: &Peer, msg: RequestVoteRequest) -> RequestVoteResponse;
}

pub struct Topology {
    node_id: PeerId,
    peers: Vec<Peer>,
}

pub struct AppendEntriesRequest<LogCommand> {
    term: Term,
    leader_id: PeerId,
    prev_log_index: Option<usize>,
    prev_log_term: Option<Term>,
    entries: Vec<LogEntry<LogCommand>>,
    leader_commit: usize,
}

pub struct AppendEntriesResponse {
    term: Term,
    success: bool,
}
pub struct RequestVoteRequest {
    candidate_id: PeerId,
    term: Term,
    last_log_index: usize,
    last_log_term: usize,
}
pub struct RequestVoteResponse {
    term: Term,
    vote_granted: bool,
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Copy)]
struct Term(u64);
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
struct PeerId(u64);

#[derive(Debug, Clone)]
struct PeerUrl(String);
#[derive(Debug, Clone)]
struct Peer {
    id: PeerId,
    log_index: usize,
}

impl Peer {
    pub fn new(id: PeerId) -> Self {
        Self { id, log_index: 0 }
    }
}

#[derive(Debug, Clone, Copy)]
enum NodeState {
    Leader,
    Candidate,
    Follower,
}
#[derive(Debug, Clone)]
pub struct LogEntry<LogCommand> {
    term: Term,
    command: LogCommand,
}

impl<LogCommand> LogEntry<LogCommand> {
    fn new(term: Term, command: LogCommand) -> Self {
        Self { term, command }
    }
}

enum RaftCommand<LogCommand> {
    AppendEntries {
        request: AppendEntriesRequest<LogCommand>,
        tx: oneshot::Sender<AppendEntriesResponse>,
    },
    RequestVote {
        request: RequestVoteRequest,
        tx: oneshot::Sender<RequestVoteResponse>,
    },
    SubmitEntry {
        request: LogCommand,
        tx: oneshot::Sender<Result<(), AppendError>>,
    },
}

pub struct Raft<T, LogCommand> {
    id: PeerId,
    term: Term,
    commit_index: usize,
    last_applied: usize,
    state: NodeState,
    peers: Vec<Peer>,
    transport: T,
    voted_for: Option<PeerId>,
    log: Vec<LogEntry<LogCommand>>,
    event_receiver: Receiver<RaftCommand<LogCommand>>,
}

pub enum AppendError {
    NotLeader,
}

impl<T: Transport<LogCommand>, LogCommand: Clone> Raft<T, LogCommand> {
    fn new(transport: T, rx: mpsc::Receiver<RaftCommand<LogCommand>>) -> Self {
        Self {
            id: PeerId(0),
            term: Term(0),
            commit_index: 0,
            last_applied: 0,
            state: NodeState::Follower,
            peers: vec![],
            transport,
            voted_for: None,
            log: vec![],
            event_receiver: rx,
        }
    }
    fn is_leader(&self) -> bool {
        matches!(self.state, NodeState::Leader)
    }
    fn request_vote() {}
    fn submit_entry(&mut self, command: LogCommand) -> Result<(), AppendError> {
        match self.state {
            NodeState::Leader => {
                self.log.push(LogEntry::new(self.term, command));
                Ok(())
            }
            _ => Err(AppendError::NotLeader),
        }
    }
    fn append_entries() {}
    fn install_config(&mut self, topology: Topology) {
        self.id = topology.node_id;
        self.peers = topology
            .peers
            .into_iter()
            .filter(|node| node.id != self.id)
            .collect();
    }
    fn handle_command(&mut self, command: RaftCommand<LogCommand>) {
        match command {
            RaftCommand::AppendEntries {
                request:
                    AppendEntriesRequest {
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    },
                tx,
            } => {
                if term <= self.term {
                    // TODO: add log index checking here
                    tx.send(AppendEntriesResponse {
                        term: self.term,
                        success: false,
                    });
                } else {
                    self.state = NodeState::Follower;
                    tx.send(AppendEntriesResponse {
                        term: self.term,
                        success: true,
                    });
                }
            }
            RaftCommand::RequestVote { request, tx } => {
                if request.term > self.term {
                    // TODO: add log index checking here
                    self.term = request.term;
                    self.voted_for = None;
                    tx.send(RequestVoteResponse {
                        term: self.term,
                        vote_granted: true,
                    });
                    self.state = NodeState::Follower;
                } else {
                    tx.send(RequestVoteResponse {
                        term: self.term,
                        vote_granted: false,
                    });
                }
            }
            RaftCommand::SubmitEntry { request, tx } => {
                let result = self.submit_entry(request);
                tx.send(result);
            }
        }
    }
    async fn send_append_entries_helper(&self) -> Vec<(PeerId, usize, AppendEntriesResponse)> {
        let mut jhs = FuturesUnordered::new();

        for peer in &self.peers {
            let peer_id = peer.id;
            let prev_log_index = if peer.log_index == 0 {
                None
            } else {
                Some(peer.log_index - 1)
            };
            let prev_log_term = prev_log_index.map(|i| self.log[i].term);
            let entries = self
                .log
                .iter()
                .skip(peer.log_index)
                .cloned()
                .collect::<Vec<_>>();
            let entries_len = entries.len();

            let append_entries_request = AppendEntriesRequest {
                term: self.term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };

            jhs.push(async move {
                let result = (self)
                    .transport
                    .append_entries(peer, append_entries_request)
                    .await;
                (peer_id, entries_len, result)
            });
        }
        let mut out = vec![];

        while let Some(result) = jhs.next().await {
            out.push(result);
        }

        out
    }
    fn handle_append_entries(
        &mut self,
        req: AppendEntriesRequest<LogCommand>,
    ) -> AppendEntriesResponse {
        if self.term > req.term {
            return AppendEntriesResponse {
                term: self.term,
                success: false,
            };
        }
        eprintln!("Raft Instance {:?} appending logs", self.id);
        if let Some(prev_log_index) = req.prev_log_index {
            let prev_log_term = req.prev_log_term.unwrap();
            let prev_log = self.log.get(prev_log_index);
            if prev_log.is_none() || prev_log.unwrap().term != prev_log_term {
                return AppendEntriesResponse {
                    term: self.term,
                    success: false,
                };
            }

            // TODO: Fix this awfulness
            for (i, log_entry) in req
                .entries
                .into_iter()
                .enumerate()
                .map(|(i, entry)| (i + prev_log_index + 1, entry))
            {
                if i < self.log.len() {
                    if self.log[i].term != log_entry.term {
                        self.log = self.log[0..i].to_vec();
                    }
                    self.log.push(log_entry);
                } else {
                    self.log.push(log_entry);
                }
            }
            if req.leader_commit > self.commit_index {
                self.commit_index = req.leader_commit.min(self.log.len());
            }
        } else {
            self.log = req.entries;
            self.commit_index = req.leader_commit.min(self.log.len());
        }
        // TODO: Send newly committed items up the commit channel
        AppendEntriesResponse {
            term: self.term,
            success: true,
        }
    }
    async fn send_append_entries_requests(&mut self) {
        let results = self.send_append_entries_helper().await;

        for (peer_id, entries_len, result) in results {
            if result.success {
                let peer = self.peers.iter_mut().find(|p| p.id == peer_id);
                if let Some(peer) = peer {
                    peer.log_index += entries_len;
                }
            } else if result.term > self.term {
                self.term = result.term;
                self.state = NodeState::Follower;
                return;
            } else {
                let peer = self.peers.iter_mut().find(|p| p.id == peer_id);
                if let Some(peer) = peer {
                    peer.log_index -= 1;
                }
            }
        }
        // TODO: Increment commit index if needed and send committed logs up the commit channel
    }
    pub async fn run(mut self) {
        loop {
            match self.state {
                NodeState::Leader => {
                    eprintln!("Raft Instance {:?} sending heartbeat", self.id);
                    self.send_append_entries_requests().await;
                    let timeout = Duration::from_millis(10);
                    tokio::select! {
                            _ = sleep(timeout) => {},
                            msg = self.event_receiver.recv() => {
                              self.handle_command(msg.unwrap());
                          }
                    }
                }
                NodeState::Candidate => {
                    eprintln!("Raft Instance {:?} initiating vote", self.id);

                    self.term.0 += 1;
                    self.voted_for = Some(self.id);
                    let needs_votes = (self.peers.len() + 1) / 2;

                    let mut jhs = self
                        .peers
                        .iter()
                        .map(|peer| {
                            self.transport.request_votes(
                                peer,
                                RequestVoteRequest {
                                    candidate_id: self.id,
                                    term: self.term,
                                    last_log_index: 0,
                                    last_log_term: 0,
                                },
                            )
                        })
                        .collect::<FuturesUnordered<_>>();

                    let mut total_votes = 0;

                    let timeout_duration = thread_rng().gen_range(700..1200);
                    let timeout = Duration::from_millis(timeout_duration);
                    let timeout_fut = sleep(timeout);
                    tokio::pin!(timeout_fut);

                    loop {
                        tokio::select! {
                              _ = &mut timeout_fut => {
                                eprintln!("Raft Instance {:?} vote timed out", self.id);
                                break;
                              }
                              res = jhs.next() => {
                                if let Some(res) = res {
                                  if res.vote_granted {
                                    total_votes += 1;
                                    if total_votes >= needs_votes {
                                        self.state = NodeState::Leader;
                                        eprintln!("Raft Instance {:?} won vote", self.id);

                                        break;
                                    }
                                  } else if res.term > self.term {
                                      self.state = NodeState::Follower;
                                      self.voted_for = None;
                                      self.term = res.term;
                                      break;
                                  }
                                } else {
                                  self.state = NodeState::Follower;
                                  break;
                                }
                              }
                              command = self.event_receiver.recv() => {
                                match command.unwrap() {
                                RaftCommand::SubmitEntry { request, tx } => {
                                    tx.send(Err(AppendError::NotLeader));
                                },
                                RaftCommand::AppendEntries { request, tx } => {
                                if request.term >= self.term {
                                    self.term = request.term;
                                    self.state = NodeState::Follower;
                                    tx.send(AppendEntriesResponse { term: self.term, success: true});
                                    break;
                                } else {
                                    tx.send(AppendEntriesResponse { term: self.term, success: false});
                                }
                                },
                                RaftCommand::RequestVote { request, tx } => {
                                if request.term > self.term {
                                    self.state = NodeState::Follower;
                                    self.term = request.term;
                                    tx.send(RequestVoteResponse {
                                    term: self.term,
                                    vote_granted: true
                                    });
                                    eprintln!("Raft Instance {:?} voting for {:?}", self.id, request.candidate_id);
                                    self.voted_for = Some(request.candidate_id);
                                    break;
                                } else {
                                    eprintln!("Raft Instance {:?} refusing vote for {:?}", self.id, request.candidate_id);
                                    tx.send(RequestVoteResponse {
                                    term: self.term,
                                    vote_granted: false
                                    });
                                }
                                }
                            }
                          }
                        }
                    }
                }
                NodeState::Follower => {
                    let timeout_duration =
                        thread_rng().gen_range(ELECTION_TIMEOUT_MIN..ELECTION_TIMEOUT_MAX);
                    let timeout = Duration::from_millis(timeout_duration);
                    tokio::select! {
                        _ = sleep(timeout) => {
                            self.state = NodeState::Candidate
                        }
                        command = self.event_receiver.recv() => {
                          match command.unwrap() {
                            RaftCommand::SubmitEntry { request, tx } => {
                                tx.send(Err(AppendError::NotLeader));
                            },
                            RaftCommand::AppendEntries { request, tx } => {
                                let response = self.handle_append_entries(request);
                                tx.send(response);

                            },
                            RaftCommand::RequestVote { request, tx } => {
                              if request.term > self.term {
                                self.term = request.term;
                                tx.send(RequestVoteResponse {
                                  term: self.term,
                                  vote_granted: true
                                });
                                eprintln!("Raft Instance {:?} voting for {:?}", self.id, request.candidate_id);
                                self.voted_for = Some(request.candidate_id);
                              } else {
                                eprintln!("Raft Instance {:?} refusing vote for {:?}", self.id, request.candidate_id);
                                tx.send(RequestVoteResponse {
                                  term: self.term,
                                  vote_granted: false
                                });
                              }
                            }
                        }
                      }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::{collections::HashMap, time::Duration};

    use tokio::sync::mpsc::{channel, Receiver, Sender};
    use tokio::sync::oneshot;

    use crate::{
        AppendEntriesRequest, AppendEntriesResponse, Peer, PeerId, PeerUrl, Raft, RaftCommand,
        RequestVoteRequest, RequestVoteResponse, Transport,
    };
    struct Message<M, R> {
        peer_id: PeerId,
        message: M,
        tx: tokio::sync::oneshot::Sender<R>,
    }
    struct LocalTransport {
        ae_channel: Sender<Message<AppendEntriesRequest<String>, AppendEntriesResponse>>,
        rv_channel: Sender<Message<RequestVoteRequest, RequestVoteResponse>>,
    }

    impl Transport<String> for LocalTransport {
        async fn append_entries(
            &self,
            peer: &crate::Peer,
            msg: AppendEntriesRequest<String>,
        ) -> AppendEntriesResponse {
            let (tx, rx) = oneshot::channel();
            self.ae_channel
                .send(Message {
                    peer_id: peer.id,
                    message: msg,
                    tx,
                })
                .await;
            rx.await.unwrap()
        }

        async fn request_votes(
            &self,
            peer: &crate::Peer,
            msg: RequestVoteRequest,
        ) -> RequestVoteResponse {
            let (tx, rx) = oneshot::channel();
            self.rv_channel
                .send(Message {
                    peer_id: peer.id,
                    message: msg,
                    tx,
                })
                .await;
            rx.await.unwrap()
        }
    }

    struct RaftNode {
        id: PeerId,
        event_emitter: Sender<RaftCommand<String>>,
    }

    #[tokio::test]
    async fn it_works() {
        let mut nodes = HashMap::new();
        let (tx2, mut rx2) = channel(100);
        let (tx3, mut rx3) = channel(100);
        for i in 0..3 {
            let (tx1, mut rx1) = channel(100);

            let transport = LocalTransport {
                ae_channel: tx2.clone(),
                rv_channel: tx3.clone(),
            };
            let raft_node = RaftNode {
                id: PeerId(i),
                event_emitter: tx1,
            };
            nodes.insert(i, raft_node);
            let mut raft = Raft::<_, String>::new(transport, rx1);
            raft.install_config(crate::Topology {
                node_id: PeerId(i),
                peers: (0..3).map(|i| Peer::new(PeerId(i))).collect(),
            });
            tokio::spawn(raft.run());
        }
        let fut = tokio::time::sleep(Duration::from_millis(5000));
        tokio::pin!(fut);

        loop {
            tokio::select! {
              _ = &mut fut => break,
              msg = rx2.recv() => {
                let msg = msg.unwrap();
                let found_node = nodes.get(&msg.peer_id.0).unwrap();
                found_node
                    .event_emitter
                    .send(RaftCommand::AppendEntries {
                        request: msg.message,
                        tx: msg.tx,
                    })
                    .await
                    .unwrap();
              }
              msg = rx3.recv() => {
                let msg = msg.unwrap();
                let found_node = nodes.get(&msg.peer_id.0).unwrap();
                found_node
                    .event_emitter
                    .send(RaftCommand::RequestVote {
                        request: msg.message,
                        tx: msg.tx,
                    })
                    .await
                    .unwrap();
              }
            }
        }
    }
}
