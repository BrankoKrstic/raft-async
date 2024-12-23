use std::pin::Pin;

use rand::{thread_rng, Rng};
use tokio::time::{sleep, Duration};

const ELECTION_TIMEOUT_MIN: u64 = 150;
const ELECTION_TIMEOUT_MAX: u64 = 300;

pub trait Transport {
    async fn send(&mut self, msg: Message);
    async fn recv(&mut self) -> Message;
}

pub struct Message {
    from: PeerId,
    to: PeerId,
    payload: Payload,
}
pub enum Payload {
    GetState,
    Topology {
        node_id: PeerId,
        peers: Vec<Peer>,
    },
    AppendEntries {
        term: Term,
        leader_id: PeerId,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<String>,
        leader_commit: usize,
    },
    AppendEntriesResult {
        term: Term,
        success: bool,
    },
    State {
        term: Term,
        state: NodeState,
    },
    RequestVote {
        term: Term,
        last_log_index: usize,
        last_log_term: usize,
    },
    RequestVoteReply {
        term: Term,
        vote_granted: bool,
    },
}

pub enum RecvMessage {}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Copy)]
struct Term(u64);
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
struct PeerId(u64);

#[derive(Debug, Clone)]
struct PeerUrl(String);
#[derive(Debug, Clone)]
struct Peer {
    id: PeerId,
    url: PeerUrl,
}

#[derive(Debug, Clone, Copy)]
enum NodeState {
    Leader,
    Candidate,
    Follower,
}

pub struct Raft<T> {
    id: PeerId,
    term: Term,
    state: NodeState,
    peers: Vec<Peer>,
    transport: T,
    voted_for: Option<PeerId>,
    log: Vec<String>,
}

impl<T: Transport> Raft<T> {
    fn new(transport: T) -> Self {
        Self {
            id: PeerId(0),
            term: Term(0),
            state: NodeState::Follower,
            peers: vec![],
            transport,
            voted_for: None,
            log: vec![],
        }
    }
    fn get_state() {}
    fn request_vote() {}
    fn append_entries() {}
    async fn recv_config(&mut self) {
        loop {
            let msg = self.transport.recv().await;
            if let Message {
                payload: Payload::Topology { node_id, peers },
                ..
            } = msg
            {
                self.id = node_id;
                self.peers = peers;
                break;
            } else {
                continue;
            }
        }
    }
    async fn run(mut self) {
        self.recv_config().await;
        loop {
            match self.state {
                NodeState::Leader => {
                    let timeout = Duration::from_millis(10);
                    tokio::select! {
                        _ = sleep(timeout) => {
                          for node in &self.peers {
                            self.transport.send(Message {
                              from: self.id,
                              to: node.id,
                              payload: Payload::AppendEntries { term: self.term, leader_id: self.id, prev_log_index: 0, prev_log_term: 0, entries: vec![], leader_commit: 0 }
                            }).await;
                          }
                        }
                        msg = self.transport.recv() => {
                          match msg.payload {
                              Payload::Topology { .. } => {},
                              Payload::GetState => {
                                self.transport.send(Message {
                                  from: self.id,
                                  to: self.id,
                                  payload: Payload::State {
                                    term: self.term,
                                    state: self.state
                                }}).await;
                              }
                              Payload::State { term, state } => {},
                              Payload::RequestVote { term, last_log_index, last_log_term } => {
                                if term > self.term { // TODO: add log index checking here
                                    self.term = term;
                                    self.voted_for = None;
                                    self.transport.send(Message {
                                      from: self.id,
                                      to: msg.from,
                                      payload: Payload::RequestVoteReply {
                                      term: self.term,
                                      vote_granted: true
                                    }}).await;
                                    self.state = NodeState::Follower;
                                    continue;
                                } else {
                                  self.transport.send(Message {
                                    from: self.id,
                                    to: msg.from,
                                    payload: Payload::RequestVoteReply {
                                    term: self.term,
                                    vote_granted: false
                                  }}).await;
                                }
                              },
                              Payload::RequestVoteReply { term, vote_granted } => {},
                              Payload::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                                if term <= self.term { // TODO: add log index checking here
                                    self.transport.send(Message {
                                      from: self.id,
                                      to: msg.from,
                                      payload: Payload::AppendEntriesResult { term: self.term, success: false }}).await;
                                } else {
                                  self.state = NodeState::Follower;
                                  self.transport.send(Message {
                                    from: self.id,
                                    to: msg.from,
                                    payload: Payload::AppendEntriesResult { term: self.term, success: true }}).await;
                                }
                              },
                              Payload::AppendEntriesResult {..} => {},
                          }
                        }
                    }
                }
                NodeState::Candidate => {
                    eprintln!("Raft Instance {:?} initiating vote", self.id);

                    self.term.0 += 1;
                    self.voted_for = Some(self.id);
                    let needs_votes = (self.peers.len() + 1) / 2;
                    for peer in &self.peers {
                        self.transport
                            .send(Message {
                                from: self.id,
                                to: peer.id,
                                payload: Payload::RequestVote {
                                    term: self.term,
                                    last_log_index: 0,
                                    last_log_term: 0,
                                },
                            })
                            .await;
                    }
                    let timeout_duration = thread_rng().gen_range(700..1200);
                    let timeout = Duration::from_millis(timeout_duration);
                    let timeout_fut = sleep(timeout);
                    tokio::pin!(timeout_fut);
                    let mut votes = 0;
                    loop {
                        tokio::select! {
                          _ = &mut timeout_fut => {
                            eprintln!("Raft Instance {:?} vote timed out", self.id);
                            break;
                          },
                          msg = self.transport.recv() => {
                            match msg.payload {
                              Payload::Topology { .. } => {},
                              Payload::GetState => {
                                self.transport.send(Message {
                                  from: self.id,
                                  to: self.id,
                                  payload: Payload::State {
                                    term: self.term,
                                    state: self.state
                                }}).await;
                              },
                              Payload::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                                  if term >= self.term {
                                    self.term = term;
                                    self.state = NodeState::Follower;
                                    self.transport.send(Message {
                                      from: self.id,
                                      to: msg.from,
                                      payload: Payload::AppendEntriesResult { term: self.term, success: true }}).await;
                                    break;
                                  } else {
                                    self.transport.send(Message {
                                      from: self.id,
                                      to: msg.from,
                                      payload: Payload::AppendEntriesResult { term: self.term, success: false }}).await;
                                  }
                                },
                                Payload::AppendEntriesResult { term, success } => {}
                                Payload::State { term, state } => {},
                                Payload::RequestVote { term, last_log_index, last_log_term } => {
                                  if term > self.term {
                                    self.state = NodeState::Follower;
                                    self.term = term;
                                    self.transport.send(Message {
                                      from: self.id,
                                      to: msg.from,
                                      payload: Payload::RequestVoteReply {
                                      term: self.term,
                                      vote_granted: true
                                    }}).await;
                                    eprintln!("Raft Instance {:?} voting for {:?}", self.id, msg.from);

                                    self.voted_for = Some(msg.from);
                                    break;
                                  } else {
                                    eprintln!("Raft Instance {:?} refusing vote for {:?}", self.id, msg.from);
                                    self.transport.send(Message {
                                      from: self.id,
                                      to: msg.from,
                                      payload: Payload::RequestVoteReply {
                                      term: self.term,
                                      vote_granted: false
                                         }}).await;
                                  }
                                },
                                Payload::RequestVoteReply { term, vote_granted } => {
                                  if vote_granted {
                                    votes += 1;
                                    if votes >= needs_votes {
                                      self.state = NodeState::Leader;
                                      eprintln!("Raft Instance {:?} won vote", self.id);

                                      break;
                                    }
                                  } else if term > self.term {
                                    self.state = NodeState::Follower;
                                    self.voted_for = None;
                                    self.term = term;
                                    break;
                                  }
                                },
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
                        msg = self.transport.recv() => {
                          match msg.payload {
                              Payload::Topology { .. } => {},
                              Payload::GetState => {
                                self.transport.send(Message {
                                  from: self.id,
                                  to: self.id,
                                  payload: Payload::State {
                                    term: self.term,
                                    state: self.state
                                }}).await;
                              }
                              Payload::State { term, state } => {},
                              Payload::RequestVote { term, last_log_index, last_log_term } => {
                                if term >= self.term { // TODO: add log index checking here
                                  if term > self.term {
                                    self.term = term;
                                    self.voted_for = None;
                                  }
                                  if self.voted_for.is_none() {
                                    self.transport.send(Message {
                                      from: self.id,
                                      to: msg.from,
                                      payload: Payload::RequestVoteReply {
                                      term: self.term,
                                      vote_granted: true
                                    }}).await;
                                    eprintln!("Raft Instance {:?} voting for {:?}", self.id, msg.from);
                                    continue;
                                  }
                                } else {
                                  eprintln!("Raft Instance {:?} refusing vote for {:?}", self.id, msg.from);

                                  self.transport.send(Message {
                                    from: self.id,
                                    to: msg.from,
                                    payload: Payload::RequestVoteReply {
                                    term: self.term,
                                    vote_granted: false
                                  }}).await;
                                }
                              },
                              Payload::RequestVoteReply { term, vote_granted } => {},
                              Payload::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                                if term < self.term { // TODO: add log index checking here
                                    self.transport.send(Message {
                                      from: self.id,
                                      to: msg.from,
                                      payload: Payload::AppendEntriesResult { term: self.term, success: false }}).await;
                                } else {
                                  self.transport.send(Message {
                                    from: self.id,
                                    to: msg.from,
                                    payload: Payload::AppendEntriesResult { term: self.term, success: true }}).await;
                                }
                              },
                              Payload::AppendEntriesResult {..} => {},
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

    use crate::{Message, NodeState, Payload, Peer, PeerId, PeerUrl, Raft, Transport};
    struct LocalTransport {
        tx: Sender<Message>,
        rx: Receiver<Message>,
    }

    impl Transport for LocalTransport {
        async fn send(&mut self, msg: Message) {
            self.tx.send(msg).await.unwrap();
        }

        async fn recv(&mut self) -> Message {
            self.rx.recv().await.unwrap()
        }
    }
    struct RaftNode {
        id: PeerId,
        sender: Sender<Message>,
    }

    #[tokio::test]
    async fn it_works() {
        let (tx1, mut rx1) = channel(100);
        let mut nodes = HashMap::new();
        for i in 0..3 {
            let (tx2, rx2) = channel(10);
            let transport = LocalTransport {
                tx: tx1.clone(),
                rx: rx2,
            };
            let raft_node = RaftNode {
                id: PeerId(i),
                sender: tx2,
            };
            nodes.insert(i, raft_node);
            tokio::spawn(Raft::new(transport).run());
        }
        for node in nodes.values() {
            node.sender
                .send(Message {
                    from: PeerId(0),
                    to: node.id,
                    payload: crate::Payload::Topology {
                        node_id: node.id,
                        peers: nodes
                            .values()
                            .filter(|v| v.id != node.id)
                            .map(|v| Peer {
                                id: v.id,
                                url: PeerUrl(String::new()),
                            })
                            .collect(),
                    },
                })
                .await
                .unwrap();
        }
        let fut = tokio::time::sleep(Duration::from_millis(5000));
        tokio::pin!(fut);
        loop {
            tokio::select! {
              _ = &mut fut => break,
              msg = rx1.recv() => {
                let msg = msg.unwrap();
                let found_node = nodes.get(&msg.to.0).unwrap();
                found_node.sender.send(msg).await.unwrap();
              }
            }
        }
        let mut num_leaders = 0;
        let mut num_acks = 0;
        for node in nodes.values() {
            node.sender
                .send(Message {
                    from: PeerId(0),
                    to: node.id,
                    payload: crate::Payload::GetState,
                })
                .await
                .unwrap();
        }
        while num_acks < 3 {
            if let Message {
                from,
                payload: Payload::State { term, state },
                to,
            } = rx1.recv().await.unwrap()
            {
                if let NodeState::Leader = state {
                    num_leaders += 1;
                }
                num_acks += 1;
            }
        }
        assert_eq!(num_leaders, 1);
    }
}
