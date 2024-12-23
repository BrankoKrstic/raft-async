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
#[derive(Debug, Clone, Copy)]
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

pub struct Raft<T: Transport> {
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
    async fn run(&mut self) {
        loop {
            match self.state {
                NodeState::Leader => todo!(),
                NodeState::Candidate => {
                    self.term.0 += 1;
                    self.voted_for = Some(self.id);
                    let needs_votes = self.peers.len() + 1 / 2;
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
                          _ = &mut timeout_fut => break,
                          msg = self.transport.recv() => {
                            match msg.payload {
                              Payload::GetState => todo!(),
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

                                    self.voted_for = Some(msg.from);
                                    break;
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
                                Payload::RequestVoteReply { term, vote_granted } => {
                                  if vote_granted {
                                    votes += 1;
                                    if votes >= needs_votes {
                                      self.state = NodeState::Leader;
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
                                    continue;
                                  }
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
