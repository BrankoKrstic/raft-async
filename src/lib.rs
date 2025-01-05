use futures::stream::{FuturesUnordered, StreamExt};
use rand::{thread_rng, Rng};
use tokio::{
    sync::{
        mpsc::{self},
        oneshot,
    },
    time::{sleep, Duration},
};

const ELECTION_TIMEOUT_MIN: u64 = 150;
const ELECTION_TIMEOUT_MAX: u64 = 300;

pub trait Transport<LogCommand> {
    async fn append_entries(
        &self,
        peer_id: PeerId,
        msg: AppendEntriesRequest<LogCommand>,
    ) -> AppendEntriesResponse;
    async fn request_votes(&self, peer_id: PeerId, msg: RequestVoteRequest) -> RequestVoteResponse;
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
    leader_commit: Option<usize>,
}

pub struct AppendEntriesResponse {
    term: Term,
    success: bool,
}
pub struct RequestVoteRequest {
    candidate_id: PeerId,
    term: Term,
    last_log_index: Option<usize>,
    last_log_term: Option<Term>,
}
pub struct RequestVoteResponse {
    term: Term,
    vote_granted: bool,
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Copy)]
struct Term(u64);
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
struct PeerId(u64);

enum VoteResult<LogCommand> {
    Winner,
    BiggerTerm(Term),
    TimedOut,
    Loser,
    OtherLeaderAppendRequest(
        AppendEntriesRequest<LogCommand>,
        oneshot::Sender<AppendEntriesResponse>,
    ),
}

#[derive(Debug, Clone)]
struct PeerUrl(String);
#[derive(Debug, Clone)]
struct Peer {
    id: PeerId,
    log_index: Option<usize>,
}

impl Peer {
    pub fn new(id: PeerId) -> Self {
        Self {
            id,
            log_index: None,
        }
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
    commit_index: Option<usize>,
    last_applied: Option<usize>,
    state: NodeState,
    peers: Vec<Peer>,
    transport: T,
    voted_for: Option<PeerId>,
    log: Vec<LogEntry<LogCommand>>,
    event_receiver: mpsc::Receiver<RaftCommand<LogCommand>>,
    commit_channel: mpsc::Sender<LogCommand>,
}

pub enum AppendError {
    NotLeader,
}

impl<T: Transport<LogCommand>, LogCommand: Clone + Send> Raft<T, LogCommand> {
    fn new(
        transport: T,
        rx: mpsc::Receiver<RaftCommand<LogCommand>>,
        tx: mpsc::Sender<LogCommand>,
    ) -> Self {
        Self {
            id: PeerId(0),
            term: Term(0),
            commit_index: None,
            last_applied: None,
            state: NodeState::Follower,
            peers: vec![],
            transport,
            voted_for: None,
            log: vec![],
            event_receiver: rx,
            commit_channel: tx,
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
            RaftCommand::AppendEntries { request, tx } => {
                let response = self.handle_append_entries(request);
                if response.success {
                    self.state = NodeState::Follower;
                }
                tx.send(response);
            }
            RaftCommand::RequestVote { request, tx } => {
                let response = self.handle_request_vote(request);
                if response.vote_granted {
                    self.state = NodeState::Follower;
                }
                tx.send(response);
            }
            RaftCommand::SubmitEntry { request, tx } => {
                let result = self.submit_entry(request);
                tx.send(result);
            }
        }
    }
    fn update_commit_index(&mut self, new_commit_index: Option<usize>) {
        println!(
            "Raft Instance {:?} updating commit index {:?}",
            self.id, new_commit_index
        );
        if new_commit_index > self.commit_index {
            let old_commit_index = self.commit_index;
            self.commit_index = new_commit_index;
            for item in &self.log
                [old_commit_index.unwrap_or_default()..new_commit_index.unwrap_or_default()]
            {
                // TODO: possibly switch this to a blocking channel
                let send_event = self.commit_channel.try_send(item.command.clone());
            }
        }
    }
    async fn send_append_entries_helper(&self) -> Vec<(PeerId, usize, AppendEntriesResponse)> {
        let mut jhs = FuturesUnordered::new();

        for peer in &self.peers {
            let peer_id = peer.id;
            let prev_log_index = peer.log_index;
            let prev_log_term = prev_log_index.map(|i| self.log[i].term);
            let entries = self
                .log
                .iter()
                .skip(peer.log_index.map_or_else(|| 0, |v| v + 1))
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
                    .append_entries(peer.id, append_entries_request)
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
        } else {
            self.log = req.entries;
        }
        if req.leader_commit.is_none() || self.log.len() == 0 {
            self.update_commit_index(None);
        } else {
            self.update_commit_index(Some(req.leader_commit.unwrap().min(self.log.len() - 1)));
        }
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
                    if entries_len > 0 {
                        peer.log_index = Some(
                            peer.log_index
                                .map_or_else(|| entries_len - 1, |v| v + entries_len),
                        );
                    }
                }
            } else if result.term > self.term {
                self.term = result.term;
                self.state = NodeState::Follower;
                return;
            } else {
                let peer = self.peers.iter_mut().find(|p| p.id == peer_id);
                // TODO: Actually have peers send their last log index here
                if let Some(peer) = peer {
                    peer.log_index = peer
                        .log_index
                        .map(|v| if v == 0 { None } else { Some(v - 1) })
                        .flatten();
                }
            }
        }
        let mut indices = self.peers.iter().map(|p| p.log_index).collect::<Vec<_>>();
        indices.sort_unstable();
        let new_commit_index = indices[(indices.len() - 1) / 2];
        self.update_commit_index(new_commit_index);
    }
    pub async fn get_votes(&mut self) -> VoteResult<LogCommand> {
        let cur_term = self.term;
        let needs_votes = (self.peers.len() + 1) / 2;
        let candidate_id = self.id;
        let last_log_index = if self.log.is_empty() {
            None
        } else {
            Some(self.log.len() - 1)
        };
        let last_log_term = last_log_index.map(|i| self.log[i].term);
        let peers = self.peers.clone();
        let mut jhs = peers
            .into_iter()
            .map(|peer| {
                self.transport.request_votes(
                    peer.id,
                    RequestVoteRequest {
                        candidate_id,
                        term: cur_term,
                        last_log_index,
                        last_log_term,
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
                    eprintln!("Raft Instance {:?} vote timed out", candidate_id);
                    return VoteResult::TimedOut;
                  }
                  res = jhs.next() => {
                    if let Some(res) = res {
                      if res.vote_granted {
                        total_votes += 1;
                        if total_votes >= needs_votes {
                            eprintln!("Raft Instance {:?} won vote", candidate_id);

                            return VoteResult::Winner;
                        }
                      } else if res.term > self.term {
                            self.term = res.term;
                          return VoteResult::BiggerTerm(res.term);
                      }
                    } else {
                      return VoteResult::Loser;
                    }
                  },
                  command = self.event_receiver.recv() => {
                    match command.unwrap() {
                        RaftCommand::SubmitEntry { request, tx } => {
                            tx.send(Err(AppendError::NotLeader));
                        },
                        RaftCommand::AppendEntries { request, tx } => {
                            if request.term >= self.term {
                                return VoteResult::OtherLeaderAppendRequest(request, tx);
                            } else {
                                tx.send(AppendEntriesResponse { term: self.term, success: false });
                            }
                        },
                        RaftCommand::RequestVote { request, tx } => {
                            if request.term > self.term  {
                                tx.send(RequestVoteResponse {
                                    term: self.term,
                                    vote_granted: true
                                });
                                eprintln!("Raft Instance {:?} voting for {:?}", self.id, request.candidate_id);
                                self.voted_for = Some(request.candidate_id);
                                self.state = NodeState::Follower;
                                self.term = request.term;
                                return VoteResult::BiggerTerm(request.term);
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
                    let vote_result = self.get_votes().await;
                    match vote_result {
                        VoteResult::Winner => {
                            self.state = NodeState::Leader;
                        }
                        VoteResult::BiggerTerm(term) => {
                            self.term = term;
                            self.state = NodeState::Follower;
                        }
                        VoteResult::TimedOut => {
                            self.state = NodeState::Follower;
                        }
                        VoteResult::Loser => self.state = NodeState::Follower,
                        VoteResult::OtherLeaderAppendRequest(req, tx) => {
                            self.state = NodeState::Follower;
                            tx.send(self.handle_append_entries(req));
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
                                let response = self.handle_request_vote(request);
                                tx.send(response);
                            }
                        }
                      }
                    }
                }
            }
        }
    }
    fn handle_request_vote(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        if req.term < self.term {
            eprintln!(
                "Raft Instance {:?} refusing vote for {:?}",
                self.id, req.candidate_id
            );
            return RequestVoteResponse {
                term: self.term,
                vote_granted: false,
            };
        } else if req.term > self.term {
            self.term = req.term;
            self.voted_for = None;
        }
        if self.voted_for == Some(req.candidate_id) {
            return RequestVoteResponse {
                term: self.term,
                vote_granted: true,
            };
        }
        let last_log_index = if self.log.is_empty() {
            None
        } else {
            Some(self.log.len() - 1)
        };
        let last_log_term = last_log_index.map(|i| self.log[i].term);

        if self.voted_for.is_none()
            && (req.last_log_term > last_log_term
                || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index))
        {
            eprintln!(
                "Raft Instance {:?} voting for {:?}",
                self.id, req.candidate_id
            );
            RequestVoteResponse {
                term: self.term,
                vote_granted: true,
            }
        } else {
            eprintln!(
                "Raft Instance {:?} refusing vote for {:?}",
                self.id, req.candidate_id
            );
            RequestVoteResponse {
                term: self.term,
                vote_granted: false,
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
            peer_id: crate::PeerId,
            msg: AppendEntriesRequest<String>,
        ) -> AppendEntriesResponse {
            let (tx, rx) = oneshot::channel();
            self.ae_channel
                .send(Message {
                    peer_id: peer_id,
                    message: msg,
                    tx,
                })
                .await;
            rx.await.unwrap()
        }

        async fn request_votes(
            &self,
            peer_id: crate::PeerId,
            msg: RequestVoteRequest,
        ) -> RequestVoteResponse {
            let (tx, rx) = oneshot::channel();
            self.rv_channel
                .send(Message {
                    peer_id: peer_id,
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
        let mut comms_channels = vec![];
        for i in 0..3 {
            let (tx1, rx1) = channel(100);
            let (tx4, rx4) = channel(100);
            comms_channels.push(tx1.clone());
            let transport = LocalTransport {
                ae_channel: tx2.clone(),
                rv_channel: tx3.clone(),
            };
            let raft_node = RaftNode {
                id: PeerId(i),
                event_emitter: tx1,
            };
            nodes.insert(i, raft_node);
            let mut raft = Raft::<_, String>::new(transport, rx1, tx4);
            raft.install_config(crate::Topology {
                node_id: PeerId(i),
                peers: (0..3).map(|i| Peer::new(PeerId(i))).collect(),
            });
            tokio::spawn(raft.run());
        }
        let fut = tokio::time::sleep(Duration::from_millis(5000));
        tokio::pin!(fut);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            for i in 0..40 {
                for item in &mut comms_channels {
                    let (tx, rx) = oneshot::channel();
                    item.send(RaftCommand::SubmitEntry {
                        request: format!("Iteration number {}", i),
                        tx,
                    })
                    .await;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
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
