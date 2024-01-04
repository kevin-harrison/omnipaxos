use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseMetaData},
};
use crate::util::{AcceptedMetaData, WRITE_ERROR_MSG};

use super::*;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) fn handle_leader(&mut self, n: Ballot) {
        if n <= self.leader_state.n_leader || n <= self.internal_storage.get_promise() {
            return;
        }
        #[cfg(feature = "logging")]
        debug!(self.logger, "Newly elected leader: {:?}", n);
        if self.pid == n.pid {
            self.leader_state = LeaderState::with(n, self.pid, self.leader_state.max_pid);
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_promise(n, self.pid)
                .expect(WRITE_ERROR_MSG);
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_idx,
                accepted_idx,
                log_sync: None,
                config_log: self.internal_storage.get_config_log(),
            };
            self.leader_state.set_promise(my_promise, self.pid, true);
            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
            let prep = Prepare {
                n,
                decided_idx,
                n_accepted: na,
                accepted_idx,
            };
            /* send prepare */
            for pid in &self.peers {
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg: PaxosMsg::Prepare(prep),
                });
            }
        } else {
            self.become_follower();
        }
    }

    fn become_follower(&mut self) {
        self.state.0 = Role::Follower;
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader && prepreq.n <= self.leader_state.n_leader {
            self.leader_state.reset_promise(from);
            self.leader_state.set_batch_accept_meta(from, None);
            self.send_prepare(from);
        }
    }

    pub(crate) fn handle_forwarded_proposal(&mut self, mut entries: Vec<T>) {
        if !self.pending_stopsign() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.buffered_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.accept_entries_leader(entries),
                _ => self.forward_proposals(entries),
            }
        }
    }

    pub(crate) fn handle_forwarded_reconfig(&mut self, reconfig: ClusterConfig) {
        let _ = self.reconfigure_joint_consensus(reconfig);
    }

    pub(crate) fn handle_forwarded_stopsign(&mut self, ss: StopSign) {
        if self.pending_stopsign() {
            return;
        }
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
    }

    pub(crate) fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
            n_accepted: self.internal_storage.get_accepted_round(),
            accepted_idx: self.internal_storage.get_accepted_idx(),
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Prepare(prep),
        });
    }

    pub(crate) fn accept_entry_leader(&mut self, entry: T) {
        let accepted_metadata = self
            .internal_storage
            .append_entry_with_batching(entry)
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    pub(crate) fn accept_entries_leader(&mut self, entries: Vec<T>) {
        let accepted_metadata = self
            .internal_storage
            .append_entries_with_batching(entries)
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    pub(crate) fn accept_quorum_config_leader(&mut self, quorum_config: QuorumConfig) {
        let config_accepted_idx = self
            .internal_storage
            .append_quorum_config(quorum_config)
            .expect(WRITE_ERROR_MSG);
        // TODO: do we to revisit prev entries to check if they are now decided in the new quorum
        // definition? Maybe on syncing we would have to?
        self.leader_state
            .set_config_accepted_idx(self.pid, config_accepted_idx);
        self.send_accept_quorum_config(quorum_config);
    }

    pub(crate) fn accept_stopsign_leader(&mut self, ss: StopSign) {
        let accepted_metadata = self
            .internal_storage
            .append_stopsign(ss.clone())
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.send_acceptdecide(metadata);
        }
        let accepted_idx = self.internal_storage.get_accepted_idx();
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accept_stopsign(pid, ss.clone(), false);
        }
    }

    fn send_accsync(&mut self, to: NodeId) {
        let current_n = self.leader_state.n_leader;
        let PromiseMetaData {
            n_accepted: prev_round_max_promise_n,
            accepted_idx: prev_round_max_accepted_idx,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n_accepted: followers_promise_n,
            accepted_idx: followers_accepted_idx,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let followers_decided_idx = self
            .leader_state
            .get_decided_idx(*pid)
            .expect("Received PromiseMetaData but not found in ld");
        // Follower can have valid accepted entries depending on which leader they were previously following
        let followers_valid_entries_idx = if *followers_promise_n == current_n {
            *followers_accepted_idx
        } else if *followers_promise_n == *prev_round_max_promise_n {
            *prev_round_max_accepted_idx.min(followers_accepted_idx)
        } else {
            followers_decided_idx
        };
        let log_sync = self.create_log_sync(followers_valid_entries_idx, followers_decided_idx);
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.get_decided_idx(),
            log_sync,
            #[cfg(feature = "unicache")]
            unicache: self.internal_storage.get_unicache(),
            config_log: self.internal_storage.get_config_log(),
        };
        let msg = PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptSync(acc_sync),
        };
        self.outgoing.push(msg);
    }

    fn send_acceptdecide(&mut self, accepted: AcceptedMetaData<T>) {
        let decided_idx = self.internal_storage.get_decided_idx();
        for pid in self.leader_state.get_promised_followers() {
            let cached_acceptdecide = match self.leader_state.get_batch_accept_meta(pid) {
                Some((bal, msg_idx)) if bal == self.leader_state.n_leader => {
                    let PaxosMessage { msg, .. } = self.outgoing.get_mut(msg_idx).unwrap();
                    match msg {
                        PaxosMsg::AcceptDecide(acc) => Some(acc),
                        _ => panic!("Cached index is not an AcceptDecide!"),
                    }
                }
                _ => None,
            };
            match cached_acceptdecide {
                // Modify existing AcceptDecide message to follower
                Some(acc) => {
                    acc.entries.append(accepted.entries.clone().as_mut());
                    acc.decided_idx = decided_idx;
                }
                // Add new AcceptDecide message to follower
                None => {
                    self.leader_state
                        .set_batch_accept_meta(pid, Some(self.outgoing.len()));
                    let acc = AcceptDecide {
                        n: self.leader_state.n_leader,
                        seq_num: self.leader_state.next_seq_num(pid),
                        decided_idx,
                        entries: accepted.entries.clone(),
                    };
                    self.outgoing.push(PaxosMessage {
                        from: self.pid,
                        to: pid,
                        msg: PaxosMsg::AcceptDecide(acc),
                    });
                }
            }
        }
    }

    fn send_accept_quorum_config(&mut self, quorum_config: QuorumConfig) {
        for pid in self.leader_state.get_promised_followers() {
            let acc_qc = PaxosMsg::AcceptConfig(AcceptConfig {
                seq_num: self.leader_state.next_seq_num(pid),
                n: self.leader_state.n_leader,
                quorum_config,
            });
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: pid,
                msg: acc_qc,
            });
        }
    }

    fn send_accept_stopsign(&mut self, to: NodeId, ss: StopSign, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            seq_num,
            n: self.leader_state.n_leader,
            ss,
        });
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: acc_ss,
        });
    }

    pub(crate) fn send_decide(&mut self, to: NodeId, decided_idx: usize, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let d = Decide {
            n: self.leader_state.n_leader,
            seq_num,
            decided_idx,
        };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Decide(d),
        });
    }

    // TODO: add resend funcionality
    pub(crate) fn send_decide_config(&mut self, decided_idx: usize) {
        for pid in self.leader_state.get_promised_followers() {
            let acc_qc = PaxosMsg::DecideConfig(DecideConfig {
                seq_num: self.leader_state.next_seq_num(pid),
                n: self.leader_state.n_leader,
                decided_idx,
            });
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: pid,
                msg: acc_qc,
            });
        }
    }

    fn handle_majority_promises(&mut self) {
        let max_promise_sync = self.leader_state.take_max_promise_sync();
        let decided_idx = self.leader_state.get_max_decided_idx();
        let max_promise_config = self
            .leader_state
            .get_max_promise_config()
            .expect("Exiting prepare phase without any promises.");
        let (mut new_accepted_idx, mut new_config_accepted_idx) = self
            .internal_storage
            .sync_logs(
                self.leader_state.n_leader,
                decided_idx,
                max_promise_sync,
                max_promise_config,
            )
            .expect(WRITE_ERROR_MSG);
        if !self.pending_stopsign() {
            if !self.buffered_proposals.is_empty() {
                let entries = std::mem::take(&mut self.buffered_proposals);
                new_accepted_idx = self
                    .internal_storage
                    .append_entries_without_batching(entries)
                    .expect(WRITE_ERROR_MSG);
            }
            if !self.pending_quorum_reconfig() {
                if let Some(config) = self.buffered_reconfig.take() {
                    new_config_accepted_idx = self
                        .internal_storage
                        .append_quorum_config(self.create_next_quorum(config))
                        .expect(WRITE_ERROR_MSG);
                }
            }
            if let Some(ss) = self.buffered_stopsign.take() {
                self.internal_storage
                    .append_stopsign(ss)
                    .expect(WRITE_ERROR_MSG);
                new_accepted_idx = self.internal_storage.get_accepted_idx();
            }
        }
        self.state = (Role::Leader, Phase::Accept);
        self.leader_state
            .set_accepted_idx(self.pid, new_accepted_idx);
        self.leader_state
            .set_config_accepted_idx(self.pid, new_config_accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_prepare(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let promised_nodes = self.leader_state.set_promise(prom, from, true);
            if self
                .internal_storage
                .get_quorum()
                .is_prepare_quorum(promised_nodes)
            {
                let max_promise_config = self
                    .leader_state
                    .get_max_promise_config()
                    .expect("Exiting prepare phase without any promises.");
                // TODO: review why we dont need to wait for the max prepare quorum of the config
                // entries we discover and instead can just rely on the most up-to-date one.
                if max_promise_config
                    .config
                    .get_active_quorum()
                    .is_prepare_quorum(promised_nodes)
                {
                    self.handle_majority_promises();
                }
            }
        }
    }

    pub(crate) fn handle_promise_accept(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from);
        }
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted, from: NodeId) {
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}, accepted: {:?}",
            from,
            accepted.accepted_idx,
            self.internal_storage.get_decided_idx(),
            self.leader_state.accepted_indexes
        );
        if accepted.n != self.leader_state.n_leader || self.state != (Role::Leader, Phase::Accept) {
            return;
        }
        self.leader_state
            .set_accepted_idx(from, accepted.accepted_idx);
        let quorum = self.internal_storage.get_quorum();
        if accepted.accepted_idx > self.internal_storage.get_decided_idx()
            && self.leader_state.is_chosen(accepted.accepted_idx, quorum)
        {
            let new_decided_idx = accepted.accepted_idx;
            self.internal_storage
                .set_decided_idx(new_decided_idx)
                .expect(WRITE_ERROR_MSG);
            for pid in self.leader_state.get_promised_followers() {
                match self.leader_state.get_batch_accept_meta(pid) {
                    Some((bal, msg_idx)) if bal == self.leader_state.n_leader => {
                        let PaxosMessage { msg, .. } = self.outgoing.get_mut(msg_idx).unwrap();
                        match msg {
                            PaxosMsg::AcceptDecide(acc) => acc.decided_idx = new_decided_idx,
                            _ => panic!("Cached index is not an AcceptDecide!"),
                        }
                    }
                    _ => self.send_decide(pid, new_decided_idx, false),
                };
            }
        }
    }

    pub(crate) fn handle_accepted_config(&mut self, acc_config: AcceptedConfig, from: NodeId) {
        if acc_config.n != self.leader_state.n_leader || self.state != (Role::Leader, Phase::Accept)
        {
            return;
        }
        self.leader_state
            .set_config_accepted_idx(from, acc_config.accepted_idx);
        let quorum = self.internal_storage.get_quorum();
        if acc_config.accepted_idx > self.internal_storage.get_config_decided_idx()
            && self
                .leader_state
                .is_config_chosen(acc_config.accepted_idx, quorum)
        {
            let new_decided_idx = acc_config.accepted_idx;
            self.internal_storage
                .set_config_decided_idx(new_decided_idx)
                .expect(WRITE_ERROR_MSG);
            self.send_decide_config(new_decided_idx);
            self.handle_decided_transition_config();
        }
    }

    pub(crate) fn handle_notaccepted(&mut self, not_acc: NotAccepted, from: NodeId) {
        if self.state.0 == Role::Leader && self.leader_state.n_leader < not_acc.n {
            self.leader_state.lost_promise(from);
        }
    }

    pub(crate) fn resend_messages_leader(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers();
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Accept => {
                // Resend AcceptStopSign or StopSign's decide
                if let Some(ss) = self.internal_storage.get_stopsign() {
                    let decided_idx = self.internal_storage.get_decided_idx();
                    for follower in self.leader_state.get_promised_followers() {
                        if self.internal_storage.stopsign_is_decided() {
                            self.send_decide(follower, decided_idx, true);
                        } else if self.leader_state.get_accepted_idx(follower)
                            != self.internal_storage.get_accepted_idx()
                        {
                            self.send_accept_stopsign(follower, ss.clone(), true);
                        }
                    }
                }
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers();
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Recover => (),
            Phase::None => (),
        }
    }

    pub(crate) fn flush_batch_leader(&mut self) {
        let accepted_metadata = self
            .internal_storage
            .flush_batch_and_get_entries()
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    pub(crate) fn relinquish_leadership(&mut self, to: NodeId) -> Option<bool> {
        if self.state.0 != Role::Leader || to == self.pid {
            None
        } else if self.state.1 == Phase::Accept
            && self.leader_state.get_promised_followers().contains(&to)
        {
            self.leader_state.increment_seq_num_session(self.pid);
            self.current_seq_num = self.leader_state.next_seq_num(self.pid);
            self.latest_accepted_meta = None;
            let rel_msg = RelinquishedLeadership {
                seq_num: self.leader_state.next_seq_num(to),
                leader_state: self.leader_state.clone(),
            };
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to,
                msg: PaxosMsg::RelinquishedLeadership(rel_msg),
            });
            self.state = (Role::Follower, Phase::Accept);
            self.internal_storage
                .set_promise(self.get_promise(), to)
                .expect(WRITE_ERROR_MSG);
            Some(true)
        } else {
            Some(false)
        }
    }

    fn handle_decided_transition_config(&mut self) {
        let current_quorum_config = self.internal_storage.get_quorum_config();
        if let QuorumConfig::Transitional(_, qc) = current_quorum_config {
            if self.internal_storage.config_is_decided() {
                self.accept_quorum_config_leader(QuorumConfig::Stable(qc));
            }
        }
    }
}
