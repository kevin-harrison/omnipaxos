use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseData, PromiseMetaData},
};
use crate::{
    storage::{Snapshot, SnapshotType, InternalStorageTxn},
    util::{WRITE_ERROR_MSG, AcceptedMetaData},
};

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
            self.leader_state =
                LeaderState::with(n, self.leader_state.max_pid, self.leader_state.quorum);
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage.set_promise(n).expect(WRITE_ERROR_MSG);
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_snapshot: None,
                decided_idx,
                accepted_idx,
                suffix: vec![],
                stopsign: self.internal_storage.get_stopsign(),
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

    pub(crate) fn become_follower(&mut self) {
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
        if !self.accepted_reconfiguration() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.pending_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.accept_entries_leader(entries),
                _ => self.forward_proposals(entries),
            }
        }
    }

    pub(crate) fn handle_forwarded_stopsign(&mut self, ss: StopSign) {
        if self.accepted_reconfiguration() {
            return;
        }
        match self.state {
            (Role::Leader, Phase::Prepare) => self.pending_stopsign = Some(ss),
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
        let accepted_metadata = self.internal_storage.append_entry_with_batching(entry).expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state.set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    pub(crate) fn accept_entries_leader(&mut self, entries: Vec<T>) {
        let accepted_metadata = self.internal_storage.append_entries_with_batching(entries).expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state.set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    pub(crate) fn accept_stopsign_leader(&mut self, ss: StopSign) {
        let accepted_metadata = self.internal_storage.append_stopsign(ss.clone()).expect(WRITE_ERROR_MSG);
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
        let my_decided_idx = self.get_decided_idx();
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
        let (delta_snapshot, suffix, sync_idx) =
            if T::Snapshot::use_snapshots() && followers_valid_entries_idx < my_decided_idx {
                // Synchronize by sending a snapshot from the follower's decided index up to
                // leader's decided index and any suffix.
                // Note: we snapshot from follower's decided and not follower's valid because
                // snapshots currently can't handle merging onto accepted entries.
                let (delta_snapshot, compacted_idx) = self
                    .internal_storage
                    .create_diff_snapshot(followers_decided_idx)
                    .expect("storage error while trying to read diff snapshot");
                let suffix = self
                    .internal_storage
                    .get_suffix(my_decided_idx)
                    .expect("storage error while trying to read log suffix");
                (delta_snapshot, suffix, compacted_idx)
            } else {
                let sfx = self
                    .internal_storage
                    .get_suffix(followers_valid_entries_idx)
                    .expect("storage error while trying to read log suffix");
                (None, sfx, followers_valid_entries_idx)
            };
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.leader_state.next_seq_num(to),
            decided_snapshot: delta_snapshot,
            suffix,
            sync_idx,
            decided_idx: my_decided_idx,
            stopsign: self.internal_storage.get_stopsign(),
            #[cfg(feature = "unicache")]
            unicache: self.internal_storage.get_unicache(),
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
            let pending_acceptdecide = match self.leader_state.get_batch_accept_meta(pid) {
                Some((bal, msg_idx)) if bal == self.leader_state.n_leader => {
                    let PaxosMessage { msg, .. } = self.outgoing.get_mut(msg_idx).unwrap();
                    match msg {
                        PaxosMsg::AcceptDecide(acc) => Some(acc),
                        _ => panic!("Cached index is not an AcceptDecide!"),
                    }
                }
                _ => None,
            };
            match pending_acceptdecide {
                // Modify existing pending AcceptDecide message to follower
                Some(acc) => {
                    acc.entries.append(accepted.entries.clone().as_mut());
                    acc.decided_idx = decided_idx;
                }
                // Add new pending AcceptDecide message to follower
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

    fn handle_majority_promises(&mut self) {
        let max_stopsign = self.leader_state.take_max_promise_stopsign();
        let max_promise = self.leader_state.take_max_promise();
        let max_promise_meta = self.leader_state.get_max_promise_meta();
        let decided_idx = self.leader_state.get_max_decided_idx().unwrap();
        let old_accepted_round = self.internal_storage.get_accepted_round();
        let old_decided_idx = self.internal_storage.get_decided_idx();

        self.state = (Role::Leader, Phase::Accept);
        let txn = InternalStorageTxn::new();
        txn.set_accepted_round(self.leader_state.n_leader);
        txn.set_decided_idx(decided_idx);
        if let Some(PromiseData {
            decided_snapshot,
            suffix,
        }) = max_promise
        {
            match decided_snapshot {
                Some(snap) => {
                    let compacted_idx = self
                        .leader_state
                        .get_decided_idx(max_promise_meta.pid)
                        .unwrap();
                    match snap {
                        SnapshotType::Complete(c) => txn.set_snapshot(compacted_idx, c),
                        SnapshotType::Delta(d) => { 
                            let snap = self
                            .internal_storage
                            .create_merged_snapshot(compacted_idx, d)
                            .expect("Error reading from storage.");
                            txn.set_snapshot(compacted_idx, snap);
                        }
                    };
                    txn.append_entries(suffix);
                }
                // No snapshot, only suffix
                None => {
                    if max_promise_meta.n_accepted > old_accepted_round {
                        txn.append_on_prefix(old_decided_idx, suffix);
                    } else if !suffix.is_empty() {
                        txn.append_entries(suffix);
                    }
                }
            }
        }
        match max_stopsign {
            Some(ss) => txn.set_stopsign(Some(ss)),
            None => {
                txn.set_stopsign(None);
                // Adopt pending proposals / stopsign
                if !self.pending_proposals.is_empty() {
                    let new_entries = std::mem::take(&mut self.pending_proposals);
                    txn.append_entries(new_entries);
                }
                if let Some(ss) = self.pending_stopsign.take() {
                    txn.set_stopsign(Some(ss));
                }
            }
        }
        let accepted_idx = self.internal_storage.execute_txn(txn).expect(WRITE_ERROR_MSG);
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
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
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises();
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
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            let old_decided_idx = self.internal_storage.get_decided_idx();
            if accepted.accepted_idx > old_decided_idx
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                let decided_idx = accepted.accepted_idx;
                self.internal_storage.set_decided_idx(decided_idx).expect(WRITE_ERROR_MSG);
                for pid in self.leader_state.get_promised_followers() {
                    match self.leader_state.get_batch_accept_meta(pid) {
                        Some((bal, msg_idx)) if bal == self.leader_state.n_leader => {
                            let PaxosMessage { msg, .. } = self.outgoing.get_mut(msg_idx).unwrap();
                            match msg {
                                PaxosMsg::AcceptDecide(acc) => acc.decided_idx = decided_idx,
                                #[cfg(feature = "unicache")]
                                PaxosMsg::EncodedAcceptDecide(e) => e.decided_idx = decided_idx,
                                _ => panic!("Cached index is not an AcceptDecide!"),
                            }
                        }
                        _ => self.send_decide(pid, decided_idx, false),
                    };
                }
            }
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
}
