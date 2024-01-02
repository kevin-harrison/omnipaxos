use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::util::{MessageStatus, WRITE_ERROR_MSG};

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** Follower ***/
    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) {
        let old_promise = self.internal_storage.get_promise();
        if old_promise < prep.n || (old_promise == prep.n && self.state.1 == Phase::Recover) {
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_promise(prep.n, from)
                .expect(WRITE_ERROR_MSG);
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let log_sync = if na > prep.n_accepted {
                // I'm more up to date: send leader what he is missing after his decided index.
                Some(self.create_log_sync(prep.decided_idx, prep.decided_idx))
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                // I'm more up to date and in same round: send leader what he is missing after his
                // accepted index.
                Some(self.create_log_sync(prep.accepted_idx, prep.decided_idx))
            } else {
                // I'm equally or less up to date
                None
            };
            let promise = Promise {
                n: prep.n,
                n_accepted: na,
                decided_idx: self.internal_storage.get_decided_idx(),
                accepted_idx,
                log_sync,
                config_log: self.internal_storage.get_config_log(),
            };
            self.cached_promise_message = Some(promise.clone());
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Promise(promise),
            });
        }
    }

    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.check_valid_ballot(accsync.n, from)
            && self.state == (Role::Follower, Phase::Prepare)
        {
            let (new_accepted_idx, new_config_accepted_idx) = self
                .internal_storage
                .sync_logs(
                    accsync.n,
                    accsync.decided_idx,
                    Some(accsync.log_sync),
                    accsync.config_log,
                )
                .expect(WRITE_ERROR_MSG);
            if self.internal_storage.get_stopsign().is_none() {
                // TODO: forward buffered reconfig
                self.forward_buffered_proposals();
            }
            let accepted = Accepted {
                n: accsync.n,
                accepted_idx: new_accepted_idx,
            };
            self.state = (Role::Follower, Phase::Accept);
            self.current_seq_num = accsync.seq_num;
            let cached_idx = self.outgoing.len();
            self.latest_accepted_meta = Some((accsync.n, from, cached_idx));
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            });
            self.reply_accepted_config(accsync.n, from, new_config_accepted_idx);
            #[cfg(feature = "unicache")]
            self.internal_storage.set_unicache(accsync.unicache);
        }
    }

    fn forward_buffered_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.buffered_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    pub(crate) fn handle_acceptdecide(&mut self, acc_dec: AcceptDecide<T>, from: NodeId) {
        if self.check_valid_ballot(acc_dec.n, from)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_dec.seq_num, from) == MessageStatus::Expected
        {
            #[cfg(not(feature = "unicache"))]
            let entries = acc_dec.entries;
            #[cfg(feature = "unicache")]
            let entries = self.internal_storage.decode_entries(acc_dec.entries);
            let mut new_accepted_idx = self
                .internal_storage
                .append_entries_and_get_accepted_idx(entries)
                .expect(WRITE_ERROR_MSG);
            let flushed_after_decide =
                self.update_decided_idx_and_get_accepted_idx(acc_dec.decided_idx);
            if flushed_after_decide.is_some() {
                new_accepted_idx = flushed_after_decide;
            }
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(acc_dec.n, from, idx);
            }
        }
    }

    pub(crate) fn handle_accept_quorum_config(&mut self, acc_qc: AcceptConfig, from: NodeId) {
        if self.check_valid_ballot(acc_qc.n, from)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_qc.seq_num, from) == MessageStatus::Expected
        {
            // NOTE: We update quorum here because BLE needs to know current quorum in order to
            // function correctly
            let config_accepted_idx = self
                .internal_storage
                .append_quorum_config(acc_qc.quorum_config)
                .expect(WRITE_ERROR_MSG);
            self.reply_accepted_config(acc_qc.n, from, config_accepted_idx);
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign, from: NodeId) {
        if self.check_valid_ballot(acc_ss.n, from)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, from) == MessageStatus::Expected
        {
            // Flush entries before appending stopsign. The accepted index is ignored here as
            // it will be updated when appending stopsign.
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            let new_accepted_idx = self
                .internal_storage
                .set_stopsign(Some(acc_ss.ss))
                .expect(WRITE_ERROR_MSG);
            self.reply_accepted(acc_ss.n, from, new_accepted_idx);
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide, from: NodeId) {
        if self.check_valid_ballot(dec.n, from)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, from) == MessageStatus::Expected
        {
            let new_accepted_idx = self.update_decided_idx_and_get_accepted_idx(dec.decided_idx);
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(dec.n, from, idx);
            }
        }
    }

    pub(crate) fn handle_decide_config(&mut self, dec_config: DecideConfig, from: NodeId) {
        if self.check_valid_ballot(dec_config.n, from)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec_config.seq_num, from) == MessageStatus::Expected
        {
            self.internal_storage
                .set_config_decided_idx(dec_config.decided_idx)
                .expect(WRITE_ERROR_MSG);
        }
    }

    pub(crate) fn handle_relinquished_leadership(
        &mut self,
        rel: RelinquishedLeadership<T>,
        from: NodeId,
    ) {
        if self.check_valid_ballot(rel.leader_state.n_leader, from)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(rel.seq_num, from) == MessageStatus::Expected
        {
            self.leader_state = rel.leader_state;
            self.leader_state.pid = self.pid;
            self.leader_state.batch_accept_meta = vec![None; self.leader_state.max_pid];
            self.state = (Role::Leader, Phase::Accept);
            self.internal_storage
                .set_promise(self.get_promise(), self.pid)
                .expect(WRITE_ERROR_MSG);
            let accepted = Accepted {
                n: self.leader_state.n_leader,
                accepted_idx: self.internal_storage.get_accepted_idx(),
            };
            self.handle_accepted(accepted, self.pid);
            let accepted_config = AcceptedConfig {
                n: self.leader_state.n_leader,
                accepted_idx: self.internal_storage.get_config_log().accepted_idx,
            };
            self.handle_accepted_config(accepted_config, self.pid);
        }
    }

    /// To maintain decided index <= accepted index, batched entries may be flushed.
    /// Returns `Some(new_accepted_idx)` if entries are flushed, otherwise `None`.
    fn update_decided_idx_and_get_accepted_idx(&mut self, new_decided_idx: usize) -> Option<usize> {
        if new_decided_idx <= self.internal_storage.get_decided_idx() {
            return None;
        }
        if new_decided_idx > self.internal_storage.get_accepted_idx() {
            let new_accepted_idx = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_decided_idx(new_decided_idx.min(new_accepted_idx))
                .expect(WRITE_ERROR_MSG);
            Some(new_accepted_idx)
        } else {
            self.internal_storage
                .set_decided_idx(new_decided_idx)
                .expect(WRITE_ERROR_MSG);
            None
        }
    }

    fn reply_accepted(&mut self, n: Ballot, from: NodeId, accepted_idx: usize) {
        match &self.latest_accepted_meta {
            Some((round, leader, outgoing_idx)) if *round == n && *leader == from => {
                let PaxosMessage { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                match msg {
                    PaxosMsg::Accepted(a) => {
                        a.accepted_idx = accepted_idx;
                    }
                    _ => panic!("Cached idx is not an Accepted Message<T>!"),
                }
            }
            _ => {
                let accepted = Accepted { n, accepted_idx };
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((n, from, cached_idx));
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: from,
                    msg: PaxosMsg::Accepted(accepted),
                });
            }
        };
    }

    fn reply_accepted_config(&mut self, n: Ballot, from: NodeId, accepted_idx: usize) {
        let accepted = AcceptedConfig { n, accepted_idx };
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to: from,
            msg: PaxosMsg::AcceptedConfig(accepted),
        });
    }

    /// Also returns whether the message's ballot was promised
    fn check_valid_ballot(&mut self, message_ballot: Ballot, from: NodeId) -> bool {
        let my_promise = self.internal_storage.get_promise();
        match my_promise.cmp(&message_ballot) {
            std::cmp::Ordering::Equal => {
                if from != self.get_leader() {
                    self.internal_storage
                        .set_promise(message_ballot, from)
                        .expect(WRITE_ERROR_MSG);
                }
                true
            }
            std::cmp::Ordering::Greater => {
                let not_acc = NotAccepted { n: my_promise };
                #[cfg(feature = "logging")]
                trace!(
                    self.logger,
                    "NotAccepted. My promise: {:?}, theirs: {:?}",
                    my_promise,
                    message_ballot
                );
                self.outgoing.push(PaxosMessage {
                    from: self.pid,
                    to: from,
                    msg: PaxosMsg::NotAccepted(not_acc),
                });
                false
            }
            std::cmp::Ordering::Less => {
                // Should never happen, but to be safe send PrepareReq
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "Received non-prepare message from a leader I've never promised. My: {:?}, theirs: {:?}", my_promise, message_ballot
                );
                self.reconnected(from);
                false
            }
        }
    }

    /// Also returns the MessageStatus of the sequence based on the incoming sequence number.
    fn handle_sequence_num(&mut self, seq_num: SequenceNumber, from: NodeId) -> MessageStatus {
        let msg_status = self.current_seq_num.check_msg_status(seq_num);
        match msg_status {
            MessageStatus::Expected => self.current_seq_num = seq_num,
            MessageStatus::DroppedPreceding => self.reconnected(from),
            MessageStatus::Outdated => (),
        };
        msg_status
    }

    pub(crate) fn resend_messages_follower(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Promise
                match &self.cached_promise_message {
                    Some(promise) => {
                        self.outgoing.push(PaxosMessage {
                            from: self.pid,
                            to: self.get_leader(),
                            msg: PaxosMsg::Promise(promise.clone()),
                        });
                    }
                    None => {
                        // Shouldn't be possible to be in prepare phase without having
                        // cached the promise sent as a response to the prepare
                        #[cfg(feature = "logging")]
                        warn!(self.logger, "In Prepare phase without a cached promise!");
                        self.state = (Role::Follower, Phase::Recover);
                        self.send_preparereq_to_all_peers();
                    }
                }
            }
            Phase::Recover => {
                // Resend PrepareReq
                self.send_preparereq_to_all_peers();
            }
            Phase::Accept => (),
            Phase::None => (),
        }
    }

    fn send_preparereq_to_all_peers(&mut self) {
        let prepreq = PrepareReq {
            n: self.get_promise(),
        };
        for peer in &self.peers {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: *peer,
                msg: PaxosMsg::PrepareReq(prepreq),
            });
        }
    }

    pub(crate) fn flush_batch_follower(&mut self) {
        let accepted_idx = self.internal_storage.get_accepted_idx();
        let new_accepted_idx = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
        if new_accepted_idx > accepted_idx {
            let promise = self.get_promise();
            let leader = self.get_leader();
            self.reply_accepted(promise, leader, new_accepted_idx);
        }
    }
}
