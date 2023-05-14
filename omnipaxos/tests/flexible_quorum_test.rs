pub mod utils;

use omnipaxos::util::NodeId;
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{verification::verify_log, TestConfig, TestSystem, Value};

/// Verifies that an OmniPaxos cluster with an append quorum size of A can still make
/// progress with A-1 failures, including leader failure.
#[test]
#[serial]
fn flexible_quorum_prepare_phase_test() {
    // Start Kompact system
    let cfg = TestConfig::load("flexible_quorum_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg.clone());
    sys.start_all_nodes();

    let initial_proposals = (0..cfg.num_proposals / 2)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let last_proposals: Vec<Value> = ((cfg.num_proposals / 2)..cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_log: Vec<Value> = (0..cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();

    // Propose some initial values
    sys.make_proposals(
        2,
        initial_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );
    let leader_id = sys.get_elected_leader(1, Duration::from_millis(cfg.wait_timeout_ms));

    // Kill maximum number of nodes (including leader) such that cluster can still function
    let maximum_tolerable_follower_failures = (1..=cfg.num_nodes as NodeId)
        .into_iter()
        .filter(|id| *id != leader_id)
        .take(cfg.append_quorum_size.unwrap() - 2);
    for node_id in maximum_tolerable_follower_failures {
        sys.kill_node(node_id);
    }
    sys.kill_node(leader_id);

    // Wait for next leader to get elected
    thread::sleep(Duration::from_millis(3 * cfg.election_timeout_ms));

    // Make some more propsals
    let still_alive_node_id = sys.nodes.keys().next().unwrap();
    let still_alive_node = sys.nodes.get(still_alive_node_id).unwrap();
    sys.make_proposals(
        *still_alive_node_id,
        last_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );

    // Verify log
    let nodes_log = still_alive_node.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log(nodes_log, expected_log, cfg.num_proposals);
}

/// Verifies that an OmniPaxos cluster with N nodes and an append quorum size of A can still make
/// progress with N - A failures so long as nodes remain in the accept phase (leader doesn't fail).
#[test]
#[serial]
fn flexible_quorum_accept_phase_test() {
    // Start Kompact system
    let cfg = TestConfig::load("flexible_quorum_test").expect("Test config couldn't be loaded");
    let mut sys = TestSystem::with(cfg.clone());
    sys.start_all_nodes();

    let initial_proposals = (0..cfg.num_proposals / 2)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let last_proposals: Vec<Value> = ((cfg.num_proposals / 2)..cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();
    let expected_log: Vec<Value> = (0..cfg.num_proposals)
        .into_iter()
        .map(|v| Value(v))
        .collect();

    // Propose some values
    sys.make_proposals(
        2,
        initial_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );
    let leader_id = sys.get_elected_leader(1, Duration::from_millis(cfg.wait_timeout_ms));

    // Kill maximum number of followers such that leader can still function
    let maximum_tolerable_follower_failures = (1..=cfg.num_nodes as NodeId)
        .into_iter()
        .filter(|id| *id != leader_id)
        .take(cfg.num_nodes - cfg.append_quorum_size.unwrap());
    for node_id in maximum_tolerable_follower_failures {
        sys.kill_node(node_id);
    }

    // Make some more propsals
    sys.make_proposals(
        leader_id,
        last_proposals,
        Duration::from_millis(cfg.wait_timeout_ms),
    );

    // Verify log
    let leader = sys.nodes.get(&leader_id).unwrap();
    let leaders_log = leader.on_definition(|comp| {
        comp.paxos
            .read_decided_suffix(0)
            .expect("Cannot read decided log entry")
    });
    verify_log(leaders_log, expected_log, cfg.num_proposals);
}
