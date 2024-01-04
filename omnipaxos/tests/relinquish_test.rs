pub mod utils;

use serial_test::serial;
use utils::{verification::verify_log, TestConfig, TestSystem};

/// Verifies that the decided StopSign is correct and error is returned when trying to append after decided StopSign.
#[test]
#[serial]
fn relinquish_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);

    let initial_proposals = utils::create_proposals(1, cfg.num_proposals / 2);
    let last_proposals = utils::create_proposals(cfg.num_proposals / 2 + 1, cfg.num_proposals);
    let expected_log = utils::create_proposals(1, cfg.num_proposals);
    sys.start_all_nodes();

    // Make some initial proposals.
    sys.make_proposals(1, initial_proposals, cfg.wait_timeout);

    // Current leader relinquishes leadership to another node.
    let prev_leader_id = sys.get_elected_leader(1, cfg.wait_timeout);
    let prev_leader = sys.nodes.get(&prev_leader_id).unwrap();
    let next_leader_id = *sys
        .nodes
        .keys()
        .filter(|k| **k != prev_leader_id)
        .next()
        .unwrap();
    prev_leader.on_definition(|x| {
        x.paxos.relinquish_leadership(next_leader_id);
    });

    // Make some more propsals
    sys.make_proposals(1, last_proposals, cfg.wait_timeout);

    // Verify leadership change
    for pid in sys.nodes.keys() {
        let nodes_leader = sys.get_elected_leader(*pid, cfg.wait_timeout);
        assert_eq!(nodes_leader, next_leader_id);
    }

    // Verify log
    let leader = sys.nodes.get(&next_leader_id).unwrap();
    let leaders_log = leader.on_definition(|x| x.read_decided_log());
    verify_log(leaders_log, expected_log);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
