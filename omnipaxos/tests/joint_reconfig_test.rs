pub mod utils;

use kompact::prelude::{promise, Ask};
use omnipaxos::{
    util::{FlexibleQuorum, LogEntry, NodeId},
    ClusterConfig,
};
use serial_test::serial;
use utils::{verification::verify_log, TestConfig, TestSystem, Value};

/// Verifies that the decided StopSign is correct and error is returned when trying to append after decided StopSign.
#[test]
#[serial]
fn reconfig_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    let initial_proposals = utils::create_proposals(1, cfg.num_proposals / 2);
    let last_proposals = utils::create_proposals(cfg.num_proposals / 2 + 1, cfg.num_proposals);
    let sentinel_value = Value::with_id(42);
    let mut expected_log = utils::create_proposals(1, cfg.num_proposals);
    expected_log.insert(cfg.num_proposals as usize / 2, sentinel_value.clone());
    sys.start_all_nodes();
    sys.make_proposals(1, initial_proposals, cfg.wait_timeout);

    let new_config_id = 2;
    let new_write_quorum_size = (cfg.num_nodes / 2 + 1) - 1;
    let new_config = ClusterConfig {
        configuration_id: new_config_id,
        nodes: (1..=cfg.num_nodes as NodeId).collect(),
        flexible_quorum: Some(FlexibleQuorum {
            read_quorum_size: (cfg.num_nodes / 2 + 1) + 1,
            write_quorum_size: new_write_quorum_size,
        }),
    };

    let first_node = sys.nodes.get(&1).unwrap();
    let reconfig_f = first_node.on_definition(|x| {
        let (kprom, kfuture) = promise::<()>();
        x.paxos
            .reconfigure_joint_consensus(new_config.clone())
            .expect("Failed to reconfigure");
        x.paxos
            .append(sentinel_value.clone())
            .expect("Failed to append");
        x.insert_decided_future(Ask::new(kprom, sentinel_value));
        kfuture
    });

    reconfig_f
        .wait_timeout(cfg.wait_timeout)
        .expect("Failed to collect reconfiguration future");

    // Kill maximum number of followers such that leader can still function
    let leader_id = sys.get_elected_leader(1, cfg.wait_timeout);
    let maximum_tolerable_follower_failures = (1..=cfg.num_nodes as NodeId)
        .filter(|id| *id != leader_id)
        .take(cfg.num_nodes - new_write_quorum_size);
    for node_id in maximum_tolerable_follower_failures {
        sys.kill_node(node_id);
    }

    // Make some more propsals
    sys.make_proposals(leader_id, last_proposals, cfg.wait_timeout);

    // Verify log
    let leader = sys.nodes.get(&leader_id).unwrap();
    let leaders_log = leader.on_definition(|x| x.read_decided_log());
    verify_log(leaders_log, expected_log);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
