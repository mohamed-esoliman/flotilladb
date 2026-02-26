#include <gtest/gtest.h>

#include "raft_sim.h"

namespace flotilla::raft {
namespace {

using sim::SimCluster;

TEST(RaftSim, ElectsSingleLeader) {
  SimCluster cluster(3, 1);
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);
  int leaders = 0;
  for (int id = 1; id <= 3; id++) {
    if (cluster.node(id).raft->role() == Role::kLeader) leaders++;
  }
  EXPECT_EQ(leaders, 1);
}

TEST(RaftSim, SingleNodeClusterCommitsAlone) {
  SimCluster cluster(1, 7);
  ASSERT_NE(cluster.WaitForLeader(), 0);
  ASSERT_TRUE(cluster.Propose("cmd1"));
  cluster.TickMany(5);
  EXPECT_EQ(cluster.node(1).applied, std::vector<std::string>{"cmd1"});
}

TEST(RaftSim, ReplicatesToAllNodes) {
  SimCluster cluster(3, 2);
  ASSERT_NE(cluster.WaitForLeader(), 0);
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(cluster.Propose("cmd" + std::to_string(i)));
    cluster.TickMany(3);
  }
  cluster.TickMany(30);
  EXPECT_TRUE(cluster.Converged());
  EXPECT_EQ(cluster.committed_count(), 10u);
  EXPECT_EQ(cluster.node(1).applied.size(), 10u);
  EXPECT_EQ(cluster.node(3).applied[4], "cmd4");
}

TEST(RaftSim, LeaderFailoverLosesNoCommittedWrites) {
  SimCluster cluster(3, 3);
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);
  for (int i = 0; i < 5; i++) {
    ASSERT_TRUE(cluster.Propose("before" + std::to_string(i)));
    cluster.TickMany(3);
  }
  cluster.TickMany(20);
  ASSERT_EQ(cluster.committed_count(), 5u);

  cluster.Kill(leader);
  int new_leader = 0;
  for (int i = 0; i < 300 && new_leader == 0; i++) {
    cluster.Tick();
    int l = cluster.LeaderId();
    if (l != 0 && l != leader) new_leader = l;
  }
  ASSERT_NE(new_leader, 0);

  for (int i = 0; i < 5; i++) {
    ASSERT_TRUE(cluster.ProposeOn(new_leader, "after" + std::to_string(i)));
    cluster.TickMany(3);
  }
  cluster.TickMany(30);
  EXPECT_EQ(cluster.committed_count(), 10u);

  // Old leader comes back, catches up, applies everything in order.
  cluster.Restart(leader);
  cluster.TickMany(100);
  EXPECT_TRUE(cluster.Converged());
  const auto& applied = cluster.node(leader).applied;
  ASSERT_EQ(applied.size(), 10u);
  EXPECT_EQ(applied[0], "before0");
  EXPECT_EQ(applied[9], "after4");
}

TEST(RaftSim, MinorityPartitionCannotCommit) {
  SimCluster cluster(5, 4);
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);
  ASSERT_TRUE(cluster.Propose("committed"));
  cluster.TickMany(30);
  ASSERT_EQ(cluster.committed_count(), 1u);

  // Cut the leader plus one follower off from the other three.
  int buddy = leader == 1 ? 2 : 1;
  std::set<int> minority = {leader, buddy};
  std::set<int> majority;
  for (int id = 1; id <= 5; id++) {
    if (!minority.count(id)) majority.insert(id);
  }
  cluster.Partition(minority, majority);

  ASSERT_TRUE(cluster.ProposeOn(leader, "lost"));
  cluster.TickMany(100);
  EXPECT_EQ(cluster.CountApplied("lost"), 0);

  // Majority side elects its own leader and commits.
  int new_leader = cluster.LeaderId();
  ASSERT_TRUE(majority.count(new_leader)) << "leader " << new_leader;
  ASSERT_TRUE(cluster.ProposeOn(new_leader, "survives"));
  cluster.TickMany(50);
  EXPECT_EQ(cluster.CountApplied("survives"), 3);

  // Heal: the minority's uncommitted entry is overwritten, everyone converges.
  cluster.Heal();
  cluster.TickMany(150);
  EXPECT_TRUE(cluster.Converged());
  EXPECT_EQ(cluster.CountApplied("lost"), 0);
  EXPECT_EQ(cluster.CountApplied("survives"), 5);
}

TEST(RaftSim, ProgressUnderMessageLossAndDelay) {
  SimCluster cluster(3, 5);
  cluster.SetDropRate(0.2);
  cluster.SetMaxDelay(5);
  ASSERT_NE(cluster.WaitForLeader(1000), 0);
  int proposed = 0;
  for (int round = 0; round < 30; round++) {
    if (cluster.Propose("cmd" + std::to_string(round))) proposed++;
    cluster.TickMany(20);
  }
  EXPECT_GT(proposed, 10);
  cluster.SetDropRate(0.0);
  cluster.TickMany(200);
  EXPECT_TRUE(cluster.Converged());
  EXPECT_GT(cluster.committed_count(), 0u);
}

TEST(RaftSim, RepeatedRestartsConverge) {
  SimCluster cluster(3, 6);
  ASSERT_NE(cluster.WaitForLeader(), 0);
  int cmd = 0;
  for (int round = 0; round < 10; round++) {
    for (int i = 0; i < 3; i++) {
      if (cluster.Propose("c" + std::to_string(cmd))) cmd++;
      cluster.TickMany(5);
    }
    int victim = (round % 3) + 1;
    cluster.Kill(victim);
    cluster.TickMany(40);
    if (cluster.LeaderId() != 0) {
      if (cluster.Propose("c" + std::to_string(cmd))) cmd++;
    }
    cluster.Restart(victim);
    cluster.TickMany(40);
  }
  cluster.TickMany(200);
  EXPECT_TRUE(cluster.Converged());
  EXPECT_GT(cluster.committed_count(), 15u);
}

TEST(RaftSim, ReadIndexConfirmsWithQuorumOnly) {
  SimCluster cluster(3, 8);
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);
  cluster.TickMany(20);  // let the term no-op commit

  ASSERT_TRUE(cluster.StartReadIndexOn(leader, 42));
  cluster.TickMany(20);
  ASSERT_EQ(cluster.node(leader).confirmed_reads.size(), 1u);
  EXPECT_EQ(cluster.node(leader).confirmed_reads[0].first, 42u);

  // Followers refuse.
  int follower = leader == 1 ? 2 : 1;
  EXPECT_FALSE(cluster.StartReadIndexOn(follower, 43));

  // An isolated leader must never confirm a read.
  std::set<int> rest;
  for (int id = 1; id <= 3; id++) {
    if (id != leader) rest.insert(id);
  }
  cluster.Partition({leader}, rest);
  size_t before = cluster.node(leader).confirmed_reads.size();
  bool started = cluster.StartReadIndexOn(leader, 44);
  if (started) {
    cluster.TickMany(200);
    EXPECT_EQ(cluster.node(leader).confirmed_reads.size(), before);
  }
}

TEST(RaftSim, SnapshotCatchesUpLaggingFollower) {
  SimCluster cluster(3, 9);
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);

  int laggard = leader == 1 ? 2 : 1;
  cluster.Kill(laggard);
  for (int i = 0; i < 20; i++) {
    ASSERT_TRUE(cluster.Propose("cmd" + std::to_string(i)));
    cluster.TickMany(3);
  }
  cluster.TickMany(30);

  // Leader compacts its log below what the laggard has.
  auto& leader_node = cluster.node(leader);
  ASSERT_GT(leader_node.applied_index, 10u);
  const_cast<Raft*>(leader_node.raft.get())
      ->CompactTo(leader_node.applied_index,
                  SimCluster::EncodeStateMachine(leader_node.applied));

  cluster.Restart(laggard);
  cluster.TickMany(150);
  EXPECT_TRUE(cluster.Converged());
  EXPECT_EQ(cluster.node(laggard).applied.size(), 20u);
  EXPECT_EQ(cluster.node(laggard).applied[19], "cmd19");
}

TEST(RaftSim, RandomizedChaosSoak) {
  for (uint64_t seed = 1; seed <= 15; seed++) {
    SimCluster cluster(3, seed * 101);
    cluster.SetDropRate(0.1);
    cluster.SetMaxDelay(4);
    std::mt19937_64 rng(seed);
    int cmd = 0;
    std::vector<bool> down(4, false);

    for (int step = 0; step < 400; step++) {
      cluster.Tick();
      if (step % 5 == 0 && cluster.Propose("s" + std::to_string(cmd))) cmd++;
      if (step % 37 == 0) {
        int victim = 1 + static_cast<int>(rng() % 3);
        int downs = static_cast<int>(std::count(down.begin(), down.end(), true));
        if (!down[static_cast<size_t>(victim)] && downs < 1) {
          cluster.Kill(victim);
          down[static_cast<size_t>(victim)] = true;
        } else if (down[static_cast<size_t>(victim)]) {
          cluster.Restart(victim);
          down[static_cast<size_t>(victim)] = false;
        }
      }
    }
    for (int id = 1; id <= 3; id++) {
      if (down[static_cast<size_t>(id)]) cluster.Restart(id);
    }
    cluster.SetDropRate(0.0);
    cluster.TickMany(400);
    EXPECT_TRUE(cluster.Converged()) << "seed " << seed;
    EXPECT_GT(cluster.committed_count(), 5u) << "seed " << seed;
  }
}

}  // namespace
}  // namespace flotilla::raft
