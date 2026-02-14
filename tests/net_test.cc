#include <gtest/gtest.h>

#include <sys/socket.h>

#include <atomic>
#include <memory>
#include <thread>

#include "client/client.h"
#include "net/frame.h"
#include "net/messages.h"
#include "net/socket.h"
#include "net/tcp_server.h"
#include "server/kv_service.h"
#include "storage/db.h"
#include "testutil.h"

namespace flotilla {
namespace {

TEST(Messages, RequestRoundtrip) {
  net::Request req;
  req.type = net::MsgType::kScan;
  req.key = "start";
  req.end_key = "stop";
  req.limit = 42;
  net::Request got;
  ASSERT_TRUE(DecodeRequest(net::EncodeRequest(req), &got));
  EXPECT_EQ(got.type, net::MsgType::kScan);
  EXPECT_EQ(got.key, "start");
  EXPECT_EQ(got.end_key, "stop");
  EXPECT_EQ(got.limit, 42u);
}

TEST(Messages, ResponseRoundtrip) {
  net::Response resp;
  resp.code = static_cast<uint8_t>(Status::Code::kNotLeader);
  resp.leader_addr = "10.0.0.1:4001";
  resp.message = "try elsewhere";
  resp.kvs = {{"a", "1"}, {"b", "2"}};
  net::Response got;
  ASSERT_TRUE(DecodeResponse(net::EncodeResponse(resp), &got));
  EXPECT_EQ(got.code, resp.code);
  EXPECT_EQ(got.leader_addr, resp.leader_addr);
  EXPECT_EQ(got.kvs, resp.kvs);
  EXPECT_TRUE(got.ToStatus().IsNotLeader());
}

TEST(Messages, RejectsGarbage) {
  net::Request req;
  EXPECT_FALSE(DecodeRequest("", &req));
  EXPECT_FALSE(DecodeRequest("\xFFgarbage", &req));
  net::Response resp;
  EXPECT_FALSE(DecodeResponse("\x01junk", &resp));
}

TEST(Frame, RoundtripOverSocket) {
  int listen_fd;
  uint16_t port;
  ASSERT_TRUE(net::Listen("127.0.0.1", 0, &listen_fd, &port).ok());

  std::thread server([&] {
    int fd = ::accept(listen_fd, nullptr, nullptr);
    ASSERT_GE(fd, 0);
    std::string payload;
    ASSERT_TRUE(net::ReadFrame(fd, &payload).ok());
    EXPECT_EQ(payload, std::string("ping with\0binary", 16));
    ASSERT_TRUE(net::WriteFrame(fd, "pong").ok());
    net::CloseSocket(fd);
  });

  int fd;
  ASSERT_TRUE(net::Connect("127.0.0.1", port, &fd).ok());
  ASSERT_TRUE(net::WriteFrame(fd, std::string_view("ping with\0binary", 16)).ok());
  std::string reply;
  ASSERT_TRUE(net::ReadFrame(fd, &reply).ok());
  EXPECT_EQ(reply, "pong");
  net::CloseSocket(fd);
  server.join();
  net::CloseSocket(listen_fd);
}

class ServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::make_unique<test::TempDir>("net_server");
    storage::Options opts;
    opts.fsync_writes = false;
    ASSERT_TRUE(storage::DB::Open(opts, dir_->path(), &db_).ok());
    service_ = std::make_unique<server::LocalKvService>(db_.get(), "test:0");
    ASSERT_TRUE(server_
                    .Start("127.0.0.1", 0,
                           [this](std::string_view req, std::string* resp) {
                             return service_->HandleFrame(req, resp);
                           })
                    .ok());
    addr_ = "127.0.0.1:" + std::to_string(server_.port());
  }

  void TearDown() override { server_.Stop(); }

  std::unique_ptr<test::TempDir> dir_;
  std::unique_ptr<storage::DB> db_;
  std::unique_ptr<server::LocalKvService> service_;
  net::TcpServer server_;
  std::string addr_;
};

TEST_F(ServerTest, EndToEndOps) {
  client::Client c({addr_});
  ASSERT_TRUE(c.Put("k1", "v1").ok());
  ASSERT_TRUE(c.Put("k2", "with spaces and \0 binary").ok());
  std::string v;
  ASSERT_TRUE(c.Get("k1", &v).ok());
  EXPECT_EQ(v, "v1");
  EXPECT_TRUE(c.Get("missing", &v).IsNotFound());
  ASSERT_TRUE(c.Delete("k1").ok());
  EXPECT_TRUE(c.Get("k1", &v).IsNotFound());

  for (int i = 0; i < 30; i++) {
    ASSERT_TRUE(c.Put("scan" + std::to_string(i / 10) + std::to_string(i % 10), "x").ok());
  }
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("scan10", "scan20", 0, &rows).ok());
  EXPECT_EQ(rows.size(), 10u);
  EXPECT_EQ(rows.front().first, "scan10");
  EXPECT_EQ(rows.back().first, "scan19");

  rows.clear();
  ASSERT_TRUE(c.Scan("", "", 5, &rows).ok());
  EXPECT_EQ(rows.size(), 5u);

  std::vector<std::pair<std::string, std::string>> fields;
  ASSERT_TRUE(c.GetStatus(&fields).ok());
  bool has_mode = false;
  for (const auto& [k, val] : fields) {
    if (k == "mode") {
      has_mode = true;
      EXPECT_EQ(val, "single-node");
    }
  }
  EXPECT_TRUE(has_mode);
}

TEST_F(ServerTest, ManyConcurrentClients) {
  constexpr int kClients = 8;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kClients; t++) {
    threads.emplace_back([&, t] {
      client::Client c({addr_});
      for (int i = 0; i < 50; i++) {
        std::string key = "c" + std::to_string(t) + "-" + std::to_string(i);
        if (!c.Put(key, "v").ok()) failures.fetch_add(1);
        std::string v;
        if (!c.Get(key, &v).ok()) failures.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(failures.load(), 0);

  client::Client c({addr_});
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("", "", 1000, &rows).ok());
  EXPECT_EQ(rows.size(), static_cast<size_t>(kClients) * 50);
}

TEST_F(ServerTest, MalformedFrameClosesConnectionOnly) {
  int fd;
  ASSERT_TRUE(net::Connect(addr_, &fd).ok());
  ASSERT_TRUE(net::WriteFrame(fd, "\x63not a valid request").ok());
  std::string reply;
  EXPECT_FALSE(net::ReadFrame(fd, &reply).ok());  // server closed on us
  net::CloseSocket(fd);

  // Server still healthy for other clients.
  client::Client c({addr_});
  ASSERT_TRUE(c.Put("still", "alive").ok());
}

}  // namespace
}  // namespace flotilla
