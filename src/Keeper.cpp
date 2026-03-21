#include "Keeper.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>

char *getIP();

std::string trim(const std::string &s) {
  std::string res = s;
  if (!res.empty()) {
    res.erase(0, res.find_first_not_of(" "));
    res.erase(res.find_last_not_of(" ") + 1);
  }
  return res;
}

const char *Keeper::SERVER_NUM_KEY = "serverNum";

Keeper::Keeper(uint32_t maxServer)
    : maxServer(maxServer), curServer(0), memc(NULL) {}

Keeper::~Keeper() {
  //   listener.detach();

  disconnectMemcached();
}

bool Keeper::connectMemcached() {
  memcached_server_st *servers = NULL;
  memcached_return rc;

  std::ifstream conf("../memcached.conf");

  if (!conf) {
    fprintf(stderr, "can't open memcached.conf\n");
    return false;
  }

  std::string addr, port;
  std::getline(conf, addr);
  std::getline(conf, port);

  memc = memcached_create(NULL);
  servers = memcached_server_list_append(servers, trim(addr).c_str(),
                                         std::stoi(trim(port)), &rc);
  rc = memcached_server_push(memc, servers);

  if (rc != MEMCACHED_SUCCESS) {
    fprintf(stderr, "Counld't add server:%s\n", memcached_strerror(memc, rc));
    sleep(1);
    return false;
  }

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
  return true;
}

bool Keeper::disconnectMemcached() {
  if (memc) {
    memcached_quit(memc);
    memcached_free(memc);
    memc = NULL;
  }
  return true;
}

void Keeper::serverEnter() {
  memcached_return rc;
  uint64_t serverNum;

  while (true) {
    rc = memcached_increment(memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY), 1,
                             &serverNum);
    if (rc == MEMCACHED_SUCCESS) {

      myNodeID = serverNum - 1;

      printf("I am servers %d [%s]\n", myNodeID, getIP());
      return;
    }
    fprintf(stderr, "Server %d Counld't incr value and get ID: %s, retry...\n",
            myNodeID, memcached_strerror(memc, rc));
    usleep(10000);
  }
}

/* March 21, 2026
Phase A experiments fail because memory nodes (node-1, node-2, node-3) cannot register themselves in memcached. Every node
runs the same newbench binary; role (compute vs memory) is determined by which node wins the memcached serverNum INCR race.
The failure is not a network issue — binary-protocol INCR from node-1 works fine in isolation. The problem only appears when
node-0's newbench is running concurrently. After serverEnter() succeeds (node-0 claims nodeID 0), Keeper::serverConnect() spins in a tight infinite polling
loop with no sleep, hammering memcached with memcached_get("serverNum") at potentially millions of QPS. Under this load, new
TCP connections from node-1/2/3 (needed for their memcached_increment("serverNum")) exceed libmemcached's default connect
timeout (~4 s), returning MEMCACHED_TIMEOUT. After ~24 retries × ~5 s = ~120 s, the orchestrator kills the memory nodes when
node-0's process exits.

Evidence:
- Reproducer: start node-0's newbench, then start node-1's newbench concurrently → node-1 gets MEMCACHED_TIMEOUT on every
INCR
- Running node-1's newbench alone (or testing binary-protocol INCR via Python) succeeds immediately
- Memory node logs show 24 repeating Server 0 Counld't incr value and get ID: A TIMEOUT OCCURRED, retry... lines before the
process is killed
*/
void Keeper::serverConnect() {
  size_t l;
  uint32_t flags;
  memcached_return rc;
  // curServer = 0, maxServer = #servers in this cluster
  while (curServer < maxServer) {
    char *serverNumStr = memcached_get(memc, SERVER_NUM_KEY,
                                       strlen(SERVER_NUM_KEY), &l, &flags, &rc);
    if (rc != MEMCACHED_SUCCESS) {
      fprintf(stderr, "Server %d Counld't get serverNum: %s, retry\n", myNodeID,
              memcached_strerror(memc, rc));
      usleep(1000);   // ADDED (1 ms throttle)
      continue;
    }
    uint32_t serverNum = atoi(serverNumStr);
    free(serverNumStr);

    // connect server K
    for (size_t k = curServer; k < serverNum; ++k) {
      if (k != myNodeID) {
        connectNode(k);
        printf("I connect server %zu\n", k);
      }
    }
    curServer = serverNum;
    usleep(1000);   // ADDED (1 ms throttle on success too)
  }
}

void Keeper::memSet(const char *key, uint32_t klen, const char *val,
                    uint32_t vlen) {

  memcached_return rc;
  while (true) {
    rc = memcached_set(memc, key, klen, val, vlen, (time_t)0, (uint32_t)0);
    if (rc == MEMCACHED_SUCCESS) {
      break;
    }
    usleep(400);
  }
}

char *Keeper::memGet(const char *key, uint32_t klen, size_t *v_size,
                     bool timeout) {

  size_t l;
  char *res;
  uint32_t flags;
  memcached_return rc;
  auto start = std::chrono::high_resolution_clock::now(); // get start time

  while (true) {
    res = memcached_get(memc, key, klen, &l, &flags, &rc);
    if (rc == MEMCACHED_SUCCESS) {
      break;
    }
    usleep(400 + 400 * myNodeID); // Changed March 21, 2026 to ensure sleep per retry
    auto end = std::chrono::high_resolution_clock::now(); // get end time
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
        end - start); // calculate duration in microseconds
    if (timeout && (duration.count() >= 10)) {
      // std::cout << "Time out for memGet" << std::endl;
      res = nullptr;
      break;
    }
  }

  if (v_size != nullptr) {
    *v_size = l;
  }

  return res;
}

uint64_t Keeper::memFetchAndAdd(const char *key, uint32_t klen) {
  uint64_t res;
  while (true) {
    memcached_return rc = memcached_increment(memc, key, klen, 1, &res);
    if (rc == MEMCACHED_SUCCESS) {
      return res;
    }
    usleep(10000);
  }
}
