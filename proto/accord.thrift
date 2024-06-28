namespace cpp rafdb

enum State {
  FOLLOWER = 0,
  CANDIDATE = 1,
  LEADER = 2,
}

enum MessageType {
  UNKNOWN = 0,
  VOTEREQ = 1,
  VOTEREP = 2,
  HEARTREQ = 3,
  HEARTREP = 4,
  LEADERREQ = 5,
  LEADERREP = 6,
  TIMER = 7,
}

struct Message {
  1:  i64 term_id,
  2:  i32 candidate_id,
  3:  i32 server_id,
  4:  i32 leader_id,
  5:  string ip,
  6:  i32 port,
  7:  bool self_healthy,
  8:  bool granted,
  9:  MessageType message_type = MessageType.UNKNOWN,
  10: bool success,
}



