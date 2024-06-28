
#include "storage/rafdb/client/rafdb_client.h"
#include "base/string_util.h"
#include "base/time.h"
#include "ts/base/crawl_config.h"
#include "base/logging.h"
#include <time.h>
DEFINE_int32(loop_count,0,"loop count");
DEFINE_int32(sleep_interval,0,"sleep us");
DEFINE_int32(index_start,0,"sleep us");
DEFINE_bool(is_write,true,"write true,read false");
int main(int argc,char** argv) {
  base::ParseCommandLineFlags(&argc, &argv, true);
  rafdb::RafdbDispatcher * test_ldb = new rafdb::RafdbDispatcher(FLAGS_rafdb_list);
  int i = FLAGS_index_start;
  if (FLAGS_is_write) {//write
    for(;i<FLAGS_loop_count;i++) {
      int64 starttime = base::GetTimeInUsec();
      std::string key;
      std::string val;
      SStringPrintf(&key,"%s:%d","url",i);
      SStringPrintf(&val,"%s:%d","html",i);
      test_ldb->Set(key,val,"test_table");
      int64 endtime = base::GetTimeInUsec();
      int64 spendtime = endtime-starttime;
      VLOG(3)<<"write spend time "<<spendtime;
      usleep(FLAGS_sleep_interval);
    }
  }else {//read
    for(;i<FLAGS_loop_count;i++) {
      int64 starttime = base::GetTimeInUsec();
      std::string key;
      SStringPrintf(&key,"%s:%d","url",i);
      std::string result;
      if (test_ldb->Get(result,key,"test_table")) {
        int64 endtime = base::GetTimeInUsec();
        int64 spendtime = endtime-starttime;
        VLOG(3)<<"read spend time "<<spendtime;
        VLOG(3)<<"[read]key=>"<<key<<"val=>"<<result;
      }
      usleep(FLAGS_sleep_interval);
    }

  }

  return 0;

}
