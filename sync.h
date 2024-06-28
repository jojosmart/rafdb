#ifndef STORAGE_RAFDB_SYNC_H_
#define STORAGE_RAFDB_SYNC_H_


#include <vector>
#include <map>
#include <string>
#include <stdlib.h>
#include "base/logging.h"
#include "base/callback.h"
#include "base/string_split.h"
#include "base/thread_pool.h"
#include "base/concurrent_queue.h"
#include "storage/rafdb/manager.h"
#include "storage/rafdb/rafdb_sync.h"



namespace {

    const int sync_thread_num=32;
    const int for_fail_thread_num=32;
    const int queue_top_size=100000000;
    const std::string cache_dir="cache_local_dir";
    typedef base::ConcurrentQueue<std::string> Queue;

}

namespace rafdb{
class RafDb;
class Sync
{
    public:
        void push(LKV_SYNC *lkv);
        Sync(RafDb *rafdb);
        ~Sync();
        void comsume_fail_queue_thread();
        void fail_queue_consumer();
    private:
        enum alive_status{alive=0,dead=1};
        void sync_process(LKV_SYNC *lkv);
        void for_fail_process(LKV_SYNC *lkv);        
        bool get_node_status(const NodeInfo &info);
        void update_node_status(const NodeInfo &info,int status);
        void load_cache();
        LKV_SYNC *create_lkv(const std::string &ip_port,const std::string &dbname,const std::string &key,const std::string &value);

        base::Mutex mutex_;
        base::Mutex flag_mutex_;
        //base::hash_map<NodeInfo,int> node_status_map;
        std::map< std::string,int > node_status_map;
        std::map< std::string,RafdbSync *> node_set_map;
        std::map< std::string,int > fail_time_map;
        //std::map< std::>
        std::string toIpPort(const NodeInfo &info);
        
        //static Sync *m_pInstance;
        //std::string cache_dir;
        base::ThreadPool *sync_pool;
        base::ThreadPool *for_fail_pool;
        RafDb *rafdb_;
        Queue fail_queue;
        //DISALLOW_COPY_AND_ASSIGN(Sync);
        
};

 
}

#endif  // D_LEVELDBD_SYNC_H_
