#include "storage/rafdb/sync.h"



namespace rafdb{


Sync::Sync(RafDb *rafdb)
{
    //cache_dir="cache_local_dir";
    if (NULL == rafdb)
        VLOG(3)<<"*rafdb==NULL";
    rafdb_=rafdb;
    node_status_map.clear();
    sync_pool= new base::ThreadPool(sync_thread_num);
    for_fail_pool=new base::ThreadPool(for_fail_thread_num);
    sync_pool->StartWorkers();
    for_fail_pool->StartWorkers();
    comsume_fail_queue_thread();
    if (NULL==sync_pool || NULL==for_fail_pool)
        VLOG(3)<<"sync_pool or for_fail_pool ==NULL";
    if (rafdb_->NodeList.empty())
        VLOG(3)<<"NodeList is Empty";
    for(std::vector<NodeInfo>::iterator iter=rafdb_->NodeList.begin(); iter != rafdb_->NodeList.end();iter++)
    {
        std::string ip_port=toIpPort(*iter);
        node_status_map[ip_port]=alive;
        rafdb::RafdbSync *dbSync=new RafdbSync(*iter);
        node_set_map[ip_port]=dbSync;
        fail_time_map[ip_port]=0;
        VLOG(3)<<"put in map : "<<ip_port;
        
    }
    load_cache();
}
template <typename TYPE, void (TYPE::*fail_queue_consumer)() > void* _thread_t(void* param)
{     
 TYPE* This = (TYPE*)param;     
 This->fail_queue_consumer();     
 return NULL;  
 } 
void Sync::comsume_fail_queue_thread()
{
    pthread_t tid;
    pthread_create(&tid,NULL,_thread_t<Sync,&Sync::fail_queue_consumer>,this);
}
void Sync::load_cache()
{
    int32_t itId=rafdb_->OpenIterator(cache_dir);
    std::vector<Pair> pairs;
    rafdb_->NextPairs(pairs,itId,1);
    while(pairs.size()>0) {
        //hotel_to_base_map[pairs[0].key] = pairs[0].value;
        std::string new_key(pairs[0].key);
        //std::string value(pairs[0].value);
        //size_t end_key_pos = new_key.find_first_of("|");
        //VLOG(6)<<new_key;
        //if (end_key_pos == std::string::npos)
        //{   
        //    VLOG(3)<<"can't parser the key : "<<new_key;
        //    
        //    rafdb_->NextPairs(pairs,itId,1);
        //    continue;
        //}
        //std::string ip_port(new_key, 0, end_key_pos);        
        //std::string dbname_key(new_key, end_key_pos+1, new_key.size() - end_key_pos);
        //
        //
        //end_key_pos = dbname_key.find_first_of("|");
        //if (end_key_pos == std::string::npos)
        //{   
        //    VLOG(3)<<"can't parser the key : "<<dbname_key;
        //    
        //    rafdb_->NextPairs(pairs,itId,1);
        //    continue;
        //}
        //std::string dbname(dbname_key,0,end_key_pos);
        //std::string key(dbname_key,end_key_pos+1,dbname_key.size() - end_key_pos);
        //
        //LKV_SYNC *lkv=create_lkv(ip_port,dbname,key,value);
        //for_fail_pool->Add(base::NewOneTimeCallback(this,&Sync::for_fail_process,lkv));
        fail_queue.Push(new_key);
        rafdb_->NextPairs(pairs,itId,1);
    }    
    rafdb_->CloseIterator(itId);
    VLOG(3)<<"load cache success ";
    
    
}
LKV_SYNC *Sync::create_lkv(const std::string &ip_port,const std::string &dbname,const std::string &key,const std::string &value)
{
    size_t end_key_pos = ip_port.find_first_of(":");
    if (end_key_pos == std::string::npos)
    {   
        VLOG(3)<<"can't parser the key : "<<ip_port;
    }
    std::string ip(ip_port, 0, end_key_pos);
    std::string port(ip_port, end_key_pos+1, ip_port.size() - end_key_pos);
    LKV_SYNC *lkv=new LKV_SYNC(dbname,key,value,ip,atoi(port.c_str()));    
    return lkv;
    
}
Sync::~Sync()
{
    if (NULL!=sync_pool)
    {    
        delete sync_pool;
        sync_pool=NULL;
    }
    if (NULL!=for_fail_pool)
    {
        delete for_fail_pool;
        for_fail_pool=NULL;
    }
    for (std::map< std::string,RafdbSync * >::iterator iter=node_set_map.begin();iter!=node_set_map.end();iter++)
    {
        if (NULL!=iter->second)
        {    
            delete iter->second;
            node_set_map[iter->first]=NULL;
        }
    }
}
void Sync::fail_queue_consumer()
{
    while(true)
    {
        std::string new_key;
        fail_queue.Pop(new_key);
        std::vector<Pair> pairs;
        //int32_t itId=rafdb_->OpenIterator(cache_dir);
        size_t end_key_pos = new_key.find_first_of("|");
        VLOG(6)<<new_key;
        if (end_key_pos == std::string::npos)
        {   
            VLOG(3)<<"can't parser the key : "<<new_key;
        }
        std::string ip_port(new_key, 0, end_key_pos);        
        std::string dbname_key(new_key, end_key_pos+1, new_key.size() - end_key_pos);
        
        
        end_key_pos = dbname_key.find_first_of("|");
        if (end_key_pos == std::string::npos)
        {   
            VLOG(3)<<"can't parser the key : "<<dbname_key;
        }
        std::string dbname(dbname_key,0,end_key_pos);
        std::string key(dbname_key,end_key_pos+1,dbname_key.size() - end_key_pos);
        std::string value;
        rafdb_->Get(value,cache_dir,new_key);
        LKV_SYNC *lkv=create_lkv(ip_port,dbname,key,value);
        
        for_fail_pool->Add(base::NewOneTimeCallback(this,&Sync::for_fail_process,lkv));
    }
    
}
void Sync::sync_process(LKV_SYNC *lkv)
{
    std::string ip_port=toIpPort(lkv->node_info);
    if (node_status_map.find(ip_port)!=node_status_map.end())
    {
        if (alive==node_status_map[ip_port])
        {
            if (node_set_map.find(ip_port)!=node_set_map.end())
            {
                if(node_set_map[ip_port]->Set(lkv->dbname,lkv->key,lkv->value))
                {
                    VLOG(6)<<"sync success key : "<<lkv->key;
                    delete lkv;
                    return;
                }
                else
                {    
                    //for_fail_pool->Add(base::NewOneTimeCallback(this,&Sync::for_fail_process,lkv));
                    std::string new_key=toIpPort(lkv->node_info)+"|"+lkv->dbname+"|"+lkv->key;
                    if (!rafdb_->Set(cache_dir,new_key,lkv->value))
                        VLOG(3)<<"set to leveldb failed key : "<<new_key;
                    fail_queue.Push(new_key);
                    VLOG(6)<<"set : "<<ip_port<<"fail,put it in fail queue";
                }
            }
            else
                VLOG(3)<<"the ip_port "<<ip_port<<"in node_status_map but not in node_set_map";
        }
        else
        {
            //for_fail_pool->Add(base::NewOneTimeCallback(this,&Sync::for_fail_process,lkv));
            std::string new_key=toIpPort(lkv->node_info)+"|"+lkv->dbname+"|"+lkv->key;
            if (!rafdb_->Set(cache_dir,new_key,lkv->value))
                VLOG(3)<<"set to leveldb failed key : "<<new_key;
            fail_queue.Push(new_key);            
            VLOG(3)<<"the ip_port : "<<ip_port<<" is not alive put it in fail queue";
        }
    }
    else
    {
        VLOG(3)<<"the ip_port :"<<ip_port<< " is not in node_status_map";
        delete lkv;
    }
}
void Sync::push(LKV_SYNC *lkv)
{
    VLOG(6)<<"push in sync_pool data : "<<lkv->key;
    sync_pool->Add(base::NewOneTimeCallback(this,&Sync::sync_process,lkv));
    VLOG(6)<<"push in sync_pool complete";
}

void Sync::for_fail_process(LKV_SYNC *lkv)
{
    VLOG(6)<<"fail queue get data : "<<lkv->key;
    std::string ip_port=toIpPort(lkv->node_info);
    std::string new_key=ip_port+"|"+lkv->dbname+"|"+lkv->key;
    //VLOG(6)<<new_key;
    if (node_status_map.find(ip_port)!=node_status_map.end())
    {
        if (node_set_map.find(ip_port)!=node_set_map.end())
        {
            //if(!rafdb_->Set(cache_dir,new_key,lkv->value))
            //    VLOG(3)<<"set local leveldb failed key : "<<lkv->key;
            if (node_set_map[ip_port]->Set(lkv->dbname,lkv->key,lkv->value))
            {
                VLOG(6)<<"set to far leveldb success";
                update_node_status(lkv->node_info,alive);
                if(!rafdb_->Delete(cache_dir,new_key))
                    VLOG(3)<<"delete from leveldb failed key : "<<lkv->key;
                else
                    VLOG(6)<<"delete from leveldb success key: "<<lkv->key;
                delete lkv;
            }
            else
            {
                VLOG(6)<<"set to far leveldb fail return to for_fail_pool";
                //for_fail_pool->Add(base::NewOneTimeCallback(this,&Sync::for_fail_process,lkv));
                fail_queue.Push(new_key);                
                if(!get_node_status(lkv->node_info))
                {
                    VLOG(6)<<"sleep 20000";
                    usleep(200000);
                }
                else
                {
                    VLOG(6)<<"update flag";
                    {
                        base::MutexLock lock(&flag_mutex_);
                        if (fail_time_map.find(ip_port)!=fail_time_map.end())
                        {
                            fail_time_map[ip_port]++;
                            if (fail_time_map[ip_port]>1)
                            {
                                update_node_status(lkv->node_info,dead);
                                fail_time_map[ip_port]=0;
                            }
                        }
                        else
                            VLOG(3)<<"the ip_port "<<ip_port<<"is not in fail_time_map ";
                    }
                    
                   
                    
                }
                delete lkv;
            }
        }
        else
            VLOG(3)<<"the ip_port "<<ip_port<<"in node_status_map but not in node_set_map";
    }
    else
        VLOG(3)<<"the ip_port :"<<ip_port<< " is not in node_status_map";
    
}

bool Sync::get_node_status(const NodeInfo &info)
{
    base::MutexLock lock(&mutex_);
    std::string ip_port=toIpPort(info);
    if (node_status_map.find(ip_port)==node_status_map.end())
    {
        VLOG(3)<<"the ip is not in node_status_map : "<<info.ip;
        return false;
    }
    if (dead==node_status_map[ip_port])
        return false;
    else
        return true;
    return false;
 
}
void Sync::update_node_status(const NodeInfo &info,int status)
{
    VLOG(6)<<"come in update_node_status";
    if (status==0||status==1)
    {
        base::MutexLock lock(&mutex_);
        std::string ip_port=toIpPort(info);
        if (node_status_map.find(ip_port)==node_status_map.end())
        {
            VLOG(3)<<"the ip_port is not in map";
            return;
        }
        node_status_map[ip_port]=status;
    }    
    else
        VLOG(3)<<"the status is not support";
    VLOG(6)<<"leave update_node_status";
    
}
std::string Sync::toIpPort(const NodeInfo &info)
{
    std::string ip=info.ip;
    int port=info.port;
    std::string ip_port;
    SStringPrintf(&ip_port,"%s:%d",ip.c_str(),port);
    return ip_port;
	
    
}

}
