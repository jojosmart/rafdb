
#include "storage/rafdb/rafdb.h"

#include "ts/base/util.h"
#include "base/logging.h"
#include "file/file.h"
#include "third_party/leveldb/write_batch.h"
#include "third_party/leveldb/cache.h"
#include "base/hash.h"
#include "base/string_util.h"
DEFINE_string(db_dir, "/data/leveldb/", "");
DEFINE_int32(leveldb_block_cache, 1073741824, "1G");
DEFINE_int32(check_interval, 3600, "");
DEFINE_int32(write_buffer_size, 4194304, "leveldb buffer");
DEFINE_string(rafdb_self, "", "rafdb self,192.168.11.12:1111:1");

namespace rafdb {

RafDb::RafDb() {
  db_map_.clear();
  it_id_ = 0;
  it_map_.clear();
  options_.create_if_missing = true;
  options_.write_buffer_size = FLAGS_write_buffer_size;
  it_checker_.reset(new IteratorChecker(this));
  it_checker_->Start();
  //image_set_.reset(new ImageSet(this));
  //image_set_->Start();
  //image_get_.reset(new ImageGet(this));
  //image_get_->Start();
  LoadDB();
  leader_id_ = 0;//no leader
  std::vector<std::string> tmp_v;
  SplitString(FLAGS_rafdb_self, ':', &tmp_v);
  ip_ = tmp_v[0];
  StringToInt(tmp_v[1], &port_);
  StringToInt(tmp_v[2], &self_id_);
  std::vector<std::string> tmp_v2;
  if (FLAGS_rafdb_list != "") {
    SplitString(FLAGS_rafdb_list, ',', &tmp_v2);
    for(int i=0;i<tmp_v2.size();i++) {
      std::string ip_tmp;
      int port_tmp;
      int id_tmp;
      crawl::GetIpPortId(ip_tmp,port_tmp,id_tmp,tmp_v2[i]);
      if (id_tmp != self_id_) {
        NodeInfo tmp;
        tmp.ip = ip_tmp;
        tmp.port = port_tmp;
        NodeList.push_back(tmp);
      }
    }
  }
  Init();

}

RafDb::~RafDb() {
  base::hash_map<std::string, leveldb::DB*>::iterator db_it = db_map_.begin();
  for (; db_it != db_map_.end(); ++db_it) {
    delete db_it->second;
  }
  base::hash_map<int32_t, WrapIterator*>::iterator it = it_map_.begin();
  for (; it != it_map_.end(); ++it) {
    delete it->second;
  }
}

void RafDb::Init() {
  accord_.reset(new Accord(this,ip_,port_));
  accord_->Start();//start accord thread
  manager_.reset(new Manager(this));
  manager_->Start();

}

bool RafDb::Set(const std::string &dbname, const std::string &key,
       const std::string &value) {
  //DLOG(INFO) << "Set";
  leveldb::DB *db = NULL;
  {
    base::MutexLock lock(&db_map_mutex_);
    base::hash_map<std::string, leveldb::DB*>::iterator it =
      db_map_.find(dbname);
    if (it == db_map_.end()) {
      db = Open(dbname);
      if (db == NULL) {
        LOG(ERROR) << "Set, dbname:" << dbname << " key:" << key
          << " value:" << value;
        return false;
      }
    } else {
      db = it->second;
    }
  }

  leveldb::Status status = db->Put(woptions_, key, value);
  if (!status.ok()) {
    LOG(ERROR) << "Set, dbname:" << dbname << " key:" << key
      << " value:" << value;
    return false;
  }
  return true;
}

bool RafDb::MPSet(const std::string &dbname,
    const std::vector<rafdb::Pair> &pairs) {
  DLOG(INFO) << "MPSet start";
  leveldb::DB *db = NULL;
  {
    base::MutexLock lock(&db_map_mutex_);
    base::hash_map<std::string, leveldb::DB*>::iterator it =
      db_map_.find(dbname);

    if (it == db_map_.end()) {
      db = Open(dbname);
      if (db == NULL) {
        LOG(ERROR) << "MPSet, db:" << dbname;
        return false;
      }
    } else {
      db = it->second;
    }
  }

  leveldb::WriteBatch wBatch;
  for (int i = 0; i < pairs.size(); i++) {
    wBatch.Put(pairs[i].key, pairs[i].value);
  }
  leveldb::Status status = db->Write(woptions_, &wBatch);
  if (!status.ok()) {
    LOG(ERROR) << "MPSet, db:" << dbname;
    return false;
  }
  DLOG(INFO) << "MPSet end";

  return true;
}

void RafDb::Get(std::string &result, const std::string &dbname,
    const std::string &key) {
  result.clear();
  leveldb::DB *db = NULL;
  {
    base::hash_map<std::string, leveldb::DB*>::iterator it;
    base::MutexLock lock(&db_map_mutex_);
    it = db_map_.find(dbname);
    if (it == db_map_.end()) {
      return;
    }
    db = it->second;
  }

  leveldb::Status status = db->Get(roptions_, key, &result);
  if (status.IsNotFound()) {
    result.clear();
    return;
  }
  if (!status.ok()) {
    LOG(ERROR) << "Get, dbname:" << dbname << " key:" << key;
    result.clear();
    return;
  }
  return;
}

void RafDb::MGet(std::vector<std::string> &results,
  const std::string &dbname, const std::vector<std::string> &keys) {
  results.clear();
  leveldb::DB *db = NULL;
  {
    base::MutexLock lock(&db_map_mutex_);
    base::hash_map<std::string, leveldb::DB*>::iterator it =
      db_map_.find(dbname);
    if (it == db_map_.end()) {
        return;
    }
    db = it->second;
  }

  for (int i = 0; i < keys.size(); i++) {
    std::string result;
    leveldb::Status status = db->Get(roptions_, keys[i], &result);
    if (status.IsNotFound()) {
      results.push_back("");
    } else if (!status.ok()) {
      results.push_back("");
      LOG(ERROR) << "MGet, dbname:" << dbname << " key:" << keys[i];
    } else {
      results.push_back(result);
    }
  }
  return;
}

bool RafDb::Delete(const std::string &dbname, const std::string &key) {
  leveldb::DB *db = NULL;
  {
    base::MutexLock lock(&db_map_mutex_);
    base::hash_map<std::string, leveldb::DB*>::iterator it =
      db_map_.find(dbname);

    if (it == db_map_.end()) {
      return true;
    }
    db = it->second;
  }
  leveldb::Status status = db->Delete(woptions_, key);
  if (!status.ok()) {
    LOG(ERROR) << "Delete, dbname:" << dbname << " key:" << key;
    return false;
  }
  return true;
}

bool RafDb::MDelete(const std::string &dbname,
    const std::vector<std::string> &keys) {
  leveldb::DB *db = NULL;
  {
    base::MutexLock lock(&db_map_mutex_);
    base::hash_map<std::string, leveldb::DB*>::iterator it =
      db_map_.find(dbname);

    if (it == db_map_.end()) {
      return true;
    }
    db = it->second;
  }
  leveldb::WriteBatch wBatch;
  for (int i = 0; i < keys.size(); i++) {
    wBatch.Delete(keys[i]);
  }
  leveldb::Status status = db->Write(woptions_, &wBatch);
  if (!status.ok()) {
    LOG(ERROR) << "MDelete, db:" << dbname;
    return false;
  }
  return true;
}

int32_t RafDb::OpenIterator(const std::string &dbname) {
  leveldb::DB *db = NULL;
  {
    base::MutexLock lock(&db_map_mutex_);
    base::hash_map<std::string, leveldb::DB*>::iterator dbIt =
      db_map_.find(dbname);

    if (dbIt == db_map_.end()) {
      db = Open(dbname);
      if (db == NULL) {
        LOG(ERROR) << "OpenIterator, db:" << dbname;
        return -1;
      }
    } else {
      db = dbIt->second;
    }
  }

  base::MutexLock it_lock(&it_map_mutex_);
  it_id_++;
  leveldb::Iterator *it = db->NewIterator(roptions_);
  it->SeekToFirst();
  WrapIterator *wrap_it = new WrapIterator();
  wrap_it->iterator_ = it;
  wrap_it->last_operation_ = time(NULL);
  wrap_it->dbname_ = dbname;
  it_map_[it_id_] = wrap_it;
  return it_id_;
}

bool RafDb::CloseIterator(int32_t itID) {
  base::MutexLock lock(&it_map_mutex_);
  base::hash_map<int32_t, WrapIterator*>::iterator it =
    it_map_.find(itID);

  if (it == it_map_.end()) {
    LOG(WARNING) << "CloseIterator, itID not exist:" << itID;
    return true;
  }
  WrapIterator *pIt = it->second;
  delete pIt;
  pIt = NULL;
  it_map_.erase(it);
  return true;
}

bool RafDb::SeekToFirst(int32_t itID) {
  WrapIterator *pIt = NULL;
  {
    base::hash_map<int, WrapIterator*>::iterator it;
    base::MutexLock lock(&it_map_mutex_);
    it = it_map_.find(itID);
    if (it == it_map_.end()) {
      LOG(WARNING) << "SeekToFirst, itID not exist:" << itID;
      return false;
    }
    pIt = it->second;
  }
  pIt->iterator_->SeekToFirst();
  pIt->last_operation_ = time(NULL);
  return true;
}

bool RafDb::SeekToLast(int32_t itID) {
  WrapIterator *pIt = NULL;
  {
    base::hash_map<int32_t, WrapIterator*>::iterator it;
    base::MutexLock lock(&it_map_mutex_);
    it = it_map_.find(itID);

    if (it == it_map_.end()) {
      LOG(WARNING) << "SeekToLast, itID not exist:" << itID;
      return false;
    }
    pIt = it->second;
  }
  pIt->iterator_->SeekToLast();
  pIt->last_operation_ = time(NULL);
  return true;
}

bool RafDb::Valid(int32_t itID) {
  WrapIterator *pIt = NULL;
  {
    base::hash_map<int32_t, WrapIterator*>::iterator it;
    base::MutexLock lock(&it_map_mutex_);
    it = it_map_.find(itID);
    if (it == it_map_.end()) {
      LOG(WARNING) << "Valid, itID not exist:" << itID;
      return false;
    }
    pIt = it->second;
  }

  pIt->last_operation_ = time(NULL);
  return pIt->iterator_->Valid();
}

bool RafDb::Seek(int32_t itID, const std::string &target) {
  WrapIterator *pIt = NULL;
  {
    base::hash_map<int32_t, WrapIterator*>::iterator it;
    base::MutexLock lock(&it_map_mutex_);
    it = it_map_.find(itID);
    if (it == it_map_.end()) {
      LOG(WARNING) << "Seek, itID not exist:" << itID;
      return false;
    }
    pIt = it->second;
  }
  pIt->iterator_->Seek(target);
  pIt->last_operation_ = time(NULL);
  return true;
}

void RafDb::NextKeys(std::vector<std::string> &keys, int32_t itID,
    int32_t number) {
  keys.clear();
  WrapIterator *pIt = NULL;
  {
    base::hash_map<int32_t, WrapIterator*>::iterator it;
    base::MutexLock lock(&it_map_mutex_);
    it = it_map_.find(itID);
    if (it == it_map_.end()) {
      LOG(WARNING) << "NextKeys, itID not exist:" << itID;
      return;
    }
    pIt = it->second;
  }

  int32_t n = number;
  while (n-- && pIt->iterator_->Valid()) {
    keys.push_back(pIt->iterator_->key().ToString());
    pIt->iterator_->Next();
  }
  pIt->last_operation_ = time(NULL);
  return;
}

void RafDb::NextValues(std::vector<std::string> &values, int32_t itID,
    int32_t number) {
  values.clear();
  WrapIterator *pIt = NULL;
  {
    base::hash_map<int32_t, WrapIterator*>::iterator it;
    base::MutexLock lock(&it_map_mutex_);
    it = it_map_.find(itID);
    if (it == it_map_.end()) {
      LOG(WARNING) << "NextValues, itID not exist:" << itID;
      return;
    }
    pIt = it->second;
  }

  int32_t n = number;
  while (n-- && pIt->iterator_->Valid()) {
    values.push_back(pIt->iterator_->value().ToString());
    pIt->iterator_->Next();
  }
  pIt->last_operation_ = time(NULL);
  return;
}

void RafDb::NextPairs(std::vector<rafdb::Pair> &pairs,
    int32_t itID, int32_t number) {
  pairs.clear();
  WrapIterator *pIt = NULL;
  {
    base::hash_map<int32_t, WrapIterator*>::iterator it;
    base::MutexLock lock(&it_map_mutex_);
    it = it_map_.find(itID);
    if (it == it_map_.end()) {
      LOG(WARNING) << "NextPairs, itID not exist:" << itID;
      return;
    }
    pIt = it->second;
  }

  int32_t n = number;
  while (n-- && pIt->iterator_->Valid()) {
    rafdb::Pair pair;
    pair.key = pIt->iterator_->key().ToString();
    pair.value = pIt->iterator_->value().ToString();
    pairs.push_back(pair);
    pIt->iterator_->Next();
  }
  pIt->last_operation_ = time(NULL);
  return;
}

bool RafDb::DeleteDatabase(const std::string& dbname) {
  DLOG(INFO) << "DeleteDatabase";
  {
    base::MutexLock lock(&it_map_mutex_);
    base::hash_map<int32_t, RafDb::WrapIterator*>::iterator it =
      it_map_.begin();
    for (; it != it_map_.end(); ) {
      if (it->second->dbname_ == dbname) {
        LOG(WARNING) << "DeleteDatabase: iterator " << it->first
          << " forget close!";
        delete it->second;
        it->second = NULL;
        it_map_.erase(it++);
      } else {
        it++;
      }
    }
  }
  leveldb::DB *db = NULL;
  {
    base::MutexLock lock(&db_map_mutex_);
    base::hash_map<std::string, leveldb::DB*>::iterator it =
      db_map_.find(dbname);
    if (it == db_map_.end()) {
      db = NULL;
    } else {
      db = it->second;
      db_map_.erase(it);
    }
  }

  delete db;
  file::File::DeleteRecursively(FLAGS_db_dir + dbname);
  return true;
}



//void RafDb::_GetRoomImageList(std::vector<std::string> & image_list, const std::string& base_hotel_id,
//    const std::string& ota_id, const std::string& room_hash) {
//  
//    int64 starttime = base::GetTimeInUsec();
//    //usleep(1000);
//    //LOG(INFO)<<"after sleep spend "<<base::GetTimeInUsec()-starttime;
//    //base::MutexLock lock(&get_room_image_mutex_);
//    std::string hotel_id;
//    std::string base_hotel_map_key = base_hotel_id + "_"+ota_id;
//    Get(hotel_id,"base_hotel_map_table", base_hotel_map_key);
//    if (hotel_id == "") {
//      LOG(INFO)<<"hotel_id is null";
//      return;
//    }
//
//    std::string room_image_list;
//    std::string room_ori_imagelist_key = hotel_id + "_"+ota_id+"_"+room_hash;
//    //get room ori imagelist by room_ori_imagelist_key
//    //for example ["123123123","3123123123"]
//    Get(room_image_list,"room_ori_imagelist_table", room_ori_imagelist_key);
//    if(room_image_list == "") {
//      LOG(INFO)<<"room_image_list is null";
//      return;
//    }
//    //parse room_image_list
//    Json::Reader reader;
//    Json::Value root;
//    if (!reader.parse(room_image_list, root, false)) {
//      LOG(INFO)<<"parse room_image_list json error";
//      return;
//    }
//    std::vector<std::string> image_docid_keys;
//    for(size_t i=0;i<root.size();i++) {
//      image_docid_keys.push_back(root[i].asString());
//    }
//
//    //get cut image list by image_docid
//    if(image_docid_keys.size() == 0) {
//      LOG(INFO)<<"image docid key is null";
//      return;
//    }
//    std::vector<std::string> cut_image_lists;
//    MGet(cut_image_lists,"cut_imagelist_store_table", image_docid_keys);
//    for(size_t i=0;i<cut_image_lists.size();i++) {
//      Json::Reader reader_tmp;
//      Json::Value root_tmp;
//      if (!reader.parse(cut_image_lists[i], root_tmp, false)) {
//        LOG(INFO)<<"parse json error";
//        continue;
//      }
//      image_list.push_back(root_tmp[1].asString());
//    }
//    if(image_list.size() == 0) {
//      LOG(INFO)<<"cut image list is null";
//      return;
//    }
//    int64 endtime = base::GetTimeInUsec();
//    int64 spendtime = endtime-starttime;
//    LOG(INFO)<<"GetRoomImageList spend time "<<spendtime;
//}
//
//void RafDb::GetRoomImageList(std::vector<std::string> & image_list, const std::string& base_hotel_id,
//    const std::string& ota_id, const std::string& room_hash) {
//uint64 room_hash_uint64;
//if ( !StringToUint64(room_hash,&room_hash_uint64)) {
//  return;
//}
//std::string room_hash_convert = Uint64ToString(room_hash_uint64);
//std::string key_str = base_hotel_id+std::string("_")+ota_id+std::string("_")+room_hash_convert;
////uint64 key_i64 = base::MurmurHash64A(key_str.c_str(),key_str.size(),19820125);
//{
//  base::MutexLock lock(&image_resp_map_mutex_);
//  base::hash_map<std::string,std::vector<std::string> >::iterator it =
//    image_resp_map_.find(key_str);
//  if (it != image_resp_map_.end()) {
//    image_list = image_resp_map_[key_str];
//    LOG(INFO)<<"hit map,key str is "<<key_str;
//    return;
//  }
//}
//return _GetRoomImageList(image_list,base_hotel_id,ota_id,room_hash_convert); 
//}
//
//
//
//bool RafDb::_UpdateRoomImageList(const std::string& hotel_id, const std::string& ota_id, 
//  const std::string& room_hash, const std::vector<std::string> & image_list) {
//
//int64 starttime = base::GetTimeInUsec();
//if (image_list.size() == 0) {
//  return false;
//}
//std::string room_ori_imagelist_key = hotel_id+std::string("_")+ota_id+std::string("_")+room_hash;
//std::string room_image_list;
//Get(room_image_list,"room_ori_imagelist_table", room_ori_imagelist_key);
//if (room_image_list == "") {
//  LOG(INFO)<<"room_image_list is null,will insert";
//  Json::Value root;
//  for(size_t i =0;i<image_list.size();i++) {
//    std::string url = image_list[i];
//    if (url.find("http://") ==std::string::npos) {
//      continue;
//    }
//    std::string image_docid = Uint64ToString(base::MurmurHash64A(url.c_str(),url.size(),19820125));
//    Json::Value tmp = image_docid;
//    root.append(tmp);
//    std::string room_pa_increment_key_str = url+hotel_id+ota_id+room_hash;
//    std::string room_pa_increment_key_docid =Uint64ToString(base::MurmurHash64A(
//          room_pa_increment_key_str.c_str(),room_pa_increment_key_str.size(),19820125));
//    Json::Value pa_room_root;
//    Json::Value item1 = url;
//    Json::Value item2 = hotel_id;
//    Json::Value item3 = ota_id;
//    Json::Value item4 = room_hash;
//    pa_room_root.append(item1);
//    pa_room_root.append(item2);
//    pa_room_root.append(item3);
//    pa_room_root.append(item4);
//    Json::FastWriter writer1;
//    std::string out1 = writer1.write(pa_room_root);
//    Set("room_pa_increment_table", room_pa_increment_key_docid,out1);
//    LOG(INFO)<<"set room_pa_increment_table key is "<<room_pa_increment_key_docid
//      <<" value is "<<out1;
//  }
//  Json::FastWriter writer2;
//  std::string out2 = writer2.write(root);
//  Set("room_ori_imagelist_table", room_ori_imagelist_key,out2);
//  LOG(INFO)<<"set room_ori_imagelist_table key is "<<room_ori_imagelist_key
//    <<" value is "<<out2;
//}else {
//  LOG(INFO)<<"room_image_list is not null,will update";
//  Json::Reader reader;
//  Json::Value root2;
//  if (!reader.parse(room_image_list, root2, false)) {
//    LOG(INFO)<<"parse room_image_list json error";
//    return false;
//  }
//  std::vector<std::string> image_docid_keys;
//  std::set<std::string> image_docid_set;
//  for(size_t i=0;i<root2.size();i++) {
//    LOG(INFO)<<"image_docid is "<<root2[i].asString();
//    image_docid_keys.push_back(root2[i].asString());
//    image_docid_set.insert(root2[i].asString());
//  }
//  std::vector<std::string> image_param_docid_keys;
//  std::set<std::string> image_param_docid_set;
//  for(size_t i=0;i<image_list.size();i++) {
//    std::string url = image_list[i];
//    if (url.find("http://") ==std::string::npos) {
//      continue;
//    }
//    std::string image_docid = Uint64ToString(base::MurmurHash64A(url.c_str(),url.size(),19820125));
//    image_param_docid_keys.push_back(image_docid);
//    image_param_docid_set.insert(image_docid);
//    std::set<std::string>::iterator it = image_docid_set.find(image_docid);
//    if(it == image_docid_set.end()) {
//      LOG(INFO)<<"image docid "<<image_docid<<" not found";
//      std::string room_pa_increment_key_str = url+hotel_id+ota_id+room_hash;
//      std::string room_pa_increment_key_docid =Uint64ToString(base::MurmurHash64A(
//          room_pa_increment_key_str.c_str(),room_pa_increment_key_str.size(),19820125));
//      Json::Value pa_room_root;
//      Json::Value item1 = url;
//      Json::Value item2 = hotel_id;
//      Json::Value item3 = ota_id;
//      Json::Value item4 = room_hash;
//      pa_room_root.append(item1);
//      pa_room_root.append(item2);
//      pa_room_root.append(item3);
//      pa_room_root.append(item4);
//      Json::FastWriter writer1;
//      std::string out1 = writer1.write(pa_room_root);
//      Set("room_pa_increment_table", room_pa_increment_key_docid,out1);
//      LOG(INFO)<<"set room_pa_increment_table key is "<<room_pa_increment_key_docid
//        <<" value is "<<out1;
//    }
//  }//if image_url not exists then insert into room_pa_increment_table
//  for(size_t i=0;i<image_docid_keys.size();i++) {
//    std::string item_docid = image_docid_keys[i];
//    std::set<std::string>::iterator it = image_param_docid_set.find(item_docid);
//    if(it == image_param_docid_set.end()) {
//      image_param_docid_keys.push_back(item_docid);
//    }
//  }//param 1,2,3,4;leveldb 2,3,5;then param become 1,2,3,4,5
//  Json::Value root3;
//  for(size_t i=0;i<image_param_docid_keys.size();i++) {
//    Json::Value item_tmp = image_param_docid_keys[i];
//    root3.append(item_tmp);
//  }
//  Json::FastWriter writer3;
//  std::string out3 = writer3.write(root3);
//  Set("room_ori_imagelist_table",room_ori_imagelist_key,out3);
//  LOG(INFO)<<"set room_ori_imagelist_table,key is "<<room_ori_imagelist_key
//    <<" value is "<<out3;
//}
//int64 endtime = base::GetTimeInUsec();
//int64 spendtime = endtime-starttime;
//
//LOG(INFO)<<"_UpdateRoomImageList spend "<<spendtime;
//return true;
//}
//
//bool RafDb::UpdateRoomImageList(const std::string& hotel_id, const std::string& ota_id, 
//  const std::string& room_hash, const std::vector<std::string> & image_list) {
//int64 starttime = base::GetTimeInUsec();
//if (image_list.size() == 0) {
//  return false;
//}
//uint64 room_hash_uint64;
//if ( !StringToUint64(room_hash,&room_hash_uint64)) {
//  return false;
//}
//std::string room_hash_convert = Uint64ToString(room_hash_uint64);
//RoomImageInfo tmp;
//tmp.hotel_id = hotel_id;
//tmp.ota_id= ota_id;
//tmp.room_hash= room_hash_convert;
//tmp.image_list= image_list;
//image_set_queue_.Push(tmp);
//int64 endtime = base::GetTimeInUsec();
//int64 spendtime = endtime-starttime;
//LOG(INFO)<<"UpdateRoomImageList spend "<<spendtime;
//return true;
//}
//
//
//
//void RafDb::GetHotelImageList(std::vector<std::string> & image_list, 
//  const std::string& base_hotel_id, const std::string& ota_id) {
////base::MutexLock lock(&get_hotel_image_mutex_);
//int64 starttime = base::GetTimeInUsec();
//std::string hotel_id;
//std::string base_hotel_map_key = base_hotel_id + "_"+ota_id;
//Get(hotel_id,"base_hotel_map_table", base_hotel_map_key);
//if (hotel_id == "") {
//  LOG(INFO)<<"hotel_id is null";
//  return;
//}
//
//std::string hotel_image_list;
//std::string hotel_ori_imagelist_key = hotel_id + "_"+ota_id;
////get hotel ori imagelist by hotel_ori_imagelist_key
////for example ["123123123:1","3123123123:0"]
//Get(hotel_image_list,"hotel_ori_imagelist_table", hotel_ori_imagelist_key);
//if(hotel_image_list == "") {
//  LOG(INFO)<<"hotel_image_list is null";
//  return;
//}
////parse hotel_image_list
//Json::Reader reader;
//Json::Value root;
//if (!reader.parse(hotel_image_list, root, false)) {
//  LOG(INFO)<<"parse hotel_image_list json error";
//  return;
//}
//std::vector<std::string> image_docid_keys;
//std::vector<std::string> image_types;
//for(size_t i=0;i<root.size();i++) {
//  std::string imageDocid_imageType = root[i].asString();
//  std::vector<std::string> tmp_vec;
//  SplitString(imageDocid_imageType,':',&tmp_vec);
//  if(tmp_vec.size() == 2) {
//    image_docid_keys.push_back(tmp_vec[0]);
//    image_types.push_back(tmp_vec[1]);
//  } 
//}
////get cut image list by image_docid
//if(image_docid_keys.size() == 0) {
//  LOG(INFO)<<"image docid key is null";
//  return;
//}
//std::vector<std::string> cut_image_lists;
//MGet(cut_image_lists,"cut_imagelist_store_table", image_docid_keys);
//for(size_t i=0;i<cut_image_lists.size();i++) {
//  Json::Reader reader_tmp;
//  Json::Value root_tmp;
//  if (!reader.parse(cut_image_lists[i], root_tmp, false)) {
//    LOG(INFO)<<"parse json error";
//    continue;
//  }
//  std::string cut_image_key = root_tmp[1].asString();
//  image_list.push_back(cut_image_key+std::string(":")+image_types[i]);
//}
//if(image_list.size() == 0) {
//  LOG(INFO)<<"cut image list is null";
//  return;
//}
//
//int64 endtime = base::GetTimeInUsec();
//LOG(INFO)<<"GetHotelImagelist spend "<<(endtime-starttime);
//}
//
//bool RafDb::UpdateHotelImageList(const std::string& hotel_id, 
//  const std::string& ota_id, const std::vector<std::string> & image_list) {
//if (image_list.size() == 0) {
//  return false;
//}
////base::MutexLock lock(&update_hotel_image_mutex_); 
//std::string hotel_ori_imagelist_key = hotel_id+std::string("_")+ota_id;
//std::string hotel_image_list;
//Get(hotel_image_list,"hotel_ori_imagelist_table", hotel_ori_imagelist_key);
//if (hotel_image_list == "") {
//  LOG(INFO)<<"hotel_image_list is null,will insert";
//  Json::Value root;
//  for(size_t i =0;i<image_list.size();i++) {
//    std::string url = image_list[i];
//    if (url.find("http://") ==std::string::npos) {
//      LOG(INFO)<<"not valid url";
//      continue;
//    }
//    std::string image_docid = Uint64ToString(base::MurmurHash64A(url.c_str(),url.size(),19820125));
//    if(i==0) {
//      image_docid += std::string(":1");
//    }else {
//      image_docid += std::string(":0");
//    }
//    Json::Value tmp = image_docid;
//    root.append(tmp);
//    std::string hotel_pa_increment_key_str = url+hotel_id+ota_id;
//    std::string hotel_pa_increment_key_docid =Uint64ToString(base::MurmurHash64A(
//          hotel_pa_increment_key_str.c_str(),hotel_pa_increment_key_str.size(),19820125));
//    Json::Value pa_hotel_root;
//    Json::Value item1 = url;
//    Json::Value item2 = hotel_id;
//    Json::Value item3 = ota_id;
//    pa_hotel_root.append(item1);
//    pa_hotel_root.append(item2);
//    pa_hotel_root.append(item3);
//    Json::FastWriter writer1;
//    std::string out1 = writer1.write(pa_hotel_root);
//    Set("hotel_pa_increment_table", hotel_pa_increment_key_docid,out1);
//    LOG(INFO)<<"set hotel_pa_increment_table key is "<<hotel_pa_increment_key_docid
//      <<" value is "<<out1;
//  }
//  Json::FastWriter writer2;
//  std::string out2 = writer2.write(root);
//  Set("hotel_ori_imagelist_table", hotel_ori_imagelist_key,out2);
//  LOG(INFO)<<"set hotel_ori_imagelist_table key is "<<hotel_ori_imagelist_key
//    <<" value is "<<out2;
//}else {
//  LOG(INFO)<<"hotel_image_list is not null,will update";
//  Json::Reader reader;
//  Json::Value root2;
//  if (!reader.parse(hotel_image_list, root2, false)) {
//    LOG(INFO)<<"parse hotel_image_list json error";
//    return false;
//  }
//  std::vector<std::string> image_docid_keys;
//  std::vector<std::string> image_types;
//  std::set<std::string> image_docid_set;
//  for(size_t i=0;i<root2.size();i++) {
//    std::string imageDocid_imageType = root2[i].asString();
//    std::vector<std::string> tmp_vec;
//    SplitString(imageDocid_imageType,':',&tmp_vec);
//    if(tmp_vec.size() == 2) {
//      image_docid_keys.push_back(tmp_vec[0]);
//      image_types.push_back(tmp_vec[1]);
//      image_docid_set.insert(tmp_vec[0]);
//    } 
//  }
//  std::vector<std::string> image_param_docid_keys;
//  std::vector<std::string> image_param_types;
//  std::set<std::string> image_param_docid_set;
//  for(size_t i=0;i<image_list.size();i++) {
//    std::string url = image_list[i];
//    if (url.find("http://") ==std::string::npos) {
//      LOG(INFO)<<"not valid url";
//      continue;
//    }
//    std::string image_docid = Uint64ToString(base::MurmurHash64A(url.c_str(),url.size(),19820125));
//    image_param_docid_keys.push_back(image_docid);
//    if(i==0) {
//      image_param_types.push_back(std::string("1"));
//    }else {
//      image_param_types.push_back(std::string("0"));
//    }
//    image_param_docid_set.insert(image_docid);
//    std::set<std::string>::iterator it = image_docid_set.find(image_docid);
//    if(it == image_docid_set.end()) {
//      LOG(INFO)<<"image docid "<<image_docid<<" not found";
//      std::string hotel_pa_increment_key_str = url+hotel_id+ota_id;
//      std::string hotel_pa_increment_key_docid =Uint64ToString(base::MurmurHash64A(
//          hotel_pa_increment_key_str.c_str(),hotel_pa_increment_key_str.size(),19820125));
//      Json::Value pa_hotel_root;
//      Json::Value item1 = url;
//      Json::Value item2 = hotel_id;
//      Json::Value item3 = ota_id;
//      pa_hotel_root.append(item1);
//      pa_hotel_root.append(item2);
//      pa_hotel_root.append(item3);
//      Json::FastWriter writer1;
//      std::string out1 = writer1.write(pa_hotel_root);
//      Set("hotel_pa_increment_table", hotel_pa_increment_key_docid,out1);
//      LOG(INFO)<<"set hotel_pa_increment_table key is "<<hotel_pa_increment_key_docid
//        <<" value is "<<out1;
//    }
//  }//if image_url not exists then insert into hotel_pa_increment_table
//  for(size_t i=0;i<image_docid_keys.size();i++) {
//    std::string item_docid = image_docid_keys[i];
//    std::set<std::string>::iterator it = image_param_docid_set.find(item_docid);
//    if(it == image_param_docid_set.end()) {
//      image_param_docid_keys.push_back(item_docid);
//      if (image_types[i] == "1") {
//        image_param_types.push_back(std::string("0"));
//      }else {
//        image_param_types.push_back(image_types[i]);
//      }
//    }
//  }//param 1,2,3,4;leveldb 2,3,5;then param become 1,2,3,4,5
//  Json::Value root3;
//  for(size_t i=0;i<image_param_docid_keys.size();i++) {
//    std::string val_tmp = image_param_docid_keys[i]+std::string(":")+image_param_types[i];
//    Json::Value item_tmp = val_tmp;
//    root3.append(item_tmp);
//  }
//  Json::FastWriter writer3;
//  std::string out3 = writer3.write(root3);
//  Set("hotel_ori_imagelist_table",hotel_ori_imagelist_key,out3);
//  LOG(INFO)<<"set hotel_ori_imagelist_table,key is "<<hotel_ori_imagelist_key
//    <<" value is "<<out3;
//}
//return true;
//}
leveldb::DB* RafDb::Open(const std::string &dbname) {
  leveldb::DB* db;
  leveldb::Options options_1;
  options_1.create_if_missing = true;
  //options_1.block_cache = (leveldb::Cache*)(new leveldb::NewLRUCache(FLAGS_leveldb_block_cache));
  options_1.block_cache = leveldb::NewLRUCache(FLAGS_leveldb_block_cache);
  leveldb::Status status = leveldb::DB::Open(options_1,
      FLAGS_db_dir + dbname, &db);
  if (!status.ok()) {
    LOG(ERROR) << "Can't open db:" << dbname;
    return NULL;
  }
  db_map_[dbname] = db;
  return db;
}

void RafDb::LoadDB() {
  base::MutexLock lock(&db_map_mutex_);
  if (file::File::Exists(FLAGS_db_dir) && !file::File::IsDir(FLAGS_db_dir)) {
    LOG(INFO) << "databases directory is normal file!";
    exit(1);
  } else if (!file::File::Exists(FLAGS_db_dir)) {
    file::File::CreateDir(FLAGS_db_dir, 0775);
  } else {
    std::vector<std::string> dirs;
    file::File::GetDirsInDir(FLAGS_db_dir, &dirs);
    for (int i = 0; i < dirs.size(); i++) {
      std::string dbname = dirs[i].substr(dirs[i].rfind('/') + 1);
      base::hash_map<std::string, leveldb::DB*>::iterator it =
        db_map_.find(dbname);
      if (it == db_map_.end()) {
        Open(dbname);
      }
    }
  }
}

void IteratorChecker::Run() {
  while (true) {
    sleep(FLAGS_check_interval);

    base::MutexLock lock(&rafdb_->it_map_mutex_);
    base::hash_map<int32_t, RafDb::WrapIterator*>::iterator it =
      rafdb_->it_map_.begin();
    time_t now = time(NULL);
    for (; it != rafdb_->it_map_.end(); ) {
      if (now - it->second->last_operation_ > 3 * FLAGS_check_interval) {
        LOG(WARNING) << "iterator " << it->first << " forget close!";
        delete it->second;
        it->second = NULL;
        rafdb_->it_map_.erase(it++);
      } else {
        it++;
      }
    }
  }
}

void RafDb::SendVote(const rafdb::Message& message) {
  Message tmp_m = message;
  tmp_m.message_type = MessageType::VOTEREQ;
  message_queue_.Push(tmp_m);
}
void RafDb::ReplyVote(const rafdb::Message& message) {
  Message tmp_m = message;
  tmp_m.message_type = MessageType::VOTEREP;
  message_queue_.Push(tmp_m);


}
void RafDb::SendHeartBeat(const rafdb::Message& message) {
  Message tmp_m = message;
  tmp_m.message_type = MessageType::HEARTREQ;
  message_queue_.Push(tmp_m);

}
void RafDb::ReplyHeartBeat(const rafdb::Message& message) {
  Message tmp_m = message;
  tmp_m.message_type = MessageType::HEARTREP;
  message_queue_.Push(tmp_m);

}
void RafDb::QueryLeaderId(const rafdb::Message& message) {
  Message tmp_m = message;
  tmp_m.message_type = MessageType::LEADERREQ;
  message_queue_.Push(tmp_m);


}
void RafDb::ReplyLeaderId(const rafdb::Message& message) {
  Message tmp_m = message;
  tmp_m.message_type = MessageType::LEADERREP;
  message_queue_.Push(tmp_m);

}
bool RafDb::IsHealthy() {
  return true;
}

bool RafDb::LSet(const std::string &dbname, const std::string &key,
       const std::string &value) {
  if(Set(dbname,key,value)) {
    LKV *tmp_lkv = new LKV();
    tmp_lkv->dbname = dbname;
    tmp_lkv->key = key;
    tmp_lkv->value = value;
    lkv_queue_.Push(tmp_lkv);
    VLOG(5)<<"set success,push to queue";
    return true;
  }  
  return false;
}


}  // namespace leveldbd
