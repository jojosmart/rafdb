
#include <vector>
#include <string>

#include "base/thrift.h"
#include "base/hash.h"
#include "base/logging.h"
#include "storage/image_leveldbd/proto/gen-cpp/LeveldbService.h"
#include "third_party/jsoncpp/include/json.h"
#include "base/string_util.h"

namespace {

const std::string kDbname = "cut_imagelist_store_table";
const std::string kKey    = "15866472803561682945";
const std::string kValue  = "value";
const std::string kIp     = "10.35.68.11";
const int    kPort   = 10001;

}
int main(int argc, char **argv) {

  std::string istr = "12314141";
  uint64 out;
  StringToUint64(istr,&out);
  LOG(INFO)<<Uint64ToString(out);
  return 0;
  base::ThriftClient<leveldbthrift::LeveldbServiceClient>
    client(kIp, kPort);

  //std::string url = argv[1];
  //LOG(INFO)<<base::FingerprintToString(base::MurmurHash64A(url.c_str(),url.size(),19820125));
  //LOG(INFO)<<base::MurmurHash64A(url.c_str(),url.size(),19820125);
  //std::string uint64_to_string = Uint64ToString(base::MurmurHash64A(url.c_str(),url.size(),19820125));
  //LOG(INFO)<<uint64_to_string;
  std::string result;
 
  try {
    //int32_t itId = client.GetService()->OpenIterator("base_hotel_map_table");
    int32_t itId = client.GetService()->OpenIterator("room_ori_imagelist_table");
    std::vector<leveldbthrift::Pair> pairs;
    client.GetService()->NextPairs(pairs,itId,1);
    while(pairs.size()>0) {
      LOG(INFO)<<pairs[0].key<<"=>"<<pairs[0].value;
      client.GetService()->NextPairs(pairs,itId,1);
    }
    client.GetService()->CloseIterator(itId);
    //LOG(INFO)<<"connect success";
    //std::string hotel_id;
    //std::string base_id = "69625";
    //std::string ota_id = "1";
    //std::string room_hash = "6819205872323150368";
    //std::string base_hotel_map_key = base_id + "_" + ota_id;
    //LOG(INFO)<<"connect success1";
    //client.GetService()->Get(hotel_id,"base_hotel_map_table", base_hotel_map_key);
    //
    //LOG(INFO)<<"connect success2";
    //LOG(INFO)<<"hotel_id is"<<hotel_id;
    //if(hotel_id== "") {
    //  LOG(INFO)<<"hotel_id kong";
    //  return -1;
    //}
    //std::string room_image_list;
    //std::string room_ori_imagelist_key = hotel_id + "_"+ota_id+"_"+room_hash;
    //client.GetService()->Get(room_image_list,"room_ori_imagelist_table", room_ori_imagelist_key);
    //if(room_image_list == "") {
    //  LOG(INFO)<<"room_image_list kong";
    //  return -1;
    //}
    //LOG(INFO)<<"room_image_list is"<<room_image_list;
    //Json::Reader reader;
    //Json::Value root;
    //if (!reader.parse(room_image_list, root, false)) {
    //  LOG(INFO)<<"parse json error";
    //  return -1;
    //}
    //std::vector<std::string> keys;
    //for (size_t i = 0;i<root.size();i++) {
    //  keys.push_back(root[i].asString());
    //}
    //std::vector<std::string> cut_image_lists;
    //client.GetService()->MGet(cut_image_lists,"cut_imagelist_store_table", keys);
    //for(size_t i=0;i<cut_image_lists.size();i++) {
    //  LOG(INFO)<<cut_image_lists[i];
    //  Json::Reader reader_tmp;
    //  Json::Value root_tmp;
    //  if (!reader.parse(cut_image_lists[i], root_tmp, false)) {
    //    LOG(INFO)<<"parse json error";
    //    continue;
    //  }
    //  LOG(INFO)<<root_tmp[1].asString();
    //}
  } catch (const TException &tx) {
    LOG(INFO)<<"error";
    LOG(INFO)<<tx.what();
    return -1;
  }
  client.GetTransport()->close();
  
  
  
 // base::ThriftClient<leveldbthrift::LeveldbServiceClient>
 //   client(kIp, kPort);
 // std::string result;
 // if (client.GetService() == NULL) {
 //   LOG(INFO) << "connect failed";
 // }else {
 //   LOG(INFO) << "connect success";
 //   client.GetService()->Get(result,"cut_imagelist_store_table", "15866472803561682945");
 //   client.GetService()->Get(result,"guangyu", "key");
 //   LOG(INFO)<<result;
 // }
  return 0;
}


