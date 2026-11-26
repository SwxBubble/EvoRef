#include "storage/file_meta.h"
#include "storage/storage.h"
#include <cstdint>
#include <filesystem>
#include <vector>
namespace Delta {
class RecoverFile {
public:
  RecoverFile();
  ~RecoverFile();
  void RecoverSingleFile();
  void RecoverAllFiles();
  void RecoverRandomChunks();
  void VerifyAllFiles();
private:
  void RecoverSingle(const FileMeta &meta, const std::string &path);
  void RecoverSingleOars(const FileMeta &meta);
  void ConfigureOars(double decode_weight_bytes);
  void UpdateRecoveryChecksums(chunk_id id, const Chunk &chunk);
  void UpdateRecoveryOrderedChecksum(chunk_id id, uint32_t length,
                                     uint8_t first, uint8_t last);
  void UpdateRecoveryFingerprint(chunk_id id, uint32_t length,
                                 uint8_t first, uint8_t last);
  std::unique_ptr<Storage> storage_;
  uint64_t recovered_files_ = 0;
  uint64_t recovered_chunks_ = 0;
  uint64_t recovered_bytes_ = 0;
  uint64_t recovery_time_ns_ = 0;
  bool print_file_timing_ = true;
  bool use_oars_ = false;
  uint64_t recovery_cache_profile_workspace_bytes_ = 0;
  uint64_t recovery_os_warm_bytes_ = 0;
  uint64_t recovery_os_warm_time_ns_ = 0;
  bool materialize_output_ = true;
  uint64_t recovery_checksum_ = UINT64_C(1469598103934665603);
  uint64_t recovery_order_independent_fingerprint_ = 0;
  uint64_t sequential_base_chunks_ = 0;
  uint64_t sequential_delta_chunks_ = 0;
  uint64_t sequential_duplicate_chunks_ = 0;
  uint64_t file_aware_plan_time_ns_ = 0;
  std::vector<ChunkMeta> forest_metadata_;
  std::vector<int32_t> forest_local_index_;
  uint64_t forest_full_node_occurrences_ = 0;
  uint64_t forest_full_delta_occurrences_ = 0;
  uint64_t forest_clipped_node_occurrences_ = 0;
  uint64_t forest_cached_roots_ = 0;
  uint64_t forest_cache_probe_misses_ = 0;
  uint64_t forest_reconstructed_base_nodes_ = 0;
  uint64_t forest_reconstructed_delta_nodes_ = 0;
  uint64_t forest_reconstructed_duplicate_nodes_ = 0;
  uint64_t forest_active_payload_bytes_ = 0;
  uint64_t forest_peak_active_payload_bytes_ = 0;
  uint64_t forest_peak_file_workspace_bytes_ = 0;
  uint64_t forest_file_build_time_ns_ = 0;
  uint64_t forest_traversal_time_ns_ = 0;
};
}
