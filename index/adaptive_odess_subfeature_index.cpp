#include "index/adaptive_odess_subfeature_index.h"
#include <algorithm>
#include <stdexcept>
#include <glog/logging.h>

namespace Delta {

AdaptiveOdessSubfeatureIndex::CompactPosting::~CompactPosting() {
  if (heap_) delete storage_.overflow;
}

AdaptiveOdessSubfeatureIndex::CompactPosting::CompactPosting(
    CompactPosting &&other) noexcept
    : size_(other.size_), heap_(other.heap_) {
  if (other.has_external_pointer()) {
    storage_.external = other.storage_.external;
  } else if (heap_) {
    storage_.overflow = other.storage_.overflow;
    other.storage_.overflow = nullptr;
  } else {
    for (size_t i = 0; i < size_; ++i) {
      storage_.inline_slots[i] = other.storage_.inline_slots[i];
    }
  }
  other.size_ = 0;
  other.heap_ = false;
}

AdaptiveOdessSubfeatureIndex::CompactPosting &
AdaptiveOdessSubfeatureIndex::CompactPosting::operator=(
    CompactPosting &&other) noexcept {
  if (this == &other) return *this;
  if (heap_) delete storage_.overflow;
  size_ = other.size_;
  heap_ = other.heap_;
  if (other.has_external_pointer()) {
    storage_.external = other.storage_.external;
  } else if (heap_) {
    storage_.overflow = other.storage_.overflow;
    other.storage_.overflow = nullptr;
  } else {
    for (size_t i = 0; i < size_; ++i) {
      storage_.inline_slots[i] = other.storage_.inline_slots[i];
    }
  }
  other.size_ = 0;
  other.heap_ = false;
  return *this;
}

void AdaptiveOdessSubfeatureIndex::CompactPosting::push_back(RefSlot slot) {
  if (!heap_ && size_ < kInlineCapacity) {
    storage_.inline_slots[size_++] = slot;
    return;
  }
  if (!heap_) {
    auto *overflow = new std::vector<RefSlot>();
    overflow->reserve(kInlineCapacity * 2);
    for (size_t i = 0; i < size_; ++i) {
      overflow->push_back(storage_.inline_slots[i]);
    }
    storage_.overflow = overflow;
    heap_ = true;
  }
  storage_.overflow->push_back(slot);
  size_++;
}

size_t AdaptiveOdessSubfeatureIndex::CompactPosting::capacity() const {
  return heap_ ? storage_.overflow->capacity() : kInlineCapacity;
}

size_t AdaptiveOdessSubfeatureIndex::CompactPosting::dynamic_bytes() const {
  if (!heap_) return 0;
  return sizeof(std::vector<RefSlot>) +
         storage_.overflow->capacity() * sizeof(RefSlot);
}

const AdaptiveOdessSubfeatureIndex::RefSlot *
AdaptiveOdessSubfeatureIndex::CompactPosting::begin() const {
  return heap_ ? storage_.overflow->data() : storage_.inline_slots;
}

const AdaptiveOdessSubfeatureIndex::RefSlot *
AdaptiveOdessSubfeatureIndex::CompactPosting::end() const {
  return begin() + size_;
}

AdaptiveOdessSubfeatureIndex::RefSlot
AdaptiveOdessSubfeatureIndex::CompactPosting::operator[](size_t index) const {
  if (index >= size_) throw std::out_of_range("compact posting index");
  return begin()[index];
}

bool AdaptiveOdessSubfeatureIndex::CompactPosting::has_external_pointer()
    const {
  return size_ == kExternalPointerSize;
}

void *AdaptiveOdessSubfeatureIndex::CompactPosting::external_pointer() const {
  if (!has_external_pointer()) return nullptr;
  return storage_.external;
}

void AdaptiveOdessSubfeatureIndex::CompactPosting::set_external_pointer(
    void *pointer) {
  if (heap_) delete storage_.overflow;
  storage_.external = pointer;
  size_ = kExternalPointerSize;
  heap_ = false;
}

void *AdaptiveOdessSubfeatureIndex::CompactPosting::
release_external_pointer() {
  if (!has_external_pointer()) return nullptr;
  void *pointer = storage_.external;
  storage_.inline_slots[0] = 0;
  storage_.inline_slots[1] = 0;
  size_ = 0;
  heap_ = false;
  return pointer;
}

AdaptiveOdessSubfeatureIndex::CompactBucket::~CompactBucket() {
  delete static_cast<CompactConditionalBucket *>(
      storage_.release_external_pointer());
}

AdaptiveOdessSubfeatureIndex::CompactBucket::CompactBucket(
    CompactBucket &&other) noexcept
    : storage_(std::move(other.storage_)) {}

AdaptiveOdessSubfeatureIndex::CompactBucket &
AdaptiveOdessSubfeatureIndex::CompactBucket::operator=(
    CompactBucket &&other) noexcept {
  if (this == &other) return *this;
  delete static_cast<CompactConditionalBucket *>(
      storage_.release_external_pointer());
  storage_ = std::move(other.storage_);
  return *this;
}

AdaptiveOdessSubfeatureIndex::CompactConditionalBucket *
AdaptiveOdessSubfeatureIndex::CompactBucket::conditional() {
  return static_cast<CompactConditionalBucket *>(
      storage_.external_pointer());
}

const AdaptiveOdessSubfeatureIndex::CompactConditionalBucket *
AdaptiveOdessSubfeatureIndex::CompactBucket::conditional() const {
  return static_cast<const CompactConditionalBucket *>(
      storage_.external_pointer());
}

void AdaptiveOdessSubfeatureIndex::CompactBucket::SetConditional(
    std::unique_ptr<CompactConditionalBucket> conditional) {
  storage_.set_external_pointer(conditional.release());
}

AdaptiveOdessSubfeatureIndex::~AdaptiveOdessSubfeatureIndex() = default;

std::vector<std::pair<chunk_id, uint32_t>>
AdaptiveOdessSubfeatureIndex::GetTopKCandidates(const Feature &feat, int k) {
  const auto &features = feat;
  if (features.size() < static_cast<size_t>(kFeatureCount) || k <= 0) {
    return {};
  }

  return GetCompactProgressiveCandidates(features, k);
}

void AdaptiveOdessSubfeatureIndex::StartVoteEpoch() {
  vote_epoch_++;
  if (vote_epoch_ == 0) {
    std::fill(vote_epochs_.begin(), vote_epochs_.end(), 0);
    vote_epoch_ = 1;
  }
  touched_ids_.clear();
}

void AdaptiveOdessSubfeatureIndex::AddVote(chunk_id id) {
  const size_t required = static_cast<size_t>(id) + 1;
  if (vote_epochs_.size() < required) {
    vote_epochs_.resize(required, 0);
    vote_counts_.resize(required, 0);
  }
  if (vote_epochs_[id] != vote_epoch_) {
    vote_epochs_[id] = vote_epoch_;
    vote_counts_[id] = 0;
    touched_ids_.push_back(id);
  }
  vote_counts_[id]++;
}

std::vector<std::pair<chunk_id, uint32_t>>
AdaptiveOdessSubfeatureIndex::SelectTopK(int k) {
  std::vector<std::pair<chunk_id, uint32_t>> candidates;
  candidates.reserve(touched_ids_.size());
  for (chunk_id id : touched_ids_) {
    const uint32_t count = vote_counts_[id];
    if (count >= kMinMatchCount) candidates.emplace_back(id, count);
  }
  const auto vote_then_id = [this](const auto &a, const auto &b) {
    if (a.second != b.second) return a.second > b.second;
    return ChunkIdForCandidate(a.first) < ChunkIdForCandidate(b.first);
  };
  if (candidates.empty()) return candidates;

  const auto original_best =
      *std::min_element(candidates.begin(), candidates.end(),
                        [&vote_then_id](const auto &a, const auto &b) {
                          return vote_then_id(a, b);
                        });
  const uint32_t best_vote_count = original_best.second;
  const auto better = [this, depth_aware_tiebreak = depth_aware_tiebreak_,
                       best_vote_count](const auto &a, const auto &b) {
    if (depth_aware_tiebreak && depth_match_tolerance_ != 0) {
      const bool a_in_window =
          a.second + depth_match_tolerance_ >= best_vote_count;
      const bool b_in_window =
          b.second + depth_match_tolerance_ >= best_vote_count;
      if (a_in_window != b_in_window) return a_in_window;
      if (a_in_window) {
        const uint16_t a_depth = ChainDepthForCandidate(a.first);
        const uint16_t b_depth = ChainDepthForCandidate(b.first);
        if (a_depth != b_depth) return a_depth < b_depth;
        if (a.second != b.second) return a.second > b.second;
        return ChunkIdForCandidate(a.first) < ChunkIdForCandidate(b.first);
      }
    }
    if (a.second != b.second) return a.second > b.second;
    if (depth_aware_tiebreak) {
      const uint16_t a_depth = ChainDepthForCandidate(a.first);
      const uint16_t b_depth = ChainDepthForCandidate(b.first);
      if (a_depth != b_depth) return a_depth < b_depth;
    }
    return ChunkIdForCandidate(a.first) < ChunkIdForCandidate(b.first);
  };
  if (static_cast<int>(candidates.size()) > k) {
    std::partial_sort(candidates.begin(), candidates.begin() + k,
                      candidates.end(), better);
    candidates.resize(k);
  } else {
    std::sort(candidates.begin(), candidates.end(), better);
  }

  for (auto &candidate : candidates) {
    candidate.first = ChunkIdForCandidate(candidate.first);
  }
  return candidates;
}

uint16_t AdaptiveOdessSubfeatureIndex::ChainDepthFor(chunk_id id) const {
  if (chain_depths_ == nullptr || id >= chain_depths_->size()) {
    return std::numeric_limits<uint16_t>::max();
  }
  return (*chain_depths_)[id];
}

chunk_id AdaptiveOdessSubfeatureIndex::ChunkIdForCandidate(
    chunk_id candidate) const {
  if (candidate >= compact_chunk_ids_.size()) {
    throw std::out_of_range("compact candidate slot is invalid");
  }
  return compact_chunk_ids_[candidate];
}

uint16_t AdaptiveOdessSubfeatureIndex::ChainDepthForCandidate(
    chunk_id candidate) const {
  return ChainDepthFor(ChunkIdForCandidate(candidate));
}

bool AdaptiveOdessSubfeatureIndex::HasConfidentWinner() const {
  uint32_t best = 0;
  uint32_t second = 0;
  for (chunk_id id : touched_ids_) {
    const uint32_t count = vote_counts_[id];
    if (count > best) {
      second = best;
      best = count;
    } else if (count > second) {
      second = count;
    }
  }
  return best >= confident_match_count_ &&
         best - second >= confidence_margin_;
}

std::vector<std::pair<chunk_id, uint32_t>>
AdaptiveOdessSubfeatureIndex::GetCompactProgressiveCandidates(
    const std::vector<uint64_t> &features, int k) {
  struct FallbackProbe {
    const CompactConditionalBucket *bucket = nullptr;
    FeatureKey target_secondary = 0;
  };

  StartVoteEpoch();
  std::vector<FallbackProbe> fallback_probes;
  fallback_probes.reserve(kFeatureCount);

  for (int i = 0; i < kFeatureCount; ++i) {
    const auto &table = compact_tables_[i];
    const FeatureKey primary_key = ToFeatureKey(features[i]);
    auto bucket_it = table.find(primary_key);
    if (bucket_it == table.end()) continue;
    const CompactBucket &bucket = bucket_it->second;
    if (!bucket.is_conditional()) {
      for (RefSlot slot : bucket.flat_slots()) AddVote(slot);
      continue;
    }

    const CompactConditionalBucket *conditional = bucket.conditional();
    const int secondary = conditional->secondary_feature;
    const FeatureKey target_secondary = ToFeatureKey(features[secondary]);
    auto partition_it =
        conditional->partitions.find(target_secondary);
    if (partition_it != conditional->partitions.end()) {
      for (RefSlot slot : partition_it->second) AddVote(slot);
    }
    fallback_probes.push_back(
        FallbackProbe{conditional, target_secondary});
  }

  const size_t max_budget =
      std::min(fallback_max_budget_, kFallbackRepresentatives);
  if ((k == 1 && HasConfidentWinner()) || fallback_probes.empty() ||
      max_budget == 0) {
    return SelectTopK(k);
  }

  size_t previous_budget = 0;
  size_t budget = std::min(
      max_budget, std::max<size_t>(1, fallback_initial_budget_));
  while (true) {
    for (const FallbackProbe &probe : fallback_probes) {
      const auto &fallback = probe.bucket->fallback_slots;
      const size_t end = std::min(budget, fallback.size());
      for (size_t position = std::min(previous_budget, end);
           position < end; ++position) {
        const RefSlot slot = fallback[position];
        const int secondary = probe.bucket->secondary_feature;
        if (StoredFeaturesForSlot(slot)[secondary] ==
            probe.target_secondary) {
          continue;
        }
        AddVote(slot);
      }
    }

    if ((k == 1 && HasConfidentWinner()) || budget >= max_budget) break;
    previous_budget = budget;
    budget = std::min(max_budget, budget * 2);
  }
  return SelectTopK(k);
}

void AdaptiveOdessSubfeatureIndex::AddFeature(const Feature &feat,
                                              chunk_id id) {
  const auto &features = feat;
  if (features.size() < static_cast<size_t>(kFeatureCount)) {
    LOG(WARNING) << "AdaptiveOdessSubfeatureIndex::AddFeature: feature vector "
                 << "has " << features.size() << " entries, expected "
                 << kFeatureCount;
    return;
  }
  AddCompactFeature(features, id);
}

AdaptiveOdessSubfeatureIndex::FeatureKey
AdaptiveOdessSubfeatureIndex::ToFeatureKey(uint64_t feature) {
  if (feature > std::numeric_limits<FeatureKey>::max()) {
    throw std::overflow_error("ODESS subfeature exceeds the uint32_t domain");
  }
  return static_cast<FeatureKey>(feature);
}

void AdaptiveOdessSubfeatureIndex::AddCompactFeature(
    const std::vector<uint64_t> &features, chunk_id id) {
  StoredFeature stored{};
  for (int i = 0; i < kFeatureCount; ++i) {
    stored[i] = ToFeatureKey(features[i]);
  }
  InsertCompactReference(stored, id);
}

void AdaptiveOdessSubfeatureIndex::InsertCompactReference(
    const StoredFeature &stored, chunk_id id) {
  if (compact_stored_features_.size() >=
      std::numeric_limits<RefSlot>::max()) {
    throw std::overflow_error("compact P-ACSI reference namespace exhausted");
  }
  const RefSlot slot =
      static_cast<RefSlot>(compact_stored_features_.size());
  compact_stored_features_.push_back(stored);
  compact_chunk_ids_.push_back(id);
  vote_epochs_.resize(compact_stored_features_.size(), 0);
  vote_counts_.resize(compact_stored_features_.size(), 0);
  IndexCompactReference(slot, stored, id);
}

void AdaptiveOdessSubfeatureIndex::IndexCompactReference(
    RefSlot slot, const StoredFeature &stored, chunk_id id) {
  for (int i = 0; i < kFeatureCount; ++i) {
    const FeatureKey primary_key = stored[i];
    CompactBucket &bucket = compact_tables_[i][primary_key];
    if (bucket.is_conditional()) {
      CompactConditionalBucket *conditional = bucket.conditional();
      const int secondary = conditional->secondary_feature;
      conditional->partitions[stored[secondary]].push_back(slot);
      conditional->seen_slots++;
      auto &fallback = conditional->fallback_slots;
      if (fallback.size() < kFallbackRepresentatives) {
        fallback.push_back(slot);
      } else {
        const uint64_t mixed =
            static_cast<uint64_t>(id) * UINT64_C(11400714819323198485);
        const uint64_t position =
            mixed % conditional->seen_slots;
        if (position < fallback.size()) {
          fallback[static_cast<size_t>(position)] = slot;
        }
      }
    } else {
      bucket.flat_slots().push_back(slot);
      if (bucket.flat_slots().size() > split_threshold_) {
        SplitCompactBucket(i, bucket);
      }
    }
  }
}

const AdaptiveOdessSubfeatureIndex::StoredFeature &
AdaptiveOdessSubfeatureIndex::StoredFeaturesForSlot(RefSlot slot) const {
  if (slot >= compact_stored_features_.size()) {
    throw std::out_of_range("compact P-ACSI feature slot is invalid");
  }
  return compact_stored_features_[slot];
}

void AdaptiveOdessSubfeatureIndex::SplitCompactBucket(
    int primary_feature, CompactBucket &bucket) {
  int best_secondary = -1;
  size_t best_largest_partition = std::numeric_limits<size_t>::max();
  size_t best_partition_count = 0;

  for (int secondary = 0; secondary < kFeatureCount; ++secondary) {
    if (secondary == primary_feature) continue;
    std::unordered_map<FeatureKey, size_t> counts;
    size_t largest_partition = 0;
    for (RefSlot slot : bucket.flat_slots()) {
      const FeatureKey value = StoredFeaturesForSlot(slot)[secondary];
      const size_t count = ++counts[value];
      largest_partition = std::max(largest_partition, count);
    }
    if (largest_partition < best_largest_partition ||
        (largest_partition == best_largest_partition &&
         counts.size() > best_partition_count)) {
      best_secondary = secondary;
      best_largest_partition = largest_partition;
      best_partition_count = counts.size();
    }
  }

  if (best_secondary < 0) return;
  auto conditional = std::make_unique<CompactConditionalBucket>();
  conditional->partitions.max_load_factor(kCompactMaxLoadFactor);
  conditional->secondary_feature = static_cast<uint8_t>(best_secondary);
  conditional->seen_slots = bucket.flat_slots().size();
  for (RefSlot slot : bucket.flat_slots()) {
    const FeatureKey value = StoredFeaturesForSlot(slot)[best_secondary];
    conditional->partitions[value].push_back(slot);
  }
  const size_t representative_count =
      std::min(kFallbackRepresentatives, bucket.flat_slots().size());
  conditional->fallback_slots.reserve(representative_count);
  for (size_t i = 0; i < representative_count; ++i) {
    const size_t index = representative_count == 1
        ? 0
        : i * (bucket.flat_slots().size() - 1) /
              (representative_count - 1);
    conditional->fallback_slots.push_back(bucket.flat_slots()[index]);
  }
  bucket.SetConditional(std::move(conditional));
}

} // namespace Delta
