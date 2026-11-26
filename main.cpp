#include "config.h"
#include "delta_compression.h"
#include "recover_file.h"
#include <algorithm>
#include <filesystem>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <string>
#include <vector>
#include <sys/resource.h>
#include <cstring>
DEFINE_string(config, "configs/compress.toml", "path to config file");
using namespace Delta;
int main(int argc, char *argv[]) {
  // Suppress verbose per-chunk LOG(INFO) messages during experiments. Final
  // timing/statistics printed with std::cout are unaffected.
  FLAGS_minloglevel = google::WARNING;
  FLAGS_stderrthreshold = google::WARNING;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  LOG(INFO) << "using config file " << FLAGS_config;
  Config::Instance().Init(FLAGS_config);
  const auto task = Config::Instance().get()->get_as<std::string>("task");
  if (!task) LOG(FATAL) << "Missing required configuration key: task";
  if (*task == "compression") {
    auto compression = std::make_unique<Delta::DeltaCompression>();
    auto task_data_dir =
        Config::Instance().get()->get_as<std::string>("task_data_dir");
    std::vector<std::filesystem::path> input_files;
    for (const auto &entry :
         std::filesystem::recursive_directory_iterator(*task_data_dir)) {
      if (entry.is_regular_file()) {
        input_files.push_back(entry.path());
      }
    }
    // EvoRef is online and order-sensitive. Natural version order is the
    // publication default; the other modes are retained for explicit datasets.
    const std::string input_order = Config::Instance()
                                        .get()
                                        ->get_as<std::string>("input_order")
                                        .value_or<std::string>("natural_version");
    if (input_order == "natural_version") {
      std::sort(input_files.begin(), input_files.end(),
                [](const std::filesystem::path &a,
                   const std::filesystem::path &b) {
                  // Compare archive stems so a base release sorts before its
                  // patch releases: 2.23 < 2.23.1 < 2.23.2.
                  const std::string a_version = a.stem().string();
                  const std::string b_version = b.stem().string();
                  return strverscmp(a_version.c_str(), b_version.c_str()) < 0;
                });
    } else if (input_order == "lexicographic") {
      std::sort(input_files.begin(), input_files.end());
    } else if (input_order != "filesystem") {
      LOG(FATAL) << "Unknown input_order " << input_order
                 << "; expected filesystem, lexicographic, or natural_version";
    }
    for (const auto &path : input_files) {
      const auto file_path = path.string();
      LOG(INFO) << "start processing file " << file_path;
      compression->AddFile(file_path);
    }
  }

  else if (*task == "recover_single") {
    RecoverFile recover_file;
    recover_file.RecoverSingleFile();
  }

  else if (*task == "recover_all") {
    RecoverFile recover_file;
    recover_file.RecoverAllFiles();
  }

  else if (*task == "recover_random") {
    RecoverFile recover_file;
    recover_file.RecoverRandomChunks();
  }

  else if (*task == "verify_all") {
    RecoverFile recover_file;
    recover_file.VerifyAllFiles();
  } else {
    LOG(FATAL) << "Unknown task " << *task;
  }
  struct rusage usage {};
  if (getrusage(RUSAGE_SELF, &usage) == 0) {
    std::cout << "[memory] Process peak RSS: " << usage.ru_maxrss
              << " KB" << std::endl;
  }
  return 0;
}
