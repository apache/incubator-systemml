
#include "spoof_native_cuda.h"

#include <filesystem>
#include <iostream>
#include <cstdlib>

size_t SpoofCudaContext::initialize_cuda(uint32_t device_id) {
  std::cout << "initializing cuda device " << device_id << std::endl;

  SpoofCudaContext *ctx = new SpoofCudaContext();
  // cuda device is handled by jCuda atm
  //cudaSetDevice(device_id);
  //cudaSetDeviceFlags(cudaDeviceScheduleBlockingSync);
  //cudaDeviceSynchronize();
  return reinterpret_cast<size_t>(ctx);
}

void SpoofCudaContext::destroy_cuda(SpoofCudaContext *ctx, uint32_t device_id) {
  //std::cout << "destroying cuda context " << ctx << " of device " << device_id
            //<< std::endl;
  delete ctx;
  ctx = nullptr;
  // cuda device is handled by jCuda atm
  //cudaDeviceReset();
}

bool SpoofCudaContext::compile_cuda(const std::string &src,
                                    const std::string &name) {
  std::cout << "compiling cuda kernel " << name << std::endl;
  std::cout << src << std::endl;

  //std::cout << "cwd: " << std::filesystem::current_path() << std::endl;

  std::string cuda_path = std::string("-I") + std::getenv("CUDA_PATH") + "/include";
  //std::cout << "cuda_path: " << cuda_path << std::endl;

  SpoofOperator::AggType type = SpoofOperator::AggType::NONE;
  SpoofOperator::AggOp op = SpoofOperator::AggOp::NONE;

  auto pos = 0;
  if((pos = src.find("CellType")) != std::string::npos) {
      if(src.substr(pos, pos+30).find("FULL_AGG") != std::string::npos)
          type = SpoofOperator::AggType::FULL_AGG;
      else if(src.substr(pos, pos+30).find("ROW_AGG") != std::string::npos)
          type = SpoofOperator::AggType::ROW_AGG;
      else if(src.substr(pos, pos+30).find("COL_AGG") != std::string::npos)
          type = SpoofOperator::AggType::COL_AGG;
      else if(src.substr(pos, pos+30).find("NO_AGG") != std::string::npos)
          type = SpoofOperator::AggType::NO_AGG;
      else
          std::cout << "error: unknown aggregation type" << std::endl;

      if(type != SpoofOperator::AggType::NO_AGG)
          if((pos = src.find("AggOp")) != std::string::npos) {
              if(src.substr(pos, pos+30).find("AggOp.SUM") != std::string::npos)
                  op = SpoofOperator::AggOp::SUM;
              else if(src.substr(pos, pos+30).find("AggOp.SUM_SQ") != std::string::npos)
                  op = SpoofOperator::AggOp::SUM_SQ;
              else if(src.substr(pos, pos+30).find("AggOp.MIN") != std::string::npos)
                  op = SpoofOperator::AggOp::MIN;
              else if(src.substr(pos, pos+30).find("AggOp.MAX") != std::string::npos)
                  op = SpoofOperator::AggOp::MAX;
              else
                  std::cout << "error: unknown aggregation operator" << std::endl;
          }
  }

  // ToDo: cleanup cuda path 
  jitify::Program program = kernel_cache.program(
      src, 0,
      {"-I./src/main/cpp/kernels/spoof_native_cuda/", 
      "-I./src/main/cpp/kernels/",
       "-I/usr/local/cuda/include",
       "-I/usr/local/cuda/include/cuda/std/detail/libcxx/include/", 
      cuda_path});

  // ToDo: agg types
  ops.insert(std::make_pair(
      name, SpoofOperator({std::move(program), type, op})));

  return true;
}
