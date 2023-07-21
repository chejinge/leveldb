// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::SkipToInitialBlock() {
  //计算在block内的偏移位置，并圆整到开始读取block的起始位置
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;
   //// 如果偏移在最后的6byte里，肯定不是一条完整的记录，跳到下一个block
  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }
  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  //设置读取偏移
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);// 跳转
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  if (last_record_offset_ < initial_offset_) {
    //当前偏移量是否小于指定的偏移
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  //在开始while循环前首先初始化几个标记：
  //当前是否在in_fragmented_record内
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;//标记我们正在读去的逻辑偏移量

  Slice fragment;
  //如果buffer_小于block header大小kHeaderSize
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    //physical_record_offset存储的是当前正在读取的偏移量的
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();
    //根据record_type的类型分别处理
    if (resyncing_) {
      if (record_type == kMiddleType) {//
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType://表明是一条完整的log record，成功返回读取的user record数据。另外需要对早期版本做些work around，
                     // 早期的Leveldb会在block的结尾生产一条空的kFirstType log record
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {// 清空scratch，读取成功不需要返回scratch数据
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;// 更新last record offset
        return true;

      case kFirstType://表明是一系列logrecord(fragment)的第一个record。同样需要对早期版本做work around。
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        //赋值给scratch
        scratch->assign(fragment.data(), fragment.size());
        // 设置fragment标记为true
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {//判断是否在fragment中如果不在
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");//直接报错
        } else {
          scratch->append(fragment.data(), fragment.size());//否则直接append到scratch中
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {//判断是否在fragment中如果不在
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");//直接报错
        } else {
          scratch->append(fragment.data(), fragment.size());//否则直接append到scratch中
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;//记录最后一条offset
          return true;
        }
        break;

      case kEof://遇到文件末尾，返回flase
        if (in_fragmented_record) {//在fragment中存在
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();//清空scratch
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        //报告错误
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;// 重置fragment标记
        scratch->clear();// 清空scratch
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    //如果buffer_小于block header大小kHeaderSize进入下面的处理逻辑中
    if (buffer_.size() < kHeaderSize) {
      //s1 如果eof_为false，表明还没有到文件结尾，清空buffer，并读取数据。
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();//清空buffer
        // 因为上次肯定读取了一个完整的record
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);//读取数据
        end_of_buffer_offset_ += buffer_.size();
        // 更新buffer读取偏移值
        if (!status.ok()) {
          //读取错误，清空buffer
          buffer_.clear();
          ReportDrop(kBlockSize, status);//report报错
          eof_ = true;//设置eof为true
          return kEof;//return eof
        } else if (buffer_.size() < kBlockSize) {// 实际读取字节<指定(Block Size)，表明到了文件结尾
          eof_ = true;//设置eof为true
        }
        continue;// 继续下次循环
      } else {//如果eof_为true并且buffer为空，表明已经到了文件结尾，正常结束，返回kEof
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();//清空buffer
        return kEof;
      }
    }
    //进入到这里表明上次循环中的Read读取到了一个完整的log record，
    // continue后的第二次循环判断buffer_.size() >= kHeaderSize将执行到此处。
    // Parse the header 解析header 判断长度是否一致
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;//根据log的格式，前4byte是crc32
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;//后面就是length和type
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {//长度超出了，汇报错误
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;//返回BadRecord
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      //对于zero类型的错误，不汇报错误
      buffer_.clear();
      return kBadRecord;//返回BadRecord
    }

    // Check crc
    //校验CRC32，如果校验出错，则汇报错误，并返回kBadRecord。
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }
    //如火record的开始位置在initinal offset之前，则跳过，并返回kBadRecord
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);//否则返回record数据和type
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
