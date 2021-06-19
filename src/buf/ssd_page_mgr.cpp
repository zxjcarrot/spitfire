//
// Created by zxjcarrot on 2019-12-22.
//
#include "buf/buf_mgr.h"

namespace spitfire {

constexpr static int kDirectIOSize = 512;
// Currently supports 4TB database at max.
constexpr static int kSSDHeapFilesCap = 65536;
Status SSDPageManager::Init() {
    std::lock_guard<std::mutex> g(mtx);
    Status s = Status::OK();
    if (!PosixEnv::FileExists(db_path)) {
        s = PosixEnv::CreateDir(db_path);
        if (!s.ok())
            return s;
    }

    std::vector<std::string> children;
    PosixEnv::GetChildren(db_path, &children);
    for (int i = 0; i < children.size(); ++i) {
        if (Slice(children[i]).starts_with(kHeapFilePrefix)) {
            std::string suffix(children[i].begin() + kHeapFilePrefix.size(), children[i].end());
            int file_no = std::stoi(suffix);
            HeapFile *heap_file = new HeapFile(db_path + "/" + children[i], file_no, direct_io);
            Status s = heap_file->Init();
            if (!s.ok())
                return s;
            files.push_back(heap_file);
            max_file_no = std::max(max_file_no, file_no);
        }
    }

    sort(files.begin(), files.end(), [](const HeapFile * f1, const HeapFile * f2) {
        return f1->file_no < f2->file_no;
    });
    num_files = files.size();
    files.resize(kSSDHeapFilesCap);
    return Status::OK();
}

Status SSDPageManager::DestroyDB(const std::string &db_path) {
    Status s;
    if (!PosixEnv::FileExists(db_path)) {
        return s;
    }
    std::vector<std::string> children;
    PosixEnv::GetChildren(db_path, &children);

    for (int i = 0; i < children.size(); ++i) {
        if (Slice(children[i]).starts_with(kHeapFilePrefix)) {
            auto db_filepath = db_path + "/" + children[i];
            s = PosixEnv::DeleteFile(db_filepath);
            if (!s.ok())
                return s;
        }
    }
    return s;
}

Status SSDPageManager::ReadPage(pid_t pid, Page *p) {
    //std::lock_guard<std::mutex> g(mtx);
    uint32_t file_no = GetFileNo(pid);
    uint32_t off_in_file = GetFileOff(pid);
    assert(file_no < num_files);
    assert(files[file_no]->Allocated(off_in_file));
    assert(direct_io == false || (uint64_t)(char*)p % kDirectIOSize == 0);
    assert(direct_io == false || off_in_file % kDirectIOSize == 0);
    return files[file_no]->ReadPage(off_in_file, (void *) p, kPageSize);
    return Status::OK();
}

Status SSDPageManager::WritePage(pid_t pid, const Page *p) {
    //std::lock_guard<std::mutex> g(mtx);
    uint32_t file_no = GetFileNo(pid);
    uint32_t off_in_file = GetFileOff(pid);
    assert(file_no < num_files);
    assert(files[file_no]->Allocated(off_in_file));
    assert(direct_io == false || (uint64_t)(char*)p % kDirectIOSize == 0);
    return files[file_no]->WritePage(off_in_file, (const void *) p, kPageSize);
    return Status::OK();
}

size_t SSDPageManager::CountPages() {
    std::lock_guard<std::mutex> g(mtx);
    size_t n = 0;
    size_t nfiles = num_files;
    for (size_t i = 0; i < nfiles; ++i) {
        n += files[i]->CountPages();
    }
    return n;
}

std::vector<pid_t> SSDPageManager::GetAllocatedPids() {
    std::lock_guard<std::mutex> g(mtx);
    std::vector<pid_t> pids;
    for (int i = 0; i < num_files; ++i){
        files[i]->GetAllocatedPids(i, pids);
    }
    return pids;
}

// Allocates a page from SSD heap.
// Returns Status and pid
Status SSDPageManager::AllocateNewPage(pid_t &pid) {
    std::lock_guard<std::mutex> g(mtx);
    Status s;
    // Start from the file of last successful allocation
    // in the hope of finding an empty page slot more quickly.
    int i = last_allocated_from;
    for (; i < num_files; ++i) {
        uint32_t off_in_file = 0;
        s = files[i]->AllocateOnePage(off_in_file);
        if (s.ok()) {
            last_allocated_from = i;
            pid = MakePID(i, off_in_file);
            return Status::OK();
        } else if (s.IsHeapFileFull()) {
            continue;
        } else {
            return s;
        }
    }
    for (i = 0; i < last_allocated_from; ++i) {
        uint32_t off_in_file = 0;
        s = files[i]->AllocateOnePage(off_in_file);
        if (s.ok()) {
            last_allocated_from = i;
            pid = MakePID(i, off_in_file);
            return Status::OK();
        } else if (s.IsHeapFileFull()) {
            continue;
        } else {
            return s;
        }
    }
    if (num_files == kSSDHeapFilesCap) {
        std::cout << "The # database files exceeds " << kSSDHeapFilesCap << "!" << std::endl;
        exit(-1);
    }

    HeapFile *new_heap_file = nullptr;
    s = NewHeapFile(&new_heap_file);
    if (!s.ok())
        return s;

    int new_num_files = ++num_files;
    files[new_num_files - 1] = (new_heap_file);

    uint32_t off_in_file = 0;
    s = files[new_num_files - 1]->AllocateOnePage(off_in_file);
    if (!s.ok()) {
        return s;
    }

    pid = MakePID(new_num_files - 1, off_in_file);
    return s;
}

// Free a page given pid
Status SSDPageManager::FreePage(const pid_t pid) {
    std::lock_guard<std::mutex> g(mtx);
    uint32_t file_no = GetFileNo(pid);
    uint32_t off_in_file = GetFileOff(pid);
    assert(file_no < num_files);
    if (file_no >= num_files)
        return Status::InvalidArgument("pid " + std::to_string(pid) + " is invalid");
    assert(files[file_no]->Allocated(off_in_file));
    return files[file_no]->DeallocateOnePage(off_in_file);
}

bool SSDPageManager::Allocated(const pid_t pid) {
    std::lock_guard<std::mutex> g(mtx);
    uint32_t file_no = GetFileNo(pid);
    uint32_t off_in_file = GetFileOff(pid);
    assert(file_no < num_files);
    if (file_no >= num_files)
        return false;
    return files[file_no]->Allocated(off_in_file);
}

Status SSDPageManager::HeapFile::Init() {
    assert(bitmap == nullptr);
    char * ptr;
    auto res = posix_memalign((void**)&ptr, kDirectIOSize, kFileBitMaskSizeInBytes);
    if (res) {
        return PosixError(heapfile_path, errno);
    }
    bitmap = new(ptr) BitMap<kFileBitMaskSizeInBytes>;

    Status s;
    if (PosixEnv::FileExists(heapfile_path)) {
        s = PosixEnv::OpenRWFile(heapfile_path, fd, direct_io);
        if (!s.ok())
            return s;
    } else {
        s = PosixEnv::CreateRWFile(heapfile_path, fd, direct_io);
        if (!s.ok())
            return s;
        s = PosixEnv::Fallocate(fd, 0, kFileSize);
        if (!s.ok())
            return s;
    }

    s = PosixEnv::GetFileSize(heapfile_path, &fsize);
    if (!s.ok())
        return PosixError(heapfile_path, errno);
    if (fsize != kFileSize)
        return Status::Corruption(heapfile_path);
    assert(bitmap->LengthInBytes() == kPageSize);

    return ReadPage(kBitmapOffset, bitmap->Data(), bitmap->LengthInBytes());
}

// Allocate one free page from the heap file
Status SSDPageManager::HeapFile::AllocateOnePage(uint32_t &off) {
    int n = bitmap->FirstNotSet();
    if (n < kPagesPerFile) {
        bitmap->Set(n);
        off = n * kPageSize;
        return SyncBitmap(bitmap->BitOffToByteOff(n), 1);
    }
    return Status::HeapFileFull(heapfile_path);
}

Status SSDPageManager::HeapFile::SyncBitmap(uint32_t byte_off, size_t bytes) {
    assert(kDirectIOSize && ((kDirectIOSize & (kDirectIOSize - 1)) == 0));

    // Round down to the closest 512-byte-aligned boundary.
    int diff = byte_off % kDirectIOSize;
    assert(diff >= 0);
    byte_off -= diff;
    // Round up to the closest 512-byte-aligned boundary.
    //bytes += diff;
    //bytes = ((bytes + kDirectIOSize - 1) / kDirectIOSize) * kDirectIOSize;
    bytes = kDirectIOSize;
    return PosixEnv::PWrite(fd, kBitmapOffset + byte_off, bitmap->Data() + byte_off, bytes);
    //return Status::OK();
}

Status SSDPageManager::HeapFile::DeallocateOnePage(uint32_t off) {
    off = (uint64_t) DO_ALIGN(off, kPageSize);
    bitmap->Clear(off / kPageSize);
    return SyncBitmap(bitmap->BitOffToByteOff(off), 1);
}

bool SSDPageManager::HeapFile::Allocated(uint32_t off) {
    off = (uint64_t) DO_ALIGN(off, kPageSize);
    int bit_pos = off / kPageSize;
    return bitmap->Test(bit_pos);
}

Status SSDPageManager::HeapFile::ReadPage(uint32_t off, void *buf, size_t buf_size) {
    return PosixEnv::PRead(fd, off, buf, buf_size);
}

Status SSDPageManager::HeapFile::WritePage(uint32_t off, const void *buf, size_t buf_size) {
    return PosixEnv::PWrite(fd, off, buf, buf_size);
}

size_t SSDPageManager::HeapFile::CountPages() {
    return bitmap->Count();
}


void SSDPageManager::HeapFile::GetAllocatedPids(int file_no, std::vector<pid_t> & res) {
    std::vector<int> bits;
    bitmap->Count(bits);
    for (int i = 0; i < bits.size(); ++i) {
        res.push_back(MakePID(file_no, bits[i] << kPageSizeBits));
    }
}

Status SSDPageManager::NewHeapFile(HeapFile **heap_file) {
    int file_no = max_file_no + 1;
    std::string new_heapfile_path = db_path + "/" + kHeapFilePrefix + std::to_string(file_no);
    HeapFile *file = new HeapFile(new_heapfile_path, file_no, direct_io);
    Status s = file->Init();
    if (!s.ok())
        return s;
    max_file_no++;
    *heap_file = file;
    return Status::OK();
}

}