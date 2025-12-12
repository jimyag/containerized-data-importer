# Zstd 解压性能优化设计文档

## 背景

CDI importer 使用 Go 实现的 zstd 库（github.com/klauspost/compress/zstd）进行解压，
在处理大型稀疏镜像时存在性能瓶颈：

- Go zstd 解压速度：~300 MB/s
- C zstd 解压速度：~1.4 GB/s
- 性能差距：约 5 倍

对于一个 19GB 的 raw.zst 镜像（实际数据 2.5GB），100% 下载完成后仍需约 45 秒处理剩余解压。

## 方案对比

| 维度 | 方案A：稀疏映射数据源 | 方案B：系统 zstd 解压 |
|------|---------------------|-------------------|
| 改动范围 | 上传工具 + CDI importer | 仅 CDI importer |
| 需要解压数据量 | 仅非零数据（如 2.5GB） | 全部数据（如 19GB） |
| 解压速度 | Go zstd ~300 MB/s | C zstd ~1.4 GB/s |
| 预计总时间 | ~8 秒 | ~14 秒 |
| 兼容性 | 需要新格式 | 兼容现有 raw.zst |
| 依赖 | 无额外依赖 | 需要安装 zstd 命令 |
| 复杂度 | 中等 | 低 |

---

## 方案A：稀疏映射数据源（长期方案）

### 概述

在压缩时记录稀疏映射，解压时只处理非零数据块。

### Map 文件格式

#### JSON 格式（便于调试）

```json
{
  "version": 1,
  "virtual_size": 20971520000,
  "block_size": 4194304,
  "data_ranges": [
    {"offset": 0, "length": 4194304},
    {"offset": 104857600, "length": 8388608},
    {"offset": 2684354560, "length": 4194304}
  ]
}
```

#### 二进制格式（更紧凑）

```
Header (32 bytes):
  - Magic:        4 bytes  "SMAP"
  - Version:      4 bytes  uint32
  - VirtualSize:  8 bytes  uint64
  - BlockSize:    4 bytes  uint32
  - RangeCount:   4 bytes  uint32
  - Reserved:     8 bytes

Ranges (16 bytes each):
  - Offset:       8 bytes  uint64
  - Length:       8 bytes  uint64
```

### 文件命名约定

```
disk.raw.sparse.zst     # 压缩的非零数据
disk.raw.sparse.map     # 稀疏映射文件
```

### 上传工具修改

```go
func uploadWithSparseMap(device string, blockSize int64) error {
    file, _ := os.Open(device)
    defer file.Close()

    var ranges []DataRange
    zstdWriter := newZstdWriter(uploadStream)

    buf := make([]byte, blockSize)
    offset := int64(0)

    for {
        n, err := file.Read(buf)
        if n > 0 {
            if !isZeroBlock(buf[:n]) {
                // 记录非零块位置
                ranges = append(ranges, DataRange{
                    Offset: offset,
                    Length: int64(n),
                })
                // 只压缩非零数据
                zstdWriter.Write(buf[:n])
            }
            offset += int64(n)
        }
        if err == io.EOF {
            break
        }
    }
    zstdWriter.Close()

    // 上传 map 文件和压缩数据
    return uploadFiles(ranges, offset)
}
```

### CDI Importer 处理

```go
func StreamSparseDataToFile(mapReader, dataReader io.Reader, fileName string) error {
    // 1. 解析 map 文件
    sparseMap, _ := parseSparseMap(mapReader)

    // 2. 创建稀疏文件
    outFile, _ := os.Create(fileName)
    outFile.Truncate(sparseMap.VirtualSize)

    // 3. 解压并按 map 写入
    zstdReader, _ := zstd.NewReader(dataReader)
    defer zstdReader.Close()

    for _, r := range sparseMap.DataRanges {
        outFile.Seek(r.Offset, io.SeekStart)
        io.CopyN(outFile, zstdReader, r.Length)
    }

    return outFile.Sync()
}
```

### 优点

- 性能最优，只处理实际数据
- 不依赖外部命令
- 精确控制写入位置

### 缺点

- 需要修改上传工具
- 不兼容现有镜像
- 开发工作量较大

---

## 方案B：系统 zstd 解压（短期方案）

### 概述

使用系统安装的 zstd 命令行工具（C 实现）替代 Go 库进行解压。

### 实现原理

```
HTTP Stream → stdin → [zstd -d -c] → stdout → 稀疏检测 → 文件写入
```

### 代码实现

```go
// 使用系统 zstd 命令解压
func (fr *FormatReaders) zstReaderCmd() (io.ReadCloser, error) {
    cmd := exec.Command("zstd", "-d", "-c")
    cmd.Stdin = fr.TopReader()

    stdout, err := cmd.StdoutPipe()
    if err != nil {
        return nil, err
    }

    if err := cmd.Start(); err != nil {
        return nil, err
    }

    return &cmdReadCloser{
        Reader: bufio.NewReaderSize(stdout, 8<<20), // 8MB 缓冲
        cmd:    cmd,
    }, nil
}

type cmdReadCloser struct {
    io.Reader
    cmd *exec.Cmd
}

func (c *cmdReadCloser) Close() error {
    return c.cmd.Wait()
}
```

### 镜像修改

在 importer 镜像的 Dockerfile 中添加 zstd：

```dockerfile
# CentOS Stream 9
RUN dnf install -y zstd && dnf clean all
```

### 优点

- 兼容现有镜像格式
- 改动小，快速实现
- 利用 C 实现的高性能

### 缺点

- 仍需解压全部数据
- 依赖外部命令
- 进程间通信有少量开销

---

## 实施计划

1. **阶段一**：实现方案B（短期）
   - 修改 format-readers.go，添加系统 zstd 支持
   - 修改 Dockerfile，安装 zstd
   - 预计性能提升：45 秒 → ~14 秒

2. **阶段二**：实现方案A（长期）
   - 定义稀疏映射格式规范
   - 修改上传工具，生成 map 文件
   - 在 CDI 中添加新数据源类型
   - 预计性能提升：14 秒 → ~8 秒

---

## 参考

- [klauspost/compress zstd](https://github.com/klauspost/compress/tree/master/zstd)
- [zstd 官方文档](https://facebook.github.io/zstd/)
