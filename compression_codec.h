#ifndef __COMPRESSION_CODEC_H__
#define __COMPRESSION_CODEC_H__

#include <string>
#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "zlib.h"
#include "snappy.h"
#include <boost/shared_ptr.hpp>

class CompressionCodec {
protected:
    FILE* xopen(const std::string &name, const char *mode) {
        FILE *fp = fopen(name.c_str(), mode);
        if (fp == NULL) {
            exit(1);
        }
        return fp;
    }
    void xwrite(const unsigned char *buf, uint32_t len, FILE *fp) {
        if (fp != NULL && fwrite(buf, 1, len, fp) != len) {
            exit(1);
        }
    }
    void xwrite(const unsigned char *buf, uint32_t len, unsigned char *out) {
        memcpy(out, buf, len);
    }
    char* string_as_array(std::string* str) {
        return str->empty() ? NULL : &*str->begin();
    }
    
    const std::string suffix;

public:
    CompressionCodec(const char *suffix_str) : suffix(suffix_str) {}
    virtual std::string Suffix() { return suffix; }
    virtual bool Init() = 0;
    virtual bool CompressFile(const std::string &path) = 0;
    virtual bool Compress(const unsigned char* in, const size_t in_len, std::string* out) = 0;
    virtual void Close(std::string *out) = 0;
};

class LZOCompressionCodec : public CompressionCodec {
private:
    typedef struct {
        unsigned version;
        unsigned lib_version;
        unsigned version_needed_to_extract;
        unsigned char method;
        unsigned char level;
        lzo_uint32 flags;
        lzo_uint32 filter;                    // filter is not supported
        lzo_uint32 mode;
        lzo_uint32 mtime_low;
        lzo_uint32 mtime_high;
        unsigned char filename_len;
    } header_t;

    static const unsigned char magic[9];
    static const lzo_uint32 LZOP_VERSION = 0x1030;
    static const lzo_uint32 F_ADLER32_D = 0x00000001L;
    static const lzo_uint32 F_ADLER32_C = 0x00000002L;
    static const int method = 1;         /* compression method: LZO1X */
    static const lzo_uint32 INITCHECKSUM;
    
    header_t header;
    lzo_uint32 header_checksum;
    size_t block_size;
private:
    void inline byte_write(size_t size, uint32_t v, FILE *fp);
    void inline byte_write(size_t size, uint32_t v, unsigned char *out);
    void inline byte_write(size_t size, uint32_t v, unsigned char *out, size_t& pos);
    const uint32_t OUTSIZE(uint32_t in_size) const {
        return in_size + in_size / 16 + 64 + 3;    // from lzo example lzopack.c
    }
public:
    LZOCompressionCodec();
    bool Init();
    bool CompressFile(const std::string &path);
    bool Compress(const unsigned char* in, const size_t in_len, std::string* out);
    void Close(std::string *out);
};

// modified from zlib's zpipe.c
class GzipCompressionCodec : public CompressionCodec {
private:
    
    size_t block_size;
    z_stream comp_stream_;    // Zlib stream data structure
    int compression_level_;   // compression level
private:
    const uint32_t OUTSIZE(uint32_t in_size) const {
        return in_size + in_size / 1000 + 40;       // from snappy test file.
    }
public:
    const std::string suffix;
    GzipCompressionCodec();
    bool Init();
    bool CompressFile(const std::string &path);
    bool Compress(const unsigned char* in, const size_t in_len, std::string* out);
    void Close(std::string *out);
};

class SnappyCompressionCodec : public CompressionCodec {
private:
    size_t block_size;
public:
    const std::string suffix;
    SnappyCompressionCodec();
    bool Init();
    bool CompressFile(const std::string &path);
    bool Compress(const unsigned char* in, const size_t in_len, std::string* out);
    void Close(std::string *out);
};

class CodecFactory {
public:
    typedef boost::shared_ptr<CompressionCodec> CodecPtr;
    static CodecPtr CodecInstance(const std::string &codec) {
        if (0 == codec.compare("lzo")) {
            return CodecPtr(new LZOCompressionCodec);
        } else if (0 == codec.compare("gz")) {
            return CodecPtr(new GzipCompressionCodec);
        } else if (0 == codec.compare("snappy")) {
            return CodecPtr(new SnappyCompressionCodec);
        } else {
            return CodecPtr();
        }
    }
};
#endif
