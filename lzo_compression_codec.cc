/*
 * =====================================================================================
 *
 *       Filename:  lzo_compression_codec.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  09/14/2015 01:41:32 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  billowkiller (), billowkiller@gmail.com
 *   Organization:  
 *
 * =====================================================================================
 */

#include "compression_codec.h"

// magic number, from com.hadoop.compression.lzo.LzoCodec
const unsigned char LZOCompressionCodec::magic[] = 
    { 0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a };
const lzo_uint32 LZOCompressionCodec::INITCHECKSUM = lzo_adler32(0, NULL, 0);

LZOCompressionCodec::LZOCompressionCodec()
    : block_size(256 * 1024L), header_checksum(INITCHECKSUM), CompressionCodec("lzo") {
    memset(&header, 0, sizeof(header_t));
    header.version = LZOP_VERSION & 0xffff;
    header.version_needed_to_extract = 0x0940;
    header.lib_version = lzo_version() & 0xffff;
    header.method = (unsigned char) 1;
    header.level = (unsigned char) 3;
    header.filter = 0;
    header.flags = F_ADLER32_D | F_ADLER32_C;
    header.mode = 0;
    header.mtime_low = 0;
    header.mtime_high = 0;
}

bool LZOCompressionCodec::Init() {
    return true;
}

void LZOCompressionCodec::byte_write(size_t size, uint32_t v, FILE *fp) {
    unsigned char b[size];
    for (int i = 0; i < size; i++) {
        b[size-i-1] = (unsigned char) ((v >> (i * 8)) & 0xff);
    }
    xwrite(b, size, fp);
    header_checksum = lzo_adler32(header_checksum, b, size);
}

void LZOCompressionCodec::byte_write(size_t size, uint32_t v, unsigned char *out) {
    unsigned char b[size];
    for (int i = 0; i < size; i++) {
        b[size-i-1] = (unsigned char) ((v >> (i * 8)) & 0xff);
    }
    memcpy(out, b, size);
    header_checksum = lzo_adler32(header_checksum, b, size);
}

void LZOCompressionCodec::byte_write(size_t size, uint32_t v, unsigned char *out, size_t &pos) {
    byte_write(size, v, out + pos);
    pos += size;
}

bool LZOCompressionCodec::CompressFile(const std::string &path) {
    FILE *fi = xopen(path, "rb");
    FILE *fo = xopen(path + ".lzo", "wb");
/*
 * Step 1: write header
 */
    xwrite(magic, sizeof(magic), fo);
    byte_write(2, header.version, fo);
    byte_write(2, header.lib_version, fo);
    byte_write(2, header.version_needed_to_extract, fo);
    byte_write(1, header.method, fo);
    byte_write(1, header.level, fo);
    byte_write(4, header.flags, fo);
    byte_write(4, header.mode, fo);
    byte_write(4, header.mtime_low, fo);
    byte_write(4, header.mtime_high, fo);
    byte_write(1, 0, fo); //file name length
    byte_write(4, header_checksum, fo);

/*
 * Step 2: allocate compression buffers and work-memory
 */
    unsigned char *in = new(std::nothrow) unsigned char[block_size];
    if (in == NULL) {
        return false;
    }
/*
 * Step 3: process blocks
 */
    while (true) {
        /* read block */
        lzo_uint in_len = fread(in, 1, block_size, fi);
        if (in_len == 0) {
            break;
        }
        /* compress block */
        std::string temp;
        bool succ  = Compress(in, in_len, &temp);
        xwrite((const unsigned char *)temp.c_str(), temp.size(), fo);
        if (!succ) {
            delete[] in;
            fclose(fi);
            fclose(fo);
            return false;
        }
    }
    /* write EOF marker */
    byte_write(4, 0, fo);
    delete[] in;
    fclose(fi);
    fclose(fo);
    return true;
}

bool LZOCompressionCodec::Compress(unsigned const char* in, const size_t in_len, std::string* out){
    unsigned char *wrkmem = new(std::nothrow) unsigned char[LZO1X_1_MEM_COMPRESS];
    if (wrkmem == NULL) {
        return false;
    }
    out->resize(OUTSIZE(in_len)+16);
    size_t out_len = 0;
    unsigned char *out_buffer = (unsigned char *)string_as_array(out);
    int r = lzo1x_1_compress(in, in_len, 16 + out_buffer, &out_len, wrkmem);
    if (r != LZO_E_OK || out_len > OUTSIZE(in_len)) {
        /* this should NEVER happen */
        delete[] wrkmem;
        return false;
    }
    size_t pos = 0;
    /* write uncompressed block size */
    byte_write(4, in_len, out_buffer, pos);
    if (out_len < in_len) {
        /* write compressed block size */
        byte_write(4, out_len, out_buffer, pos);
        /* checksum */
        byte_write(4, lzo_adler32(INITCHECKSUM, in, in_len), out_buffer, pos);
        byte_write(4, lzo_adler32(INITCHECKSUM, 16 + out_buffer, out_len), out_buffer, pos);
    } else {
        /* not compressible - write uncompressed block */
        byte_write(4, in_len, out_buffer, pos);
        /* checksum */
        byte_write(4, lzo_adler32(INITCHECKSUM, in, in_len), out_buffer, pos);
        byte_write(4, lzo_adler32(INITCHECKSUM, 16 + out_buffer, out_len), out_buffer, pos);
    }
    out->resize(16 + out_len);
    delete[] wrkmem;
    return true;
}

void LZOCompressionCodec::Close(std::string *out) {
    /* write EOF marker */
    out->resize(4);
    byte_write(4, 0, (unsigned char *)string_as_array(out));
}
