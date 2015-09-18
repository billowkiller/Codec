/*
 * =====================================================================================
 *
 *       Filename:  gzip_compression_codec.cc
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

//const std::string GzipCompressionCodec::suffix = "gz";

GzipCompressionCodec::GzipCompressionCodec() : block_size(16384), CompressionCodec("gz") {

}

bool GzipCompressionCodec::Init() {
    comp_stream_.zalloc = Z_NULL;
    comp_stream_.zfree = Z_NULL;
    comp_stream_.opaque = Z_NULL;
    int ret = deflateInit2(&comp_stream_, Z_DEFAULT_COMPRESSION, 
                           Z_DEFLATED, MAX_WBITS+16, 8, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) {
        return false;
    }
    return true;
}

bool GzipCompressionCodec::CompressFile(const std::string &path) {
    if (!Init()) {
        return false;
    }
    FILE *fi = xopen(path, "rb");
    FILE *fo = xopen(path + ".gz", "wb");

    unsigned char *in = new(std::nothrow) unsigned char[block_size];
    if (in == NULL) {
        return false;
    }
    while (true) {
        /* read block */
        size_t in_len = fread(in, 1, block_size, fi);
        if (in_len == 0) {
            break;
        }
        /* compress block */
        std::string temp;
        bool succ  = Compress(in, in_len, &temp);
        if (!succ) {
            delete[] in;
            fclose(fi);
            fclose(fo);
            return false;
        }
        xwrite((const unsigned char *)temp.c_str(), temp.size(), fo);
    }
    std::string temp;
    Close(&temp);
    xwrite((const unsigned char *)temp.c_str(), temp.size(), fo);
    delete[] in;
    fclose(fi);
    fclose(fo);
    return true;
}

bool GzipCompressionCodec::Compress(const unsigned char* in, 
                                    const size_t in_len, 
                                    std::string* out) {
    out->resize(OUTSIZE(in_len));
    comp_stream_.next_in = const_cast<Bytef*>(in);
    comp_stream_.avail_in = in_len;
    comp_stream_.avail_out = OUTSIZE(in_len);
    comp_stream_.next_out = (Bytef*)string_as_array(out);
    int ret = deflate(&comp_stream_, Z_NO_FLUSH);
    //assert(ret != Z_STREAM_ERROR);
    switch (ret) {
        case Z_NEED_DICT:
        case Z_ERRNO:
        case Z_STREAM_ERROR:
        case Z_VERSION_ERROR:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            (void)deflateEnd(&comp_stream_);
            return false;
    }
    out->resize(OUTSIZE(in_len) - comp_stream_.avail_out);
    return true;
}

void GzipCompressionCodec::Close(std::string *out) {
    out->resize(block_size);
    comp_stream_.avail_in = 0;
    comp_stream_.avail_out = block_size;
    comp_stream_.next_out = (Bytef*)string_as_array(out);
    int ret = deflate(&comp_stream_, Z_FINISH);
    (void)deflateEnd(&comp_stream_);
    out->resize(block_size - comp_stream_.avail_out);
}
