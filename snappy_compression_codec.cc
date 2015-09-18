/*
 * =====================================================================================
 *
 *       Filename:  snappy_compression_codec.cc
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

//const std::string SnappyCompressionCodec::suffix = "snappy";

SnappyCompressionCodec::SnappyCompressionCodec() : block_size(16384), CompressionCodec("snappy") {

}

bool SnappyCompressionCodec::Init() {
    return true;
}

bool SnappyCompressionCodec::CompressFile(const std::string &path) {
    FILE *fi = xopen(path, "rb");
    FILE *fo = xopen(path + ".snappy", "wb");

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
    delete[] in;
    fclose(fi);
    fclose(fo);
    return true;
}

bool SnappyCompressionCodec::Compress(const unsigned char* in, 
                                      const size_t in_len, 
                                      std::string* out) {
    size_t size = 0;
    out->resize(snappy::MaxCompressedLength(in_len));
    snappy::RawCompress((const char*)in, in_len, string_as_array(out), &size);
    out->resize(size);
    return true;
}

void SnappyCompressionCodec::Close(std::string *out) {

}
