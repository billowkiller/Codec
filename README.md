lzo、snappy、gzip压缩方法.

usage: 

    // 压缩文件
    CodecFactory::CodecInstance("lzo")->CompressFile("test.txt");

    //流式压缩
    std::string temp;
    CodecFactory::CodecPtr compressor = CodecFactory::CodecInstance("lzo");
    compressor->Init();
    while ((int in_len = read(buffer))!= 0) {
        compressor->Compress(buffer, in_len, &temp);
        handle(temp);     //写文件或网络传输
    }
    compressor->Close(temp);
    handle(temp);
