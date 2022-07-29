package sstabledemo

// 整体的构建流程
/*
	首先是向block中添加k-v对，同时也会向filterBuffer中添加key，
	当block中的数量到达一定的阈值时就会持久化为磁盘Block，同时会将filterBuffer添加到filterBlock中；
	当所有的的Blocker都持久化后，整个FilterBlock也构建完成了，再将filterBlock持久化；
	同时DataBlock的信息会构建为IndexBlock，FilterBlock的信息会构建为Meta Index Block；
	然后将IndexBlock和MetaIndexBlock 加上缓冲的空区域Padding 和 校验数MagicNumber 组装为Footer
*/

// Entry：KV对
/*
	将k-v的长度和k-v封装为Entry
	K-V采用变长变量存储
*/

// Block：以数组的形式排列Entry
/*
	把Entry以数据的方式排列
	提升查询效率
	减少随机读
*/

// 前缀压缩减少空间放大
/*
	因为Key是有序的
	相邻的两个key重合度会比较大，可以通过共享的方式来存储
	只有block中第一个key会保留全量key，后续的entry只保存非共享部分的key
	减少空间放大
*/

// 分组前缀压缩，组间二分查找
/*
	前缀压缩使得Entry不能直接被解析
	无法通过二分查找的方式加速查找
	对Block中的entry进行分组，每十六个一组，组内进行前缀压缩减少空间放大
	每组的第一个entry保留全量key
	每组之间进行二分查找，组内顺序遍历
	同时需要一个Restart Point指向每一个Group
*/

// 一个完整的SSTable
/*
	存储k-v的block是DataBlock
	存储索引DataBlock索引信息的Block是Index Block
	每持久化一个DataBlcok就像IndexBlock中写入一个<key,DataBlockHandle>
	DataBlock全部持久化完成后IndexBlock构建完成，
	持久化IndexBlock获得IndexBlockHandle
	将IndexBlockHandle写入固定大小的Footer
	持久化Footer
*/

// 加入bloomfilter
/*
	每次向DataBlcok写入entry时都会向filterBuffer中写入key
	一个DataBlock写入持久化完成后，将FilterBuffer数据写入到FilterBlock；
	所有的datablock持久化完成，FilterBlock也会构建完成
	持久化FilterBlock获得FilterBlockHandle
	将 <filterName，FilterBlockHandle>写入到MetaIndexBlock(考虑到以后可能会使用其他不同的过滤器)
	持久化MetaIndexFilter，得到MetaBlockHandle，并写入到Footer中
	持久化Footer
*/

// 读取流程
/*
	首先读取固定大小的Footer，将IndexBlock和MetaIndexBlock读取出来，根据MetaIndexBlock的信息读取BloomFilter；
	根据要查询的key在IndexBlock中查询到在哪个Block中，然后根据BlockOffset获得BlockBloomFiter；
	在BlockBloomFilter中判断是否存在，如果不存在直接返回false，
	如果存在就将磁盘中的对应的Block读取到内存中，然后在Block中查找得到Value
*/
