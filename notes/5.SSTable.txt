一、简介
	· 先导知识：
		1. 可变长编码
		2. mmap
		3. 策略模式
		4. 迭代器模式
		5. 布隆过滤器
		6. 二分查找
	
	· 介绍：
		sst文件是LSM用于存储持久化kv的文件，其设计充分考虑了持久化、读写性能、存储空间三者的权衡，
		经过本节可以学习到当今主流Nosql数据库的文件存储格式的设计思想和方法，并且理解一个计算机领域重要应用之一：
		也就是 KV数据结构是如何存储到磁盘上的？ 也可以说 内存数据结构是如何序列化到磁盘上的；


二、接口设计
	· 首先需要考虑SSTable的应用场景是什么？会在什么情况下使用？
	  可以划分为四个使用场景：1.检索；2.初始化；3.序列化；4.创建；

	· 创建：
		· 当向KV引擎put数据时，会检查当前memtable是否达到指定的阈值，如果达到了阈值就会触发Flush行为；
		  Flush行为会将整个skiplist的数据序列化到磁盘上的sst文件上；
		  因此需要一个open()函数，打开一个sst文件，并且将需要的数据序列化，然后将序列化的数据写入到sst文件中；
			{func openTable(option,tableName,skiplist) -> table}

		· 序列化的过程是相对复杂的，为了合理的组织代码，并且可预测未来sst的存储格式会经常变化，例如 在后续的merge的过程中需要Flush两个stt文件；
		  可以引入一个builder对象来处理将skiplist的数据序列化为sst格式逻辑(策略模式)，代替直接传递skiplist对象；
		  也就是说，在openTable这里传入的对象做了一个抽象，openTable不需要具体考虑需要处理的数据类型是什么，只需要事先对builder的flaush，所有的数据结构都会事先被转化为builder对象
		  	{func openTable(option,tableName,builder) -> table}
	
	· 初始化：
		· 如果KV引擎不是第一次启动，应该要在指定的工作目录下去加载所有的sst后缀的文件，然后注意初始化，恢复索引等结构用于检索；
		  这个过程的接口依然可以复用openTable()，只要判断builder为nil，在打开tableName存在的情况下就可以进行初始化；
			{func openTable}

	· 检索：
		· 首先sst文件需要识别key的版本大小(时间戳)，因为可能存在指定版本的点/范围查询
			{func Search(key,maxVersion) -> kv}
		
		· 同时需要支持繁琐的范围查询，指定版本、前缀、升序、降序等范围查询，并且设计在内存、磁盘等多种数据结构上的统一查询接口，所以可以使用迭代器模式来封装；
		  具体来说，一个系统中可能会嵌套多种数据结构，比如说外界创建一个迭代器，同时会创建memtable迭代器和磁盘中的其他数据结构的迭代器，
		  当memtable迭代器中没有该数据会继续在其他迭代器中查询，但是对外界来说，所有请求都被一个全局迭代器给处理了
			// 创建一个新的迭代器
			func NewIterator() iterator {}
			// 跳转到下一个项
			func Next() {}
			// 判断是否还有效
			func Valid() bool {}
			// 从头开始
			func Rewind() {}
			// 获取当前的项
			func Item() Item {}
			// 关闭迭代器
			func Close() {}
			// 跳转到指定位置
			func Seek(key) {}

		· 针对上面的函数同时需要设计一个结构体
			type Item struct{
				key bytes
				value bytes
			}			
			type Options struct{
				// 前缀是什么
				Prefix bytes
				// 是否升序查询
				IsAsc bool
			}
		
		· 一个迭代器的工作逻辑：
			// 从iterator的头开始，不断调用Next()到下一个项，直到iterator无效；
			for iterator.Rewind() ; iterator.Valid() ; iterator.Next(){
				...
			}

	· 序列化
		· 在将skiplist flush为sst文件时，需要序列化数据为二进制格式，并适用于存储在磁盘上；
		  也就是之前提到的将各种数据结构转变为builder的过程
			// 构建builder对象
			builder := newTableBuilder(lm.opt)
			skiplistiter := immutable.sl.NewIterator(&iterator.Options{})
			for iter.Rewind(); iter.Valid(); iter.Next(){
				entry=iter.Item.Entry()
				builder.add(entry)
			}
			
			// 创建table对象 
			table = openTable(lm,sstName,builder)

			// 将builder flush到这个table中
			builder.flush(table)

	· 总结：
		1. 当KV存储引擎第一次启动，此时工作目录空的，此时不用创建任何table，
		   直到内存中的memtable大小到达一定阈值时，就会flush一个sstable到L0层；
		   L0层的数量太多了就会触发L0层到L1层的merge操作，此时会将L0层的sstable在merge为一个L1层的大一点的sstable，然后删除L0；

		2. 当KV存储引擎不是第一次启动，需要恢复，此时会通过清单文件记录的信息，初始化构建一个table


三、基本原理
	· 要实现table架构需要一些组件来分工完成
		1. opts参数组件
		2. builder序列化组件
		3. Iterator迭代器组件
		4. table在内存中作为一个结构体存在，有三个重要的组成：
			a. block，对应的是磁盘文件中的block，同时还对应着block cache，来标识热点block；
			b. index，标识哪个key在哪个sst文件的哪个block上，拿到一个key首先要判断在第几层，在第几层的哪个sst文件里，在sst文件的哪个block，在block的哪个位置；
			  		  同时为了优化读取，还会有一个bloomfilter来快速判断key是否被保存下来了
			c. ssTable，会持有一个mmap的对象，mmap会封装对持久化文件操作；
			   			mmap是对虚拟内存的映射，有data和pd两部分，当通过mmap写入的时候，会先写到data内存中，再会异步的写入到磁盘中；

	· 编码思想(序列化)
		· 编码的本质就是遵守约定的映射，按照固定的规律编码，按照固定的规律解码，并且总在编解码的性能与存储空间之间做出权衡；
		
		· 通常需要写入三种要素：
			1. 类型：用于标示一段数据用什么解码器来还原解释二进制的数据；
			2. 长度，用于标示当前数据段占用的大小，以便于再二进制序列上移动指针读取数据
			3. 数据本身

			4. 索引：为了提高查询性能，通常会在编码协议中写入数据索引，将索引也作为一种数据存储在编码的二进制文件中；
			5. 同时，为了可以更加复杂、精确的描述一段数据的类型(例如是否被压缩、由哪个事务写入、时间戳、是否删除等等)，
			   单一类型的数据无法藐视，因此引入了元数据代替之前类型这一个枚举值来描述数据段；

		· 根据上面的结构分析，基本上可以确定 最外层是 [元数据 -- 索引段 -- 数据段]；
		  其中元数据段中都包括 [type -- len -- data]，元数据中的data中可以包括 index_len、data_len等信息
		  而索引段中的data可以包括 offset、bloomfilter信息；
		  数据段就是实际的k-v数据

		· 读取数据的时候，解码器也会被设计为递归形式，每一段都会有自己的解码规则，首先会根据元数据段拿到对应读取段的解码器类型；
		  然后通过len + 起始地址 + offset来读取对应段的数据，在对每个段进行解码，这一过程往往是递归完成的；

	· 内存映射
		mmap是linux提供的一种高效的内存与磁盘之间的数据传输方式；能够实现磁盘文件和进程虚拟内存空间之间的相互同步，
		因此也就基于磁盘文件实现了进程之间的内存共享，他通过减少磁盘到页缓存再到用户进程攻坚的两次拷贝提高了整体的吞吐性能；

		mmap需要讲进程虚拟地址空间的一段内存关联到某个磁盘上，在进程的地址空间中会维护内存地址空间与磁盘物理地址的映射；
		在进程访问这片内存地址时，如果没有查询到磁盘物理地址就会引发缺页终端，中断逻辑会去拷贝磁盘中相应的文件到用户空间；

		一旦进程中的内存地址空间被更新，在一段时间后，更新的数据会同步会磁盘文件上，完成异步地更新；

		1. 延迟拷贝数据到进程虚拟地址空间，比普通的IO减少了同步文件系统页缓存的步骤，效率更高；
		2. 异步落盘有增加数据丢失的风险：
			sst文件是仅追加的，因此当一个sst文件生成后，就不会有更新操作，不会频繁的更新，也就不会出现数据脏页的情况，那么mmap就可以高效的运行；
			也可以在flush时手动调用同步函数（msync()），保证数据落盘；

		· read/write操作会先将byte数组拷贝到内核的页缓存区中，然后在交换到用户的内存中，
		  当写入达到一定次数之后再将buffer拷贝到内核页缓存中中，然后在写会到磁盘；

		· mmap相比于read/write操作，不需要经过页缓存，直接映射磁盘的文件，也可以理解为内核页缓存和用户使用的是同一个物理内存；
		  当进程发起对这片映射空间的访问引发了缺页异常，将磁盘或者swap cache中的页交换到物理内存中；

		· 为什么要使用mmap？为什么不使用read/write
			1. 使用场景可能会是一个频繁的读写操作，所以为了减少拷贝次数，mmap会更好；
			2. 因为不会出现对sst文件的更新操作，所以不会出现脏页的情况；
			   基于这样的一个使用场景，mmap的缺点(只能以页为单位映射/落盘)就可以避免了
			3. 同时因为可以使用msync()同步到磁盘，所以也不会出现数据丢失的情况；
			4. 也就是说，一个sst文件只会在一开始全量的刷盘一次，然后后续都只会读取

	· 逻辑描述
		· 创建/初始化：
			· 时机：manifest中加载，落盘memtable
				func openTable(opt,tableName,builder) table {}

			· 1. 构建参数上下文，创建sstable对象(创建mmap文件，并关联一块区域)
			  2. 判断builder是否为nil，如果存在就执行builder的flush动作序列化数据到sst；
			  3. 初始化sst文件(初始化table对象、索引、元数据等)
			  		a. 读取尾部4bytes获得checksum长度
					b. 根据长度读取checksum
					c. 读取4bytes获得index_len长度
					d. 根据长度读取index_data
					e. 计算校验和checksum比较
					f. pb反序列化data为tableIndex对象

		· 序列化：
			· 时机：L0层flush，merge压缩

			· 