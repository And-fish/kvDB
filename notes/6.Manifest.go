一、简介
    · Manifest文件是一个用于存储sst所属层级关系的元数据文件，
      当数据库重启时，需要却文件用于恢复sstable的层级关系的元数据；
      其次，每一次fulsh或者merge了sst文件后都需要变更Manifest文件；
    
    · Manifets文件期望提供的的两个功能：
        1. 能够存储sst文件层级元数据信息
        2. 支持高性能的update操作

    · 如果不使用Manifest可以实现吗？
        把level信息作为文件本身写入，每次初始化直接对当前workdir扫描一遍，来恢复level也可以做到恢复level信息；
        但是问题在于，这样处理，可能会导致当sst文件过多时恢复时间过长，直接反映在业务上的问题就是数据库的重启速度；

    · 组件设计
        · Normal：
            · 简单的sstIndex二维数组：能够存储sst文件层级的元数据(sst文件属于哪一个level，以及用于快速检索key是否[可能]存在于这个sst文件)
                type manifest struct{
                    levels *[][]*sstInfo
                }
                type sstInfo struct{
                    fileName string
                    maxKey,minKey []byte
                }

            · 对外暴露接口：read --> protobufData --> write
        
            · 问题在于，当flush或者merge一个sst的时候，需要将旧的manifest删除掉，再写回到磁盘中；
        · Superior：
            · 支持高性能的update操作：顺序写的性能远大于随机写，因此使用mmap+append的设计思想可以充分发挥ssd的性能；

            · 可能会遇到的问题：
                1. append策略可能会导致读取的逻辑变得负责，需要重播所有变更，那么同样也会导致重启速度的变慢。
                    可以引入CheckPoint机制，通过度量程序来衡量CheckPoint的时机，并覆写文件；
                    因此可以将manifest文件设计为只有创建和删除sst两种命令的状态机；
                    因为有删除操作，内存中使用list会导致多次移动元素，所以再内存中可以使用mmap来组织数据；

                2. 如果整个db只有一个manifest文件，那么单点故障的问题可能会发生。
                    正对单点故障的问题可以通过底层磁盘的冗余阵列来保证，并且保证数据库每次都在一个正确的状态下启动；

                3. 因为是一个Manifest是元数据信息，那么应该要保证立刻持久化到磁盘中(默认定时/内存不足/进程退出 时回写)，否则可能会丢失数据；
                    可以通过一个配置让用户灵活选择安全级别

            · 数据结构：
                type Manifest struct{
                    Levels []map[uint64]struct{} // 每一层有哪些sst
                    Tables map[uint64]TableManifest // 每个table在哪一层
                    Creations int   // sst创建的次数
                    Deleteions int  // sst删除的次数
                }
                type TableManifest struct{  // Table元数据
                    Leavls uint8
                }
            
            · 对外接口设计：
                1. 打开/创建manifest文件；
                    func OpenManifestFile(opt *Options) (*ManifestFile,error){}

                2. 添加table元信息到manifest文件；
                   在这里添加一定的阈值，会进行覆写，以便于提高数据库的恢复的速度；(checkPoint)
                    func AddTableMeta(level int,tm *TableMeta) error {..}

                3. 检查manifest文件的正确性，保证db在一个正确的状态启动；
                   对于manifest文件和工作目录中的sst文件，删除非重合的文件；
                    func RevertToManifest(levelMap map[uint64]struct{}) error{}

    
二、数据结构
    · 首先会有一个LSM大对象引用一个LevelManager来管理所有的sst的level
        type levelManager struct {
            maxFID uint64
            opt    *Options
            cache  *cache
            manifestFile *file.ManifestFile
            levels       []*levelHandler
            lsm           *LSM
            compactStatus *compactStatus
        }
      levelManager会关联一个levelHandler去执行一些管理level的操作；
        type levelHandler struct {
            sync.RWMutex
            levelNum       int
            tables         []*table // 里面装的就是每一个table对象
            totalSize      int64
            totalStaleSize int64
            lm             *levelManager
        }
      levelManager下面还会关联一个ManifestFile用于管理Manifest对外暴露的方法
        type ManifestFile struct {
            opt                       *Options
            f                         *os.File
            lock                      sync.Mutex
            deletionsRewriteThreshold int
            manifest                  *Manifest
        }
      ManifestFile中会有一个对于的Manifest，用于实际的维护元数据
        type Manifest struct {
            Levels    []levelManifest
            Tables    map[uint64]TableManifest
            Creations int
            Deletions int
        }
      其中TableManifest就是每个table对应的元数据
        type TableManifest struct {
            Level    uint8
            Checksum []byte // 方便今后扩展
        }
      而levelManifest就是每个level的sstabel
        type levelManifest struct {
            Tables map[uint64]struct{} // Set of table id's
        }

    · 除了内存中的数据结构，manifest相关的data结构被序列化为
        message ManifestChangeSet {
            // A set of changes that are applied atomically.
            repeated ManifestChange changes = 1;
        }
        message ManifestChange {
            uint64 Id = 1;
            enum Operation {
                    CREATE = 0;
                    DELETE = 1;
            }
            Operation Op   = 2;
            uint32 Level   = 3; // Only used for CREATE
            bytes Checksum = 4; // Only used for CREATE
        }


三、逻辑实现
    · 从LSM触出发会首先创建一个levelManager，
        func newLevelManager(opt *Option) *leavelManager{}    
      leavelManager创建的时候，会