Bolt [![Coverage Status](https://coveralls.io/repos/boltdb/bolt/badge.svg?branch=master)](https://coveralls.io/r/boltdb/bolt?branch=master) [![GoDoc](https://godoc.org/github.com/boltdb/bolt?status.svg)](https://godoc.org/github.com/boltdb/bolt) ![Version](https://img.shields.io/badge/version-1.2.1-green.svg)
====

Bolt是根据 [Howard Chu's][hyc_symas][LMDB 项目][lmdb] 开发的一个存粹的go语言版的key/value存储.
它的目标是为项目提供一个简单、高效、可靠的嵌入式数据库而不是要求一个完成的数据库服务器，例如Postgres和MySQL.

如此低级别的功能模块,简单是关键,所以bollt的API很小并且仅仅关注geting和seting

[hyc_symas]: https://twitter.com/hyc_symas
[lmdb]: http://symas.com/mdb/

## 项目状态

Bolt是稳定的, API和文件格式也是固定的. 单元测试完全覆盖和随机黑盒测试确保数据一致性和线程安全。
Bolt当前被用在T级别高负载的产品环境中，Shopify和Heroku也每天使用Bolt服务
## BoltDB中文网
为了让国内开发者更方便的了解,学习和使用Bolt,因此成立[BoltDB中文网](http://boltdb.cn)。BoltDB代码精简,质量高,对BoltDB的学习将会使你对数据库是如何工作有深刻的认识,欢迎大家投稿,有兴趣的同学可以一起来维护. BoltDB QQ群:687423439

## 开发者对项目的说明

> Bolt最初目标是提供一个简单纯粹的go语言key/value存储而不需要额外的特性,
> 最后项目成功了,不需要额外的特性也就意味着项目已经完成了
> 
> 维护一个开源数据库需要大量的时间和精力,对代码的更改可能会产生意想不到的、有时是灾难性的影响，
> 因此即使是简单的更改也需要数小时的仔细测试和验证。
>
> 不幸的是，我没有时间和精力继续这项工作。bolt处于一个稳定的状态，有多年成功的生产使用。
> 因此，我认为，把它留在目前的状态是最审慎的行动方针。
>
> 如果你有兴趣使用更多特性的bolt版本,我建议你看一下CoreOS的[bbolt](https://github.com/coreos/bbolt).

- Ben Johnson ([@benbjohnson](https://twitter.com/benbjohnson))

## 目录

- [开始](#开始)
  - [安装](#安装)
  - [打开一个数据库](#打开一个数据库)
  - [事务](#事务)
    - [读写事务](#读写事务)
    - [只读事务](#只读事务)
    - [批处理读写事务](#批处理读写事务)
    - [手动管理事务](#手动管理事务)
  - [使用buckets](#使用buckets)
  - [使用键值对](#使用键值对)
  - [bucket自增](#bucket自增)
  - [遍历键](#遍历键)
    - [前缀扫描](#前缀扫描)
    - [区间扫描](#区间扫描)
    - [ForEach()](#ForEach)
  - [bucket嵌套](#bucket嵌套)
  - [数据备份](#数据备份)
  - [统计](#统计)
  - [只读模式](#只读模式)
  - [手机上使用](#手机上使用)
- [相关资源](#相关资源)
- [和其他数据库比较](#和其他数据库比较)
  - [Postgres-MySQL及其他的关系数据库](#Postgres-MySQL及其他的关系数据库)
  - [LevelDB-RocksDB](#LevelDB-RocksDB)
  - [LMDB](#LMDB)
- [注意和限制](#注意和限制)
- [源代码阅读](#源代码阅读)
- [使用Bolt的项目](#使用Bolt的项目)

## 开始

### 安装

首先安装go,然后运行go get:

```sh
$ go get github.com/boltdb/bolt/...
```
这将检索库并且在$GOBIN路径下安装bolt命令行工具


### 打开一个数据库

bolt中最高级对象是DB,它相当于磁盘上的单个文件，并且提供数据快照的一致性

要打开数据库,只需使用bolt.Open()函数:

```go
package main

import (
	"log"

	"github.com/boltdb/bolt"
)

func main() {
	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	...
}
```
请注意，bolt在数据文件上获得了一个文件锁，因此多个进程不能同时打开同一个数据库。
打开一个已经打开的bolt数据库将使其挂起直到另一个进程关闭它。为了防止无限期等待，
您可以将超时选项传递给Open()函数:

```go
db, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
```


### 事务

Bolt一次只允许一个读写事务，但同时允许尽可能多的只读事务。
每个事务在事务启动时都有一个一致的数据视图。

单个事务和从它们创建的所有对象(例如bucket、键)都不是线程安全的。
要处理多个goroutines的数据，您必须为每个goroutines启动一个事务，
或者使用锁来确保每次只能有一个goroutine访问事务。从DB创建事务是线程安全的。

只读事务和读写事务不应该相互依赖，通常不应该同时在同一个goroutine中打开。
这可能导致一个死锁，因为读写事务需要周期性地重新映射数据文件，但它不能这样做，
而只读事务是打开的。


#### 读写事务

要开始读写事务，可以使用DB.Update()函数:

```go
err := db.Update(func(tx *bolt.Tx) error {
	...
	return nil
})
```

在闭包内，您对数据库有一个一致的视图。您将在结束时返回nil来提交事务。
您还可以通过返回一个错误来回滚该事务。所有数据库操作都允许在读写事务中。

总是检查返回错误，因为它将报告任何可能导致您的事务不完整的磁盘故障。
如果在闭包中返回一个错误，它将被传递。


#### 只读事务

要开始读事务，可以使用DB.View()函数:
```go
err := db.View(func(tx *bolt.Tx) error {
	...
	return nil
})
```

您还可以在这个闭包中得到数据库的一致视图，但是，在只读事务中不允许进行任何突变操作。
您只能检索bucket、检索值，并在只读事务中复制数据库。


#### 批处理读写事务

每个DB.Update()等待磁盘提交写入。通过将多个更新与DB.Batch()函数组合在一起，可以最小化这个开销:

```go
err := db.Batch(func(tx *bolt.Tx) error {
	...
	return nil
})
```

并发批处理调用是随机组合成更大的事务的。当有多个goroutines调用时，批处理才有用。

如果事务部分失败，则该批处理可以多次调用给定的函数。
该函数必须具有幂等性，并且只有在从DB.Batch()成功返回后，副作用才会生效。

例如:不要在函数内显示消息，而是在封闭范围内设置变量:

```go
var id uint64
err := db.Batch(func(tx *bolt.Tx) error {
	// Find last key in bucket, decode as bigendian uint64, increment
	// by one, encode back to []byte, and add new key.
	...
	id = newValue
	return nil
})
if err != nil {
	return ...
}
fmt.Println("Allocated ID %d", id)
```


#### 手动管理事务

The DB.View()和DB.Update()是对DB.Begin()的封装,这些函数将启动事务，
执行一个函数，然后在返回错误时安全地关闭您的事务。这是使用bolt事务的推荐方法。

然而，有时您可能需要手动启动和结束事务。您可以直接使用DB.Begin()函数，但请确保关闭该事务。

```go
// Start a writable transaction.
tx, err := db.Begin(true)
if err != nil {
    return err
}
defer tx.Rollback()

// Use the transaction...
_, err := tx.CreateBucket([]byte("MyBucket"))
if err != nil {
    return err
}

// Commit the transaction and check for error.
if err := tx.Commit(); err != nil {
    return err
}
```

DB.Begin()的第一个参数是布尔值来标识是否是写事务

### 使用 buckets

bucket是数据库中键/值对的集合。bucket中的所有键必须是惟一的。您可以使用DB.CreateBucket()函数创建一个bucket:

```go
db.Update(func(tx *bolt.Tx) error {
	b, err := tx.CreateBucket([]byte("MyBucket"))
	if err != nil {
		return fmt.Errorf("create bucket: %s", err)
	}
	return nil
})
```

你还可以使用Tx.CreateBucketIfNotExists()函数在bucket不存在使才创建bucket,
在打开数据库之后，为所有顶级桶调用此函数是一种常见的模式，这样您就可以保证它们存在于将来的事务中。

要删除一个bucket，只需调用Tx.DeleteBucket()函数。


### 使用键值对

要在bucket存一个键值对,使用Bucket.Put()函数

```go
db.Update(func(tx *bolt.Tx) error {
	b := tx.Bucket([]byte("MyBucket"))
	err := b.Put([]byte("answer"), []byte("42"))
	return err
})
```

这将在MyBucket中把answer的值设为42,获取这个值时使用Bucket.Get()函数

```go
db.View(func(tx *bolt.Tx) error {
	b := tx.Bucket([]byte("MyBucket"))
	v := b.Get([]byte("answer"))
	fmt.Printf("The answer is: %s\n", v)
	return nil
})
```

Get()函数不会返回一个错误，因为它的操作可以保证(除非出现某种系统故障)。
如果键存在，它将返回它的字节切片值。如果它不存在，它将返回nil。
需要注意的是，您可以将一个零长度的值设置为一个与不存在的键不同的键。

使用Bucket.Delete()函数从桶中删除一个键。

请注意从Get()返回的值只有在事务打开时才有效。如果您需要在事务之外使用一个值，那么您必须使用copy()将其复制到另一个字节片。


### bucket自增

通过使用NextSequence()函数，可以让Bolt确定一个序列，该序列可以作为键/值对的唯一标识符。看下面的例子。

```go
// CreateUser saves u to the store. The new user ID is set on u once the data is persisted.
func (s *Store) CreateUser(u *User) error {
    return s.db.Update(func(tx *bolt.Tx) error {
        // Retrieve the users bucket.
        // This should be created when the DB is first opened.
        b := tx.Bucket([]byte("users"))

        // Generate ID for the user.
        // This returns an error only if the Tx is closed or not writeable.
        // That can't happen in an Update() call so I ignore the error check.
        id, _ := b.NextSequence()
        u.ID = int(id)

        // Marshal user data into bytes.
        buf, err := json.Marshal(u)
        if err != nil {
            return err
        }

        // Persist bytes to users bucket.
        return b.Put(itob(u.ID), buf)
    })
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
    b := make([]byte, 8)
    binary.BigEndian.PutUint64(b, uint64(v))
    return b
}

type User struct {
    ID int
    ...
}
```

### 遍历键

Bolt的key在bucket中顺序排列。这使得在这些键上的连续迭代非常快。要遍历键，我们将使用Cursor:

```go
db.View(func(tx *bolt.Tx) error {
	// Assume bucket exists and has keys
	b := tx.Bucket([]byte("MyBucket"))

	c := b.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("key=%s, value=%s\n", k, v)
	}

	return nil
})
```
Curser允许您移动到key列表中的特定点，并一次向前或向后移动一个键

Curser有以下函数可用

```
First()  Move to the first key.
Last()   Move to the last key.
Seek()   Move to a specific key.
Next()   Move to the next key.
Prev()   Move to the previous key.
```

每个函数都有一个返回值(key[]byte，value[]byte)。当您迭代到游标的末尾时，
Next()返回的key为nil 。在调用Next()或Prev()之前，
必须使用First()、Last()或Seek()来查找位置。如果您不找到一个位置，
那么这些函数将返回nil键。在迭代过程中，如果key是非nil，但value为nil，
这意味着键引用的是一个bucket而不是一个value。使用Bucket.Bucket()来访问子桶。


#### 前缀扫描

要遍历一个键前缀，可以组合Seek()和bytes.HasPrefix():

```go
db.View(func(tx *bolt.Tx) error {
	// Assume bucket exists and has keys
	c := tx.Bucket([]byte("MyBucket")).Cursor()

	prefix := []byte("1234")
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		fmt.Printf("key=%s, value=%s\n", k, v)
	}

	return nil
})
```

#### 区间扫描

另一个常见的用例是扫描一个范围，例如一个时间范围。如果您使用sortable时间编码，
比如RFC3339，那么您可以查询一个特定的日期范围，如下所示:

```go
db.View(func(tx *bolt.Tx) error {
	// Assume our events bucket exists and has RFC3339 encoded time keys.
	c := tx.Bucket([]byte("Events")).Cursor()

	// Our time range spans the 90's decade.
	min := []byte("1990-01-01T00:00:00Z")
	max := []byte("2000-01-01T00:00:00Z")

	// Iterate over the 90's.
	for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
		fmt.Printf("%s: %s\n", k, v)
	}

	return nil
})
```

请注意，虽然RFC3339是可排序的，但是RFC3339Nano的Golang实现在小数点后并没有使用固定的位数，因此不会被排序。

#### ForEach()

如果要遍历所有键,还可以使用ForEach()

```go
db.View(func(tx *bolt.Tx) error {
	// Assume bucket exists and has keys
	b := tx.Bucket([]byte("MyBucket"))

	b.ForEach(func(k, v []byte) error {
		fmt.Printf("key=%s, value=%s\n", k, v)
		return nil
	})
	return nil
})
```

请注意，ForEach()中的键和值只在事务打开时有效。如果需要在事务之外使用键或值，则必须使用copy()将其复制到另一个字节片。

### bucket嵌套

您还可以将一个bucket存储在一个键中，以创建嵌套的bucket。API与DB对象上的桶管理API相同:

```go
func (*Bucket) CreateBucket(key []byte) (*Bucket, error)
func (*Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error)
func (*Bucket) DeleteBucket(key []byte) error
```

如果你是一个多用户应用程序，其中根级别的bucket存储许多本身就是bucket的账号。
在账户bucket中，您可以有许多与账号本身(用户、Notes等)相关的bucket，将信息隔离到逻辑分组中。

```go

// createUser creates a new user in the given account.
func createUser(accountID int, u *User) error {
    // Start the transaction.
    tx, err := db.Begin(true)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // Retrieve the root bucket for the account.
    // Assume this has already been created when the account was set up.
    root := tx.Bucket([]byte(strconv.FormatUint(accountID, 10)))

    // Setup the users bucket.
    bkt, err := root.CreateBucketIfNotExists([]byte("USERS"))
    if err != nil {
        return err
    }

    // Generate an ID for the new user.
    userID, err := bkt.NextSequence()
    if err != nil {
        return err
    }
    u.ID = userID

    // Marshal and save the encoded user.
    if buf, err := json.Marshal(u); err != nil {
        return err
    } else if err := bkt.Put([]byte(strconv.FormatUint(u.ID, 10)), buf); err != nil {
        return err
    }

    // Commit the transaction.
    if err := tx.Commit(); err != nil {
        return err
    }

    return nil
}

```




### 数据备份

Bolt是一个单独的文件，所以很容易备份。可以使用Tx.WriteTo()函数将数据库的一致视图写入Writer。
如果您从一个只读事务中调用这个，它将执行一个热备份，而不会阻塞您的其他数据库读写。

默认情况下，它将使用一个常规的文件句柄来使用操作系统的页面缓存。请参阅Tx文档，了解关于优化大于ram数据集的信息。

一个常见的用例是通过HTTP进行备份，这样您就可以使用curl这样的工具进行数据库备份:

```go
func BackupHandleFunc(w http.ResponseWriter, req *http.Request) {
	err := db.View(func(tx *bolt.Tx) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="my.db"`)
		w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
		_, err := tx.WriteTo(w)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
```

然后你就可以使用下面的方法备份了

```sh
$ curl http://localhost/backup > my.db
```
或者打开你的浏览器输入http://localhost/backup来自动下载

如果你想备份到其他文件中去可以使用Tx.CopyFile()函数


### 统计

数据库保存了它执行的许多内部操作的运行计数，这样您就可以更好地理解发生了什么。
通过在两个时间点抓取这些数据的快照，我们可以看到在那个时间范围内执行了什么操作。

例如，我们可以启动一个goroutine每隔10秒记录日志:

```go
go func() {
	// Grab the initial stats.
	prev := db.Stats()

	for {
		// Wait for 10s.
		time.Sleep(10 * time.Second)

		// Grab the current stats and diff them.
		stats := db.Stats()
		diff := stats.Sub(&prev)

		// Encode stats to JSON and print to STDERR.
		json.NewEncoder(os.Stderr).Encode(diff)

		// Save stats for the next loop.
		prev = stats
	}
}()
```

还可以将这些统计信息传输到像statsd这样的服务中，以监视或提供一个将执行固定长度示例的HTTP端点。


### 只读模式

有时创建一个共享的只读的bolt数据库是很有用的。仅仅在打开数据库时设置Option.ReadOnly就行。
只读模式使用共享锁来允许多个进程从数据库中读取，但是它将阻止任何进程以读写模式打开数据库。

```go
db, err := bolt.Open("my.db", 0666, &bolt.Options{ReadOnly: true})
if err != nil {
	log.Fatal(err)
}
```

### 手机上使用

利用gomobile工具的绑定特性，Bolt能够在移动设备上运行。
创建一个包含数据库逻辑的结构体和一个接受数据库实际保存文件路径的初始化构造函数。
无论是Android还是iOS，这种方法都不需要获得额外的权限或清理。

```go
func NewBoltDB(filepath string) *BoltDB {
	db, err := bolt.Open(filepath+"/demo.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	return &BoltDB{db}
}

type BoltDB struct {
	db *bolt.DB
	...
}

func (b *BoltDB) Path() string {
	return b.db.Path()
}
		
func (b *BoltDB) Close() {
	b.db.Close()
}

```

数据库逻辑应该是在这个被包装的结构体上定义方法

要从本地环境初始化这个结构(两个平台现在都将本地存储与云同步,这些代码片段会使数据库文件功能不可用):

#### Android

```java
String path;
if (android.os.Build.VERSION.SDK_INT >=android.os.Build.VERSION_CODES.LOLLIPOP){
    path = getNoBackupFilesDir().getAbsolutePath();
} else{
    path = getFilesDir().getAbsolutePath();
}
Boltmobiledemo.BoltDB boltDB = Boltmobiledemo.NewBoltDB(path)
```

#### iOS

```objc
- (void)demo {
    NSString* path = [NSSearchPathForDirectoriesInDomains(NSLibraryDirectory,
                                                          NSUserDomainMask,
                                                          YES) objectAtIndex:0];
	GoBoltmobiledemoBoltDB * demo = GoBoltmobiledemoNewBoltDB(path);
	[self addSkipBackupAttributeToItemAtPath:demo.path];
	//Some DB Logic would go here
	[demo close];
}

- (BOOL)addSkipBackupAttributeToItemAtPath:(NSString *) filePathString
{
    NSURL* URL= [NSURL fileURLWithPath: filePathString];
    assert([[NSFileManager defaultManager] fileExistsAtPath: [URL path]]);

    NSError *error = nil;
    BOOL success = [URL setResourceValue: [NSNumber numberWithBool: YES]
                                  forKey: NSURLIsExcludedFromBackupKey error: &error];
    if(!success){
        NSLog(@"Error excluding %@ from backup %@", [URL lastPathComponent], error);
    }
    return success;
}

```

## 相关资源

要了解更多关于Bolt的信息，请查阅以下文章:

* [Intro to BoltDB: Painless Performant Persistence](http://npf.io/2014/07/intro-to-boltdb-painless-performant-persistence/) by [Nate Finch](https://github.com/natefinch).
* [Bolt -- an embedded key/value database for Go](https://www.progville.com/go/bolt-embedded-db-golang/) by Progville


## 和其他数据库比较

### Postgres-MySQL及其他的关系数据库

关系数据库将数据结构化为行，并且只能通过使用SQL访问。
这种方法提供了如何存储和查询数据的灵活性，但也会在解析和规划SQL语句时产生开销。
Bolt通过一个字节片键访问所有数据。这使得Bolt能够快速读取和写入数据，
但是并没有提供内置的支持来关联值。

大多数关系数据库(除了SQLite之外)都是独立的服务器，它们与应用程序分开运行。
这使您的系统能够灵活地将多个应用程序服务器连接到单个数据库服务器，
但是增加了在网络上序列化和传输数据的开销。Bolt是一个包含在应用程序中的库，
所以所有的数据访问都必须经过应用程序。这使数据更接近您的应用程序，但限制了对数据的多进程访问。

### LevelDB-RocksDB

LevelDB及其衍生物(RocksDB, HyperLevelDB)类似于Bolt，
因为它们是绑定到应用程序中的库，但是，它们的底层结构是一种日志结构的merge-tree (LSM树)。
LSM树通过使用前面的日志和多层、排序的文件SSTables来优化随机写入。Bolt在内部使用B+树，只有一个文件。

如果您需要一个高的随机写吞吐量(>10000w/秒)，或者您需要使用机械磁盘，那么LevelDB可能是一个不错的选择。
如果您的应用程序是重读的，或者进行了大量的范围扫描，那么Bolt可能是一个不错的选择。

另一个重要的考虑是，LevelDB没有事务。它支持键/值对的批写操作，它支持读取快照，
但它不会让您安全地进行比较和交换操作。Bolt支持完全可序列化的ACID事务。


### LMDB

Bolt最初是LMDB的一部分，因此在架构上类似。它们都使用B+树，使用ACID语义和完全可序列化的事务，并使用单个writer和多个reader支持无锁MVCC。

这两个项目有些不同。LMDB着重于原始性能，而Bolt专注于简单易用。
例如，LMDB允许一些不安全的操作，比如直接为性能而写。bolt选择不允许操作，使数据库处于损坏状态。在Bolt中唯一的例外是DB.NoSync。

API中也有一些不同之处。在打开mdb_env时，LMDB需要最大mmap大小，
而Bolt将自动处理增量mmap大小调整。LMDB使用多个标志重载了getter和setter函数，而Bolt将这些特殊的情况分解为它们自己的函数。

## 注意和限制

为工作挑选合适的工具很重要，Bolt也不例外。在评估和使用Bolt时，需要注意以下几点:

* bolt适合阅读密集的工作负载。顺序写性能也很快速，但是随机写的速度很慢。您可以使用DB.Batch()或添加一个写前日志来帮助缓解这个问题。

* Bolt在内部使用了B+树，因此可以有很多随机的页面访问。ssd在旋转磁盘上提供了显著的性能提升。尽量避免长期运行的读取事务。Bolt使用的是copy-on-write，所以旧页面不能被回收，而旧的事务正在使用它们。

* 从bolt返回的字节片只在事务期间有效。一旦事务被提交或回滚，那么它们指向的内存可以被新页面重用，或者可以从虚拟内存中映射，在访问它时，您将看到一个意外的故障地址。

* Bolt在数据库文件上使用了一个独占的写锁，因此它不能被多个进程共享。

* 使用时要小心。对于具有随机插入的bucket，设置高填充百分比会导致数据库的页面利用率非常低

* 一般使用较大的桶。较小的bucket一旦超过页面大小(通常为4KB)，就会导致页面利用率不高

* 批量加载大量随机写入到一个新桶中可能会很慢，因为在事务提交之前，页面不会分裂。不建议在单个事务中随机插入超过100,000个键/值对到单个新bucket中。

* Bolt使用内存映射文件，因此底层操作系统处理数据的缓存。通常，操作系统会在内存中缓存尽可能多的文件，并根据需要释放内存到其他进程。这意味着在使用大型数据库时，Bolt可以显示很高的内存使用量。但是，这是预期的，操作系统将根据需要释放内存。Bolt可以处理比可用物理RAM大得多的数据库，只要它的内存映射适合于进程虚拟地址空间。在32位系统上可能会有问题。

* Bolt数据库中的数据结构是内存映射，因此数据文件将是endian特有的。这意味着您不能从一个小的endian机器复制一个bolt文件到一个大的endian机器并使它工作。对于大多数用户来说，这并不是一个值得关注的问题，因为大多数现代的cpu都是小的endian。

* 由于页面是在磁盘上放置的，所以Bolt不能截断数据文件并将空闲页面返回到磁盘。相反，Bolt在其数据文件中维护一个未使用页面的空闲列表。这些空闲页面可以在以后的事务中重用。这对于许多用例来说很有效，因为数据库通常会增长。但是，需要注意的是，删除大量数据将不允许您回收磁盘上的空间。

  更多信息, [see this comment][page-allocation].

[page-allocation]: https://github.com/boltdb/bolt/issues/308#issuecomment-74811638


## 源代码阅读

Bolt是一个相对较小的代码库(<3K)，它是一个嵌入式的、可序列化的、事务性的键/值数据库，
因此对于那些对数据库工作感兴趣的人来说，它是一个很好的起点。

bolt最好的开始为以下几点:

- `DB.Open()` 初始化对数据库的引用。如果数据库不存在，它负责创建数据库，获取文件的独占锁，读取元页面，以及内存映射文件。

- `DB.Begin()` 根据可写参数的值启动一个只读或读写事务。这需要简单地获取“meta”锁来跟踪公开事务。只有一个读写事务可以同时存在，因此“rwlock”是在读写事务的生命周期中获得的。

- `Bucket.Put()` 将一个键/值对写入到一个bucket中。在验证参数之后，将使用游标遍历B+树，并将其键值和值写入位置。找到位置后，bucket将底层页面和页面的父页面变为“节点”。这些节点是在读写事务期间发生突变的地方。这些更改在提交期间被刷新到磁盘。

- `Bucket.Get()` 从一个桶中检索键/值对。它使用游标移动到键/值对的页面和位置。在只读事务期间，返回键和值数据作为对底层mmap文件的直接引用，因此没有分配开销。对于读写事务，此数据可以引用mmap文件或内存中的节点值之一。

- `Curser` 该对象只是用于遍历磁盘页或内存节点中的B+树。它可以寻找一个特定的键，移动到第一个或最后一个值，或者它可以向前或向后移动。游标以透明的方式将B+树上下移动到最终用户。

- `Tx.Commit()` 将内存中的脏节点和空闲页面的列表转换为要写到磁盘的页面。写入磁盘的过程分为两个阶段。首先，将脏页面写入磁盘，然后发生fsync()。其次，写入一个带有递增事务ID的新元页面，并出现另一个fsync()。这两个阶段编写确保在发生崩溃时忽略部分写入的数据页，因为指向它们的meta页面从未被写入。部分写的元页无效，因为它们是用校验和写的。

If you have additional notes that could be helpful for others, please submit
them via pull request.


## 使用Bolt的项目

下面是公开开源的使用bolt的项目:

* [BoltDbWeb](https://github.com/evnix/boltdbweb) - A web based GUI for BoltDB files.
* [Operation Go: A Routine Mission](http://gocode.io) - An online programming game for Golang using Bolt for user accounts and a leaderboard.
* [Bazil](https://bazil.org/) - A file system that lets your data reside where it is most convenient for it to reside.
* [DVID](https://github.com/janelia-flyem/dvid) - Added Bolt as optional storage engine and testing it against Basho-tuned leveldb.
* [Skybox Analytics](https://github.com/skybox/skybox) - A standalone funnel analysis tool for web analytics.
* [Scuttlebutt](https://github.com/benbjohnson/scuttlebutt) - Uses Bolt to store and process all Twitter mentions of GitHub projects.
* [Wiki](https://github.com/peterhellberg/wiki) - A tiny wiki using Goji, BoltDB and Blackfriday.
* [ChainStore](https://github.com/pressly/chainstore) - Simple key-value interface to a variety of storage engines organized as a chain of operations.
* [MetricBase](https://github.com/msiebuhr/MetricBase) - Single-binary version of Graphite.
* [Gitchain](https://github.com/gitchain/gitchain) - Decentralized, peer-to-peer Git repositories aka "Git meets Bitcoin".
* [event-shuttle](https://github.com/sclasen/event-shuttle) - A Unix system service to collect and reliably deliver messages to Kafka.
* [ipxed](https://github.com/kelseyhightower/ipxed) - Web interface and api for ipxed.
* [BoltStore](https://github.com/yosssi/boltstore) - Session store using Bolt.
* [photosite/session](https://godoc.org/bitbucket.org/kardianos/photosite/session) - Sessions for a photo viewing site.
* [LedisDB](https://github.com/siddontang/ledisdb) - A high performance NoSQL, using Bolt as optional storage.
* [ipLocator](https://github.com/AndreasBriese/ipLocator) - A fast ip-geo-location-server using bolt with bloom filters.
* [cayley](https://github.com/google/cayley) - Cayley is an open-source graph database using Bolt as optional backend.
* [bleve](http://www.blevesearch.com/) - A pure Go search engine similar to ElasticSearch that uses Bolt as the default storage backend.
* [tentacool](https://github.com/optiflows/tentacool) - REST api server to manage system stuff (IP, DNS, Gateway...) on a linux server.
* [Seaweed File System](https://github.com/chrislusf/seaweedfs) - Highly scalable distributed key~file system with O(1) disk read.
* [InfluxDB](https://influxdata.com) - Scalable datastore for metrics, events, and real-time analytics.
* [Freehold](http://tshannon.bitbucket.org/freehold/) - An open, secure, and lightweight platform for your files and data.
* [Prometheus Annotation Server](https://github.com/oliver006/prom_annotation_server) - Annotation server for PromDash & Prometheus service monitoring system.
* [Consul](https://github.com/hashicorp/consul) - Consul is service discovery and configuration made easy. Distributed, highly available, and datacenter-aware.
* [Kala](https://github.com/ajvb/kala) - Kala is a modern job scheduler optimized to run on a single node. It is persistent, JSON over HTTP API, ISO 8601 duration notation, and dependent jobs.
* [drive](https://github.com/odeke-em/drive) - drive is an unofficial Google Drive command line client for \*NIX operating systems.
* [stow](https://github.com/djherbis/stow) -  a persistence manager for objects
  backed by boltdb.
* [buckets](https://github.com/joyrexus/buckets) - a bolt wrapper streamlining
  simple tx and key scans.
* [mbuckets](https://github.com/abhigupta912/mbuckets) - A Bolt wrapper that allows easy operations on multi level (nested) buckets.
* [Request Baskets](https://github.com/darklynx/request-baskets) - A web service to collect arbitrary HTTP requests and inspect them via REST API or simple web UI, similar to [RequestBin](http://requestb.in/) service
* [Go Report Card](https://goreportcard.com/) - Go code quality report cards as a (free and open source) service.
* [Boltdb Boilerplate](https://github.com/bobintornado/boltdb-boilerplate) - Boilerplate wrapper around bolt aiming to make simple calls one-liners.
* [lru](https://github.com/crowdriff/lru) - Easy to use Bolt-backed Least-Recently-Used (LRU) read-through cache with chainable remote stores.
* [Storm](https://github.com/asdine/storm) - Simple and powerful ORM for BoltDB.
* [GoWebApp](https://github.com/josephspurrier/gowebapp) - A basic MVC web application in Go using BoltDB.
* [SimpleBolt](https://github.com/xyproto/simplebolt) - A simple way to use BoltDB. Deals mainly with strings.
* [Algernon](https://github.com/xyproto/algernon) - A HTTP/2 web server with built-in support for Lua. Uses BoltDB as the default database backend.
* [MuLiFS](https://github.com/dankomiocevic/mulifs) - Music Library Filesystem creates a filesystem to organise your music files.
* [GoShort](https://github.com/pankajkhairnar/goShort) - GoShort is a URL shortener written in Golang and BoltDB for persistent key/value storage and for routing it's using high performent HTTPRouter.
* [torrent](https://github.com/anacrolix/torrent) - Full-featured BitTorrent client package and utilities in Go. BoltDB is a storage backend in development.
* [gopherpit](https://github.com/gopherpit/gopherpit) - A web service to manage Go remote import paths with custom domains
* [bolter](https://github.com/hasit/bolter) - Command-line app for viewing BoltDB file in your terminal.
* [btcwallet](https://github.com/btcsuite/btcwallet) - A bitcoin wallet.
* [dcrwallet](https://github.com/decred/dcrwallet) - A wallet for the Decred cryptocurrency.
* [Ironsmith](https://github.com/timshannon/ironsmith) - A simple, script-driven continuous integration (build - > test -> release) tool, with no external dependencies
* [BoltHold](https://github.com/timshannon/bolthold) - An embeddable NoSQL store for Go types built on BoltDB
* [Ponzu CMS](https://ponzu-cms.org) - Headless CMS + automatic JSON API with auto-HTTPS, HTTP/2 Server Push, and flexible server framework.

If you are using Bolt in a project please send a pull request to add it to the list.
