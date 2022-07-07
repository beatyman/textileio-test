package database

import (
	"context"
	"github.com/alecthomas/jsonschema"
	log "github.com/sirupsen/logrus"
	dbClient "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
	"google.golang.org/grpc"
	"time"
)
type Person struct {
	ID        string `json:"_id"`
	Name      string `json:"name"`
	Age       int    `json:"age"`
	CreatedAt int    `json:"created_at"`
}
func DBApi()  {
	//DB1连接节点1
	db1, err := dbClient.NewClient("192.168.100.51:6006", grpc.WithInsecure())
	if err!=nil{
		log.Error(err)
		return
	}
	//DB1创建一个thread库
	threadID := thread.NewIDV1(thread.Raw , 32)
	err = db1.NewDB(context.Background(), threadID)
	if err!=nil{
		log.Error(err)
		return
	}
	//记录DB1中threadID的连接信息,DB2需要根据这个信息连接这个数据库
	dbInfo1, err := db1.GetDBInfo(context.Background(), threadID)
	if err!=nil{
		log.Error(err)
		return
	}
	log.Infof(" dbInfo1.Addrs[0]: %+v", dbInfo1.Addrs[0].String())
	log.Infof("dbInfo1.Key : %+v",dbInfo1.Key.String())


	//DB2连接节点2
	db2, err := dbClient.NewClient("192.168.100.54:6006", grpc.WithInsecure())
	if err!=nil{
		log.Error(err)
		return
	}
	//DB2连接到DB1创建的库(threadID)
	err = db2.NewDBFromAddr(context.Background (), dbInfo1.Addrs[0], dbInfo1.Key)
	if err!=nil{
		log.Error(err)
		return
	}

	//在DB1中创建集合testPersons
	reflector := jsonschema.Reflector{}
	mySchema := reflector.Reflect(&Person{})
	err = db1.NewCollection(context.Background(), threadID, db.CollectionConfig{
		Name:    "testPersons",
		Schema:  mySchema,
		Indexes: []db.Index{{
			Path:   "name", // Value matches json tags
			Unique: true, // Create a unique index on "name"
		}},
	})
	if err!=nil{
		log.Error(err)
		return
	}

	//在DB2中创建集合testPersons
	err = db2.NewCollection(context.Background(), threadID, db.CollectionConfig{
		Name:    "testPersons",
		Schema:  mySchema,
		Indexes: []db.Index{{
			Path:   "name", // Value matches json tags
			Unique: true, // Create a unique index on "name"
		}},
	})
	if err!=nil{
		log.Error(err)
		return
	}

	//测试数据写入
	log.Info("测试数据写入")
	//DB1 写入数据,DB2 查询
	TestReadWrite(db1,db2,threadID)
	log.Info("反向测试================================================")
	//DB2 写入数据,DB1 查询
	TestReadWrite(db2,db1,threadID)
}

func TestReadWrite(db1  *dbClient.Client, db2 *dbClient.Client,threadID thread.ID,)  {
	info1,_:=db1.GetDBInfo(context.Background(),threadID)
	info2,_:=db2.GetDBInfo(context.Background(),threadID)
	log.Infof("%+v 写入 %+v 读取",info1.Name,info2.Name)
	//DB1 写入数据
	alice := &Person{
		ID:        "",
		Name:      "Alice_"+time.Now().String(),
		Age:       30,
		CreatedAt: int(time.Now().UnixNano()),
	}

	ids, err := db1.Create(context.Background(), threadID, "testPersons", dbClient.Instances{alice})
	if err!=nil{
		log.Error(err)
		return
	}
	alice.ID = ids[0]

	QueryID:=alice.ID

	log.Infof("写入一条: %+v",alice)

	// DB1 读取
	exists, err := db1.Has(context.Background(), threadID, "testPersons", []string{QueryID})
	if err!=nil{
		log.Error(err)
		return
	}
	log.Infof("DB1查找： %+v",exists)
	if exists{
		alice = &Person{}
		err = db1.FindByID(context.Background(), threadID, "testPersons", QueryID, alice)
		if err!=nil{
			log.Errorf("DB1查找：%+v",err)
			return
		}
		log.Infof("%+v",alice)
	}

	time.Sleep(time.Second*5)

	// 从DB2 读取
	exists, err = db2.Has(context.Background(), threadID, "testPersons", []string{QueryID})
	if err!=nil{
		log.Errorf("DB2查找：%+v",err)
		return
	}
	log.Infof("DB2查找： %+v",exists)
	if exists {
		// Find an instance by ID
		alice = &Person{}
		err = db2.FindByID(context.Background(), threadID, "testPersons",QueryID, alice)
		if err!=nil{
			log.Error(err)
			return
		}
		log.Infof("DB2查找：%+v",alice)
	}
}

