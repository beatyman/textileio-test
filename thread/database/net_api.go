package database

import (
	"context"
	crand "crypto/rand"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	log "github.com/sirupsen/logrus"
	"github.com/textileio/go-threads/cbor"
	core "github.com/textileio/go-threads/core/net"
	"github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/net/api/client"
	"google.golang.org/grpc"
)

func NetAPi()  {
	//连接节点1
	client1, err := client.NewClient("192.168.100.51:6006", grpc.WithInsecure(),grpc.WithPerRPCCredentials(thread.Credentials{}))
	if err != nil {
		log.Fatal(err)
	}
	//创建私钥令牌
	privateKey, _, err := crypto.GenerateEd25519Key(crand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	myIdentity := thread.NewLibp2pIdentity(privateKey)

	threadToken, err := client1.GetToken(context.Background(), myIdentity)

	threadID := thread.NewIDV1(thread.Raw, 32)
	threadKey := thread.NewRandomKey()


	logPrivateKey, logPublicKey, err := crypto.GenerateEd25519Key(crand.Reader)
	logID, err := peer.IDFromPublicKey(logPublicKey)

	//节点1创建Thread
	threadInfo, err := client1.CreateThread(
		context.Background(),
		threadID,
		core.WithThreadKey(thread.NewServiceKey(threadKey.Service())), // Read key is kept locally
		core.WithLogKey(logPublicKey),                                 // Private key is kept locally
		core.WithNewThreadToken(threadToken))                          // Thread token for identity is needed to verify records
	log.Infof("%+v",threadInfo)

	log.Infof("threadInfo.ID : %+v",threadInfo.ID)
	log.Infof("threadInfo.Key: %+v",threadInfo.Key)

	//将节点2加入节点一复制组
	replicatorAddr, err := multiaddr.NewMultiaddr("/ip4/192.168.100.54/tcp/4006/p2p/12D3KooWKV5JnPWxj7J6NYdmagU2YthDm9JNCubbzXNYPW5ZZmHm")
	id,err:=client1.AddReplicator(context.Background(),threadID,replicatorAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("AddReplicator %+v",id)

	//节点1接入记录
	body, err := ipldcbor.WrapObject(map[string]interface{}{
		"foo": "bar",
		"baz": []byte("howdy"),
	}, mh.SHA2_256, -1)
	log.Infof("CreateRecord: %+v",body.Cid())
	// Create the event locally
	event, err := cbor.CreateEvent(context.Background(), nil, body, threadKey.Read())

	// Create the record locally
	record, err := cbor.CreateRecord(context.Background(), nil, cbor.CreateRecordConfig{
		Block:      event,
		Prev:       cid.Undef,              // No previous records because this is the first
		Key:        logPrivateKey,
		PubKey:     myIdentity.GetPublic(),
		ServiceKey: threadKey.Service(),
	})
	err = client1.AddRecord(context.Background(), threadID, logID, record)


	//连接节点2
	client2, err := client.NewClient("192.168.100.54:6006", grpc.WithInsecure(),grpc.WithPerRPCCredentials(thread.Credentials{}))
	if err != nil {
		log.Fatal(err)
	}
	//拉取最新threadID记录
	err=client2.PullThread(context.Background(),threadID)
	if err != nil {
		log.Fatal(err)
	}
	//查询threadID
	info,err:=client2.GetThread(context.Background(),threadID)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("GetThread %+v",info)

	//通过cid获取记录信息,解码
	rRecord,err:=client2.GetRecord(context.Background(),threadID,record.Cid())
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("GetRecord  %+v",rRecord.String())
	block,err:=record.GetBlock(context.Background(),nil)
	if err != nil {
		log.Fatal(err)
	}
	event, ok := block.(*cbor.Event)
	if !ok{
		log.Fatal("convert err")
	}
	log.Infof("cbor.Event: %+v",event)
	evt,err:=event.GetBody(context.Background(),nil, threadKey.Read())
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("event.GetBody %+v",evt.String())
	data:=make(map[string]interface{})
	err = ipldcbor.DecodeInto(evt.RawData(), &data)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("ipldcbor.DecodeInto: %+v" ,data)
}