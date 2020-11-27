package main

import (
	"context"
	"fmt"
	"github.com/docker/docker/pkg/pubsub"
	"github.com/x-debug/remote_pubsub/message"
	"google.golang.org/grpc"
	"log"
	"net"
	"strings"
	"time"
)

type PubSubServiceServerImpl struct {
	*pubsub.Publisher
}

func NewPubSubService() *PubSubServiceServerImpl {
	return &PubSubServiceServerImpl{Publisher: pubsub.NewPublisher(time.Second, 10)}
}

func (srv *PubSubServiceServerImpl) Publish(ctx context.Context, topic *message.Topic) (reply *message.Resp, err error) {
	srv.Publisher.Publish(topic.GetValue())
	return &message.Resp{Value: "OK"}, nil
}

func (srv *PubSubServiceServerImpl) Subscribe(topic *message.Topic, ch message.PubSubService_SubscribeServer) error {
	topicCh := srv.SubscribeTopic(func(v interface{}) bool {
		if key, ok := v.(string); ok {
			if strings.HasPrefix(key, topic.GetValue()) {
				return true
			}
		}

		return false
	})

	for t := range topicCh {
		err := ch.Send(&message.Resp{Value: t.(string)})
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	//server endpoint
	go func() {
		srv := grpc.NewServer()
		message.RegisterPubSubServiceServer(srv, NewPubSubService())

		lis, err := net.Listen("tcp", ":4321")
		if err != nil {
			log.Fatalln("listen error")
		}

		srv.Serve(lis)
	}()

	time.Sleep(3 * time.Second)

	//client endpoint
	conn, err := grpc.Dial("localhost:4321", grpc.WithInsecure())
	if err != nil {
		log.Fatalln("dial error")
	}
	defer conn.Close()

	service := message.NewPubSubServiceClient(conn)
	golangReceiver, _ := service.Subscribe(context.Background(), &message.Topic{Value: "golang"})
	go doSubWork(golangReceiver)
	rubyReceiver, _ := service.Subscribe(context.Background(), &message.Topic{Value: "ruby"})
	go doSubWork(rubyReceiver)

	time.Sleep(3 * time.Second)
	service.Publish(context.Background(), &message.Topic{Value: "golang,golang"})
	service.Publish(context.Background(), &message.Topic{Value: "golan"})
	service.Publish(context.Background(), &message.Topic{Value: "r u by"})
	service.Publish(context.Background(), &message.Topic{Value: "ruby, hello"})

	time.Sleep(5 * time.Second)
}

func doSubWork(receiver message.PubSubService_SubscribeClient) {
	for {
		resp, err := receiver.Recv()
		if err != nil {
			log.Fatalln("receive error")
		}

		fmt.Println("receive value ", resp.GetValue())
	}
}
