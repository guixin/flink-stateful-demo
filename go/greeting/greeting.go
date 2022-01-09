package main

import (
	"fmt"
	"net/http"

	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
)

type Greeter struct {
	SeenCount statefun.ValueSpec
}

func (g *Greeter) Invoke(ctx statefun.Context, message statefun.Message) error {
	fmt.Println("someone coming, message type:", message.ValueTypeName())
	msg := string(message.RawValue())
	fmt.Println("message raw value is:", msg)
	//if !message.Is(statefun.StringType) {
	//	return fmt.Errorf("unexpected message type %s", message.ValueTypeName())
	//}

	var name string
	_ = message.As(statefun.StringType, &name)

	storage := ctx.Storage()

	var count int32
	storage.Get(g.SeenCount, &count)

	count += 1

	storage.Set(g.SeenCount, count)

	output := fmt.Sprintf("%s comming %v times", msg, count)
	fmt.Println(output)

	//ctx.Send(statefun.MessageBuilder{
	//	Target: statefun.Address{
	//		FunctionType: statefun.TypeNameFrom("pers.evan.demo/greetings"),
	//		Id:           name,
	//	},
	//	Value: fmt.Sprintf("Hello %s for the %dth time!", name, count),
	//})
	ctx.SendEgress(&statefun.KafkaEgressBuilder{
		Target: statefun.TypeNameFrom("pers.evan.demo/greetings"),
		Topic:  "greetings",
		Key:    name,
		Value:  output,
	})

	return nil
}

func main() {
	greeter := &Greeter{
		SeenCount: statefun.ValueSpec{
			Name:      "seen_count",
			ValueType: statefun.Int32Type,
		},
	}
	builder := statefun.StatefulFunctionsBuilder()
	_ = builder.WithSpec(statefun.StatefulFunctionSpec{
		FunctionType: statefun.TypeNameFrom("pers.evan.demo/greeter"),
		States:       []statefun.ValueSpec{greeter.SeenCount},
		Function:     greeter,
	})

	http.Handle("/statefun", builder.AsHandler())
	_ = http.ListenAndServe(":8000", nil)
}
