package main

import(
	"os"
	"mapreduce/utils"
	"strconv"
	"mapreduce/method"
	"net/rpc"
	"net"
	"log"
)

func main() {
	me, err := strconv.Atoi(os.Args[1])
	utils.CheckError(err)

	//read from config file and get the port associated with this worker

	config := utils.ReadConfig()
	if(me>=config.N_workers){
		log.Fatal("Worker index not valid")
	}
	port := config.Ports[me]

	//ISTANZIA IL SERVER

	mapHandler := new(method.MapHandler)
	server := rpc.NewServer()
	err = server.RegisterName("Map", mapHandler)
	utils.CheckError(err)

	err = server.RegisterName("Reduce", mapHandler)
	utils.CheckError(err)

	err = server.RegisterName("Shuffle", mapHandler)
	utils.CheckError(err)

	addr := "localhost:" + port
	lis, err := net.Listen("tcp", addr)
	utils.CheckError(err)
	log.Printf("RPC server listens on port %s", port)

	go func(){
		for{
			server.Accept(lis)
		}
	}()
	select{}

}
