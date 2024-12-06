package main

import (
	"fmt"
	"mapreduce/utils"
	"net/rpc"
	"mapreduce/method"
	"sync"
	"io"
	"os"
	"net"
	"log"
)

var config utils.Config

func mergeFiles(){

	var file *os.File
	out, err := os.OpenFile("merged.txt", os.O_CREATE | os.O_TRUNC | os.O_WRONLY, 0644)
	utils.CheckError(err)
	defer out.Close()

	for _,filename := range config.Out_files {
		file, err = os.Open(filename)
		utils.CheckError(err)

		_, err = io.Copy(out, file)
		utils.CheckError(err)
		file.Close()
	}
}

func main() {

	var err error
	
	//read from config file
	config = utils.ReadConfig()

	// master becomes a Server to receive Merge requests from workers

	//ISTANZIA IL SERVER

	port := config.Master

	//read from input file
	input := utils.ReadInput()
	split := utils.SplitInput(input.Nums, config.N_workers)

	//sample input data to optimize partitioning between reduce workers
	utils.SampleInput(input.Nums)


	// CONNECT TO WORKERS //

	addr := []string{}
	clients := make([]*rpc.Client, config.N_workers, config.N_workers)

	// compute the addresses from config file
	for i:=0; i<config.N_workers; i++ {
		addr = append(addr, "localhost:" + config.Ports[i])
	}

	for i,a := range addr {
		clients[i], err = rpc.Dial("tcp", a)
		utils.CheckError(err)
		fmt.Println("[MASTER] RPC server @", a, "dialed")
	}

	reply := make([]method.MapReply, config.N_workers, config.N_workers)

	var wg sync.WaitGroup

	// send RPC calls to workers
	for i:=0; i<config.N_workers; i++ {
		
		args := method.MapRequest{split[i]}

		fmt.Println("[MASTER] Call to RPC server @", addr[i])

		wg.Add(1)

		//Sync call in separate gorouting
		go func(client *rpc.Client, args method.MapRequest, reply *method.MapReply){
			err = client.Call("Map.Map", args, reply)
			utils.CheckError(err)
			wg.Done()
		}(clients[i], args, &reply[i])

	}

	//wait for all Map workers to finish executing
	wg.Wait()

	for i:=0; i<config.N_workers; i++ {
		fmt.Println("[MASTER] Received from MAP worker" , i, reply[i].Sorted)
	}

	mapHandler := new(method.MapHandler)
	server := rpc.NewServer()
	err = server.RegisterName("Map", mapHandler)
	utils.CheckError(err)

	address := "localhost:" + port
	lis, err := net.Listen("tcp", address)
	utils.CheckError(err)
	log.Printf("RPC server listens on port %s", port)

	go func(){
		for{
			server.Accept(lis)
		}
	}()

	for method.MergeRequestCounter!=config.N_workers{}
	mergeFiles()

	fmt.Println("Files merged...MapReduce DONE!")
	method.MergeRequestCounter=0

}
