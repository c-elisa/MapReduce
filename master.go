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

	//read from input file
	input := utils.ReadInput()
	split := utils.SplitInput(input.Nums, len(config.Map_nodes))

	//sample input data to optimize partitioning between reduce workers
	utils.SampleInput(input.Nums)


	// CONNECT TO WORKERS //

	n_map_workers := len(config.Map_nodes)

	clients := make([]*rpc.Client, n_map_workers, n_map_workers)

	for i,a := range config.Workers {
		clients[i], err = rpc.Dial("tcp", a)
		utils.CheckError(err)
		fmt.Println("[MASTER] RPC server @", a, "dialed")
	}

	reply := make([]method.MapReply, n_map_workers, n_map_workers)

	var wg sync.WaitGroup

	// send RPC calls to workers
	for i:=0; i<n_map_workers; i++ {
		
		args := method.MapRequest{split[i]}

		fmt.Println("[MASTER] Call to RPC server @", config.Workers[i])

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

	for i:=0; i<n_map_workers; i++ {
		fmt.Println("[MASTER] Received from MAP worker" , i, reply[i].Sorted)
	}

	//ISTANZIA IL SERVER

	addr := config.Master

	mapHandler := new(method.MapReduceHandler)
	server := rpc.NewServer()
	err = server.RegisterName("Map", mapHandler)
	utils.CheckError(err)

	lis, err := net.Listen("tcp", addr)
	utils.CheckError(err)
	log.Printf("RPC server listens @%s", addr)

	go func(){
		for{
			server.Accept(lis)
		}
	}()

	for method.MergeRequestCounter!=len(config.Reduce_nodes){}
	mergeFiles()

	fmt.Println("Files merged...MapReduce DONE!")
	method.MergeRequestCounter=0

}
