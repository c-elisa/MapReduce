package main

import (
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
		file, err = os.Open("inout_files/" + filename)
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
	if len(config.Map_nodes) == 0{
		log.Fatal("[ERROR] There are no Map nodes.")
	}else if len(config.Reduce_nodes) == 0{
		log.Fatal("[ERROR] There are no Reduce nodes.")
	}

	//read from input file
	input := utils.ReadInput()
	if len(input.Nums) == 0 {
		log.Fatal("Empty input...cannot execute sorting.")
	}
	split := utils.SplitInput(input.Nums, len(config.Map_nodes))

	//sample input data to optimize partitioning between reduce workers
	utils.SampleInput(input.Nums)

	//START SERVER

	addr := config.Master

	mapHandler := new(method.MapReduceHandler)
	server := rpc.NewServer()
	err = server.RegisterName("MapReduce", mapHandler)
	utils.CheckError(err)

	lis, err := net.Listen("tcp", addr)
	utils.CheckError(err)
	log.Printf("RPC server listens @%s", addr)

	go func(){
		for{
			server.Accept(lis)
		}
	}()

	// CONNECT TO MAPPERS //

	n_map_workers := len(config.Map_nodes)

	clients := make([]*rpc.Client, n_map_workers, n_map_workers)

	for i,el := range config.Map_nodes {
		a := config.Workers[el-1]
		clients[i], err = rpc.Dial("tcp", a)
		utils.CheckError(err)
		log.Printf("RPC server @%s dialed", a)
	}

	reply := make([]method.MapReply, n_map_workers, n_map_workers)

	var wg sync.WaitGroup

	// send RPC calls to Mappers
	for i:=0; i<n_map_workers; i++ {
		
		args := method.MapRequest{split[i]}

		log.Printf("Call to RPC server @%s", config.Workers[i])

		wg.Add(1)

		//Sync call in separate gorouting
		go func(client *rpc.Client, args method.MapRequest, reply *method.MapReply){
			err = client.Call("MapReduce.Map", args, reply)
			utils.CheckError(err)
			wg.Done()
		}(clients[i], args, &reply[i])

	}

	//wait for all Map workers to finish executing
	wg.Wait()

	for i:=0; i<n_map_workers; i++ {
		log.Printf("Received from MAP worker %d: %v" , i, reply[i].Sorted)
	}

	for method.MergeRequestCounter!=len(config.Reduce_nodes){}
	mergeFiles()

	log.Printf("Files merged...MapReduce DONE!")
	method.MergeRequestCounter=0

}
