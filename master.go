package main

import (
	"fmt"
	"mapreduce/utils"
	"log"
	"net/rpc"
	"mapreduce/method"
	"sync"
)

func main() {

	//read from config file
	config := utils.ReadConfig()

	//read from input file
	input := utils.ReadInput()
	split := utils.SplitInput(input.Nums, config.N_workers)
	fmt.Println(split)

	// CONNECT TO WORKERS //

	addr := []string{}
	clients := make([]*rpc.Client, config.N_workers, config.N_workers)
	var err error

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

	// distribute reduce tasks

	replyReduce := make([]*method.ReduceReply, config.N_workers, config.N_workers)
	asyncCall := make([]*rpc.Call, config.N_workers, config.N_workers)

	for i:=0; i<config.N_workers; i++ {
		args := method.ReduceRequest{i}
		asyncCall[i] = clients[i].Go("Map.Reduce", args, &replyReduce[i], nil)
	}

	for i,call := range asyncCall {
		<-call.Done
		if call.Error != nil {
			log.Fatal("Error in Map.Reduce: ", call.Error.Error())
		}
		fmt.Println("[MASTER] Reduce result #" , i, replyReduce[i].AllSorted)
	}

	fmt.Println("All reduce tasks completed")

}
