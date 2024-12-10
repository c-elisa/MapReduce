package main

import(
	"os"
	"mapreduce/utils"
	"strconv"
	"mapreduce/method"
	"net/rpc"
	"net"
	"log"
	"sync"
	"fmt"
	"slices"
)

var n_reduce_workers int

func shuffle() [][]int{
	
	sampledInput := utils.GetSampledInput()
	reduceSplits := make([][]int, n_reduce_workers, n_reduce_workers)

	if len(sampledInput.SampledNums) == 0 {
		return [][]int{method.MapResult}
	}

	for i:=0; i<n_reduce_workers; i++{
		for _, e := range method.MapResult {
			if i==0 {
				if e<sampledInput.SampledNums[i]{
					reduceSplits[i]=append(reduceSplits[i], e)
				}
			}else if i==n_reduce_workers-1 {
				if e>=sampledInput.SampledNums[i-1]{
					reduceSplits[i]=append(reduceSplits[i], e)
				}
			}else{
				if e<sampledInput.SampledNums[i] && e>=sampledInput.SampledNums[i-1]{
					reduceSplits[i]=append(reduceSplits[i], e)
				}
			}
		}
	}

	fmt.Println(reduceSplits)
	return reduceSplits
}

func main() {
	me, err := strconv.Atoi(os.Args[1])
	utils.CheckError(err)

	//read from config file and get the port associated with this worker

	config := utils.ReadConfig()
	if(me>=len(config.Ports)){
		log.Fatal("Worker index not valid")
	}
	port := config.Ports[me]

	mapMode := slices.Contains(config.Map_nodes, me+1)
	reduceMode := slices.Contains(config.Reduce_nodes, me+1)
	n_reduce_workers = len(config.Reduce_nodes)

	//ISTANZIA IL SERVER

	mapHandler := new(method.MapReduceHandler)
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

	for{
		if mapMode{
			for !method.MapDone{}
			log.Printf("Map task done!")
			log.Printf("Sending reduce tasks...")
		
			reduceSplits := shuffle()
		
			// CONNECT TO WORKERS //
		
			addr := []string{}
			clients := make([]*rpc.Client, n_reduce_workers, n_reduce_workers)
			replies := make([]method.ReduceReply, n_reduce_workers, n_reduce_workers)
		
			// compute the addresses from config file
			for _,n := range config.Reduce_nodes {
				addr = append(addr, "localhost:" + config.Ports[n-1])
			}
		
			for i,a := range addr {
				clients[i], err = rpc.Dial("tcp", a)
				utils.CheckError(err)
				log.Println("RPC server @", a, "dialed")
			}
		
			var wg sync.WaitGroup
		
			// send RPC calls to workers
			for i:=0; i<n_reduce_workers; i++ {
				args := method.ReduceRequest{i,reduceSplits[i]}
		
				log.Printf("Call to RPC server @%s", addr[i])
		
				wg.Add(1)
		
				//Sync call in separate gorouting
				go func(client *rpc.Client, args method.ReduceRequest, reply *method.ReduceReply){
					err = client.Call("Map.Reduce", args, reply)
					utils.CheckError(err)
					wg.Done()
				}(clients[i], args, &replies[i])
		
			}
		
			wg.Wait()

			method.MapDone = false
		}

		if reduceMode{
			for !method.ReduceDone{}
			log.Printf("Reduce task done!")
			log.Printf("Sending merge request to master...")

			// CONNECT TO MASTER
			var client *rpc.Client

			masterAddress := "localhost:" + config.Master
			client, err = rpc.Dial("tcp", masterAddress)
			utils.CheckError(err)
			log.Println("Master RPC server @", masterAddress, "dialed")
		
			reply := method.MergeReply{}
			arg := method.MergeRequest{me, method.ReduceResult}
			err = client.Call("Map.Merge", arg, &reply)
			utils.CheckError(err)

			log.Printf("Merge response: %d %s\n", reply.Status, reply.Message)

			method.ReduceDone = false
		}
	}

}
