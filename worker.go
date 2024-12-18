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
	"slices"
)

var n_reduce_workers int

func shuffle() [][]int{
	
	//function to partition intermediate result between Reducers

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

	return reduceSplits
}

func main() {
	me, err := strconv.Atoi(os.Args[1])
	utils.CheckError(err)

	//read from config file and get the port associated with this worker

	config := utils.ReadConfig()
	if(me>=len(config.Workers)){
		log.Fatal("Worker index not valid")
	}

	//determine in what mode the worker is supposed to work (Mapper or Reducer or both)
	mapMode := slices.Contains(config.Map_nodes, me+1)
	reduceMode := slices.Contains(config.Reduce_nodes, me+1)
	n_reduce_workers = len(config.Reduce_nodes)

	// START SERVER //

	mapHandler := new(method.MapReduceHandler)
	server := rpc.NewServer()
	err = server.RegisterName("MapReduce", mapHandler)
	utils.CheckError(err)

	address := config.Workers[me]
	lis, err := net.Listen("tcp", address)
	utils.CheckError(err)
	log.Printf("RPC server listens @%s", address)

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
		
			//partition intermediate result
			reduceSplits := shuffle()
		
			// CONNECT TO WORKERS //
		
			clients := make([]*rpc.Client, n_reduce_workers, n_reduce_workers)
			replies := make([]method.ReduceReply, n_reduce_workers, n_reduce_workers)
		
			for i,el := range config.Reduce_nodes {
				a := config.Workers[el-1]
				clients[i], err = rpc.Dial("tcp", a)
				utils.CheckError(err)
				log.Println("RPC server @", a, "dialed")
			}
		
			var wg sync.WaitGroup
		
			// send RPC calls to Reducers
			for i:=0; i<n_reduce_workers; i++ {
				args := method.ReduceRequest{i,reduceSplits[i]}
		
				log.Printf("Call to RPC server @%s", config.Workers[i])
		
				wg.Add(1)
		
				//Sync call in separate goroutine
				go func(client *rpc.Client, args method.ReduceRequest, reply *method.ReduceReply){
					err = client.Call("MapReduce.Reduce", args, reply)
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

			masterAddress := config.Master
			client, err = rpc.Dial("tcp", masterAddress)
			utils.CheckError(err)
			log.Println("Master RPC server @", masterAddress, "dialed")
		
			//send Merge request
			
			reply := method.MergeReply{}
			arg := method.MergeRequest{me, method.ReduceResult}
			err = client.Call("MapReduce.Merge", arg, &reply)
			utils.CheckError(err)

			log.Printf("Merge response: %d %s\n", reply.Status, reply.Message)

			method.ReduceDone = false
		}
	}

}
